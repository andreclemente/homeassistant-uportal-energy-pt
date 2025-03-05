import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.util import dt as dt_util
from .const import DOMAIN, CONF_BASE_URL, UNIT_MAP, PRODUCT_NAMES

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass, config_entry, async_add_entities):
    """Full setup including service registration"""
    api = UportalEnergyPtApiClient(hass, config_entry)
    await api.async_initialize()
    
    sensors = [
        UportalEnergyPtSensor(
            api,
            counter["codigoMarca"],
            counter["numeroContador"],
            counter["codigoProduto"],
            func["codigoFuncao"],
            func["descFuncao"]
        )
        for counter in config_entry.data["counters"]
        for func in counter["functions"]
    ]
    
    async_add_entities(sensors, True)

    async def import_history(call):
        """Service handler for history import"""
        for sensor in sensors:
            await sensor.async_import_historical_data()
    
    hass.services.async_register(DOMAIN, "import_history", import_history)

class UportalEnergyPtApiClient:
    """Complete API client implementation"""
    def __init__(self, hass, config_entry):
        self.hass = hass
        self.config_entry = config_entry
        self.session = async_get_clientsession(hass)
        self.data = {}
        self.auth_data = {
            "token": None,
            "expiry": 0
        }
        self.last_update = None

    async def async_initialize(self):
        """Full initialization sequence"""
        await self.async_refresh_token()

    async def async_refresh_token(self):
        """Complete token refresh implementation"""
        try:
            if not self.auth_data["token"] or dt_util.utcnow().timestamp() > self.auth_data["expiry"] - 300:
                response = await self.session.post(
                    f"{self.config_entry.data[CONF_BASE_URL]}login",
                    json={
                        "username": self.config_entry.data["username"],
                        "password": self.config_entry.data["password"]
                    }
                )
                response.raise_for_status()
                data = await response.json()
                
                self.auth_data.update({
                    "token": data["token"]["token"],
                    "expiry": data["token"]["expirationDate"]
                })
        except Exception as e:
            _LOGGER.error("Token refresh failed: %s", str(e))
            raise

    async def async_update_data(self, counter):
        """Complete data update implementation"""
        try:
            await self.async_refresh_token()
            
            params = {
                "codigoMarca": counter["codigoMarca"],
                "codigoProduto": counter["codigoProduto"],
                "numeroContador": counter["numeroContador"],
                "subscriptionId": self.config_entry.data["subscription_id"],
                "dataFim": dt_util.now().strftime("%Y-%m-%d"),
                "dataInicio": (dt_util.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            }
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers={"X-Auth-Token": self.auth_data["token"]},
                params=params,
                timeout=30
            ) as response:
                if response.status != 200:
                    raise Exception(f"API returned {response.status}")
                    
                data = await response.json()
                processed = []
                
                for entry in data:
                    try:
                        entry_date = dt_util.parse_datetime(entry["data"].replace("Z", "+00:00"))
                        processed.extend([
                            {
                                "codFuncao": r["codFuncao"],
                                "leitura": r["leitura"],
                                "entry_date": entry_date
                            }
                            for r in entry["leituras"]
                        ])
                    except KeyError:
                        continue
                        
                self.data[counter["numeroContador"]] = processed
                self.last_update = dt_util.utcnow()
                
        except Exception as e:
            _LOGGER.error("Data update failed: %s", str(e))
            raise

class UportalEnergyPtSensor(SensorEntity):
    """Complete sensor entity implementation"""
    def __init__(self, api, marca, numero, produto, funcao, descricao):
        self.api = api
        self.marca = marca
        self.numero = numero
        self.produto = produto
        self.funcao = funcao
        self.descricao = descricao
        
        self._attr_name = f"uPortal {PRODUCT_NAMES[produto]} {descricao} ({numero})"
        self._attr_unique_id = f"uportal_{produto}_{numero}_{funcao}"
        self._attr_native_unit_of_measurement = UNIT_MAP[produto]
        self._attr_state_class = "total_increasing"
        self._attr_device_class = "energy" if produto == "EB" else "gas" if produto == "GP" else "water"
        self._attr_native_value = 0  # Default value
        self._attr_available = True

    async def async_update(self):
        """Complete update method with error handling"""
        try:
            await self.api.async_update_data({
                "codigoMarca": self.marca,
                "numeroContador": self.numero,
                "codigoProduto": self.produto
            })
            
            readings = self.api.data.get(self.numero, [])
            valid_readings = sorted(
                [r for r in readings if r["codFuncao"] == self.funcao],
                key=lambda x: x["entry_date"],
                reverse=True
            )
            
            if valid_readings:
                self._attr_native_value = valid_readings[0]["leitura"]
                self._attr_last_updated = valid_readings[0]["entry_date"]
                self._attr_available = True
            else:
                self._attr_native_value = 0
                self._attr_available = False
                
        except Exception as e:
            self._attr_native_value = 0
            self._attr_available = False
            _LOGGER.error("Update failed: %s", str(e))

    async def async_import_historical_data(self):
        """Complete historical import implementation"""
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.statistics import async_add_external_statistics
        
        try:
            # Get existing statistics
            stats = await get_instance(self.hass).async_add_executor_job(
                get_instance(self.hass).statistics_short_term,
                self.entity_id,
                dt_util.parse_datetime("1970-01-01T00:00:00+00:00"),
                dt_util.utcnow()
            )
            
            # Fetch historical data
            counter = {
                "codigoMarca": self.marca,
                "codigoProduto": self.produto,
                "numeroContador": self.numero
            }
            readings = await self.api.async_get_historical_data(counter, full_history=True)
            
            # Filter new data points
            existing_starts = {stat["start"] for stat in stats}
            new_data = [
                {
                    "start": r["entry_date"],
                    "state": r["leitura"],
                    "sum": r["leitura"]
                }
                for r in readings
                if r["codFuncao"] == self.funcao 
                and r["entry_date"].timestamp() not in existing_starts
            ]
            
            # Import in batches
            batch_size = 100
            for i in range(0, len(new_data), batch_size):
                await async_add_external_statistics(
                    self.hass,
                    {
                        "source": DOMAIN,
                        "name": self.name,
                        "statistic_id": self.entity_id,
                        "unit_of_measurement": self._attr_native_unit_of_measurement,
                        "has_sum": True
                    },
                    new_data[i:i+batch_size]
                )
            
            _LOGGER.info("Imported %d historical entries for %s", len(new_data), self.entity_id)
            
        except Exception as e:
            _LOGGER.error("Historical import failed: %s", str(e))
