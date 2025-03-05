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
    """Set up sensors and register import service."""
    api = UportalEnergyPtApiClient(hass, config_entry)
    await api.async_initialize()
    
    sensors = []
    for counter in config_entry.data["counters"]:
        product_type = counter["codigoProduto"]
        for function in counter["functions"]:
            sensor = UportalEnergyPtSensor(
                api, 
                counter["codigoMarca"],
                counter["numeroContador"],
                product_type,
                function["codigoFuncao"],
                function["descFuncao"]
            )
            sensors.append(sensor)
    
    async_add_entities(sensors, True)

    async def import_history(call):
        """Handle historical data import service call."""
        _LOGGER.info("Starting historical data import")
        for sensor in sensors:
            await sensor.async_import_historical_data()
    
    hass.services.async_register(DOMAIN, "import_history", import_history)

class UportalEnergyPtApiClient:
    def __init__(self, hass, config_entry):
        self.hass = hass
        self.config_entry = config_entry
        self.session = async_get_clientsession(hass)
        self.data = {}
        self.auth_data = {
            "token": config_entry.data.get("token"),
            "expiry": config_entry.data.get("expiry", 0)
        }

    async def async_initialize(self):
        """Initialize auth state."""
        await self.async_refresh_token()

    async def async_refresh_token(self):
        """Refresh authentication token with retries."""
        try:
            if (not self.auth_data["token"] or 
                dt_util.utcnow().timestamp() > self.auth_data["expiry"] - 300):
                
                _LOGGER.debug("Refreshing expired token")
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
                _LOGGER.info("Token refreshed successfully")
                
        except aiohttp.ClientResponseError as e:
            _LOGGER.error("Token refresh failed (HTTP %s): %s", e.status, e.message)
            self.auth_data["token"] = None
            raise
        except Exception as e:
            _LOGGER.error("Token refresh error: %s", str(e))
            self.auth_data["token"] = None
            raise

    async def async_get_historical_data(self, counter):
        """Retrieve historical data with proper error handling."""
        try:
            await self.async_refresh_token()  # Ensure valid token before request
            params = {
                "codigoMarca": counter["codigoMarca"],
                "codigoProduto": counter["codigoProduto"],
                "numeroContador": counter["numeroContador"],
                "subscriptionId": self.config_entry.data["subscription_id"],
                "dataFim": dt_util.now().strftime("%Y-%m-%d"),
                "dataInicio": "2000-01-01"  # Get all available history
            }
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers={"X-Auth-Token": self.auth_data["token"]},
                params=params,
                timeout=60
            ) as response:
                if response.status != 200:
                    _LOGGER.error("Historical data request failed: %s", response.status)
                    return []
                
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' not in content_type:
                    _LOGGER.error("Non-JSON response: %s", content_type)
                    return []
                
                data = await response.json()
                return self._process_historical_data(data)
                
        except Exception as e:
            _LOGGER.error("Historical data fetch failed: %s", str(e))
            return []

    def _process_historical_data(self, data):
        """Process and validate historical data structure."""
        processed = []
        for entry in data:
            try:
                entry_date = dt_util.parse_datetime(entry["data"].replace("Z", "+00:00"))
                for reading in entry["leituras"]:
                    processed.append({
                        "codFuncao": reading["codFuncao"],
                        "leitura": reading["leitura"],
                        "entry_date": entry_date,
                        "isEstimativa": reading.get("isEstimativa", False)
                    })
            except (KeyError, ValueError) as e:
                _LOGGER.warning("Skipping invalid entry: %s", str(e))
        return processed

class UportalEnergyPtSensor(SensorEntity):
    """Sensor entity with historical data support."""
    
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
        self._attr_native_value = 0
        self._attr_available = True

    async def async_update(self):
        """Update current sensor value."""
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
                self._attr_available = True
            else:
                self._attr_native_value = 0
                self._attr_available = False
                
        except Exception as e:
            self._attr_native_value = 0
            self._attr_available = False
            _LOGGER.error("Update failed: %s", str(e))

    async def async_import_historical_data(self):
        """Import historical data into statistics."""
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.statistics import async_add_external_statistics

        try:
            _LOGGER.info("Starting historical import for %s", self.entity_id)
            
            # Get existing statistics
            recorder = get_instance(self.hass)
            existing_stats = await recorder.async_add_executor_job(
                recorder.statistics_during_period,
                dt_util.utc_from_timestamp(0),
                None,
                [self.entity_id],
                "hour",
                None,
                {"state", "sum"}
            )
            
            # Correct existing times to use timestamps
            existing_times = {stat["start"].timestamp() for stat in existing_stats.get(self.entity_id, [])}

            new_data = [
                {
                    "start": reading["entry_date"],
                    "state": reading["leitura"],
                    "sum": reading["leitura"]
                }
                for reading in readings
                if reading["codFuncao"] == self.funcao
                and not reading["isEstimativa"]
                and reading["entry_date"].timestamp() not in existing_times
            ]
            
            if new_data:
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
                            "has_sum": True,
                        },
                        new_data[i:i+batch_size]
                    )
                _LOGGER.info("Imported %d historical points for %s", len(new_data), self.entity_id)
            else:
                _LOGGER.info("No new historical data to import for %s", self.entity_id)
                
        except Exception as e:
            _LOGGER.error("Historical import failed for %s: %s", self.entity_id, str(e))
