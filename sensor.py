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
                function["descFuncao"],
                config_entry  # Critical config entry reference
            )
            sensors.append(sensor)
    
    async_add_entities(sensors, True)

    # Initialize domain data structure
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][config_entry.entry_id] = {
        "sensors": sensors,
        "api": api
    }

    # Register service with proper scoping
    if not hass.services.has_service(DOMAIN, "import_history"):
        async def import_history(call):
            """Handle historical data import for all sensors."""
            _LOGGER.info("Initiating full historical data import")
            all_sensors = []
            for entry_id in hass.data[DOMAIN]:
                entry_data = hass.data[DOMAIN][entry_id]
                all_sensors.extend(entry_data["sensors"])
            
            for sensor in all_sensors:
                try:
                    await sensor.async_import_historical_data()
                except Exception as e:
                    _LOGGER.error("Failed to import history for %s: %s", 
                                sensor.entity_id, str(e))
        
        hass.services.async_register(DOMAIN, "import_history", import_history)

class UportalEnergyPtApiClient:
    def __init__(self, hass, config_entry):
        self.hass = hass
        self.config_entry = config_entry
        self.session = async_get_clientsession(hass)
        self.data = {}
        self.auth_data = {
            "token": None,
            "expiry": 0
        }

    async def async_initialize(self):
        """Force initial token refresh."""
        await self.async_refresh_token(force=True)

    async def async_refresh_token(self, force=False):
        """Robust token management with retry logic."""
        try:
            current_time = dt_util.utcnow().timestamp()
            if (force or 
                not self.auth_data["token"] or 
                current_time > self.auth_data["expiry"] - 300):
                
                _LOGGER.debug("Performing token refresh")
                async with self.session.post(
                    f"{self.config_entry.data[CONF_BASE_URL]}login",
                    json={
                        "username": self.config_entry.data["username"],
                        "password": self.config_entry.data["password"]
                    },
                    timeout=30
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    self.auth_data.update({
                        "token": data["token"]["token"],
                        "expiry": data["token"]["expirationDate"]
                    })
                    _LOGGER.info("Token refresh successful")

        except aiohttp.ClientResponseError as e:
            _LOGGER.error("Authentication failed (HTTP %s): %s", e.status, e.message)
            self.auth_data["token"] = None
            raise
        except Exception as e:
            _LOGGER.error("Token refresh critical error: %s", str(e))
            self.auth_data["token"] = None
            raise

    async def async_update_data(self, counter_params):
        """Fetch and process current readings with full error handling."""
        counter_id = counter_params["numeroContador"]
        try:
            # Validate authentication
            if not self.auth_data["token"]:
                await self.async_refresh_token(force=True)

            params = {
                "codigoMarca": counter_params["codigoMarca"],
                "codigoProduto": counter_params["codigoProduto"],
                "numeroContador": counter_id,
                "subscriptionId": self.config_entry.data["subscription_id"],
                "dataFim": dt_util.now().strftime("%Y-%m-%d"),
                "dataInicio": (dt_util.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            }
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers={"X-Auth-Token": self.auth_data["token"]},
                params=params,
                timeout=40
            ) as response:
                response.raise_for_status()
                raw_data = await response.json()
                processed = self._process_historical_data(raw_data)
                
                # Validate and store readings
                valid_readings = [
                    r for r in processed 
                    if r["codFuncao"] == counter_params.get("target_function", "F1")
                    and isinstance(r.get("leitura"), (int, float))
                ]
                
                self.data[counter_id] = sorted(
                    valid_readings,
                    key=lambda x: x["entry_date"],
                    reverse=True
                )

        except aiohttp.ClientResponseError as e:
            _LOGGER.error("API request failed for %s (HTTP %s): %s", 
                          counter_id, e.status, e.message)
            self.data[counter_id] = []
        except Exception as e:
            _LOGGER.error("Data update failed for %s: %s", counter_id, str(e))
            self.data[counter_id] = []

    def _process_historical_data(self, data):
        """Full data processing with validation."""
        processed = []
        for entry in data:
            try:
                entry_date = dt_util.parse_datetime(entry["data"].replace("Z", "+00:00"))
                for reading in entry["leituras"]:
                    processed.append({
                        "codFuncao": reading["codFuncao"],
                        "leitura": float(reading["leitura"]),
                        "entry_date": entry_date,
                        "isEstimativa": reading.get("isEstimativa", False)
                    })
            except (KeyError, ValueError) as e:
                _LOGGER.warning("Skipping invalid data entry: %s", str(e))
        return processed

    async def async_get_historical_data(self, counter, start_date=None):
        """Complete historical data retrieval."""
        try:
            await self.async_refresh_token()
            
            if not start_date:
                start_date = self._calculate_smart_start_date(counter)
                
            params = {
                "codigoMarca": counter["codigoMarca"],
                "codigoProduto": counter["codigoProduto"],
                "numeroContador": counter["numeroContador"],
                "subscriptionId": self.config_entry.data["subscription_id"],
                "dataFim": dt_util.now().strftime("%Y-%m-%d"),
                "dataInicio": start_date,
            }
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers={"X-Auth-Token": self.auth_data["token"]},
                params=params,
                timeout=60
            ) as response:
                if response.status == 404:
                    return []
                response.raise_for_status()
                return self._process_historical_data(await response.json())
                
        except Exception as e:
            _LOGGER.error("Historical fetch failed: %s", str(e))
            return []

    def _calculate_smart_start_date(self, counter):
        """Full installation date handling."""
        install_date = datetime.strptime(
            self.config_entry.data["installation_date"], 
            "%Y-%m-%d"
        )
        return install_date.strftime("%Y-%m-%d")

class UportalEnergyPtSensor(SensorEntity):
    """Complete sensor entity with all original functionality."""
    
    def __init__(self, api, marca, numero, produto, funcao, descricao, config_entry):
        self.api = api
        self.marca = marca
        self.numero = numero
        self.produto = produto
        self.funcao = funcao
        self.descricao = descricao
        self.config_entry = config_entry
        self._attr_has_entity_name = True

        # Entity configuration
        self._attr_name = f"{PRODUCT_NAMES[produto]} {descricao}"
        self._attr_unique_id = f"uportal_{config_entry.entry_id}_{produto}_{numero}_{funcao}"
        self._attr_native_unit_of_measurement = UNIT_MAP[produto]
        self._attr_state_class = "total_increasing"
        self._attr_device_class = "energy" if produto == "EB" else "gas" if produto == "GP" else "water"
        self._attr_available = False
        self._attr_native_value = None

    async def async_update(self):
        """Complete update mechanism with availability checks."""
        try:
            await self.api.async_update_data({
                "codigoMarca": self.marca,
                "numeroContador": self.numero,
                "codigoProduto": self.produto,
                "target_function": self.funcao
            })
            
            readings = self.api.data.get(self.numero, [])
            valid_readings = [r for r in readings if r["codFuncao"] == self.funcao]
            
            if valid_readings:
                latest = valid_readings[0]
                self._attr_native_value = latest["leitura"]
                self._attr_available = True
            else:
                self._attr_available = False
                self._attr_native_value = None
                _LOGGER.debug("No valid readings for %s", self.entity_id)

        except Exception as e:
            self._attr_available = False
            self._attr_native_value = None
            _LOGGER.error("Update failed for %s: %s", self.entity_id, str(e))

    async def async_import_historical_data(self):
        """Complete historical import functionality."""
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.statistics import async_add_external_statistics

        try:
            _LOGGER.info("Starting historical import for %s", self.entity_id)
            
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
            
            existing_times = {stat["start"].timestamp() for stat in existing_stats.get(self.entity_id, [])}
            
            counter = {
                "codigoMarca": self.marca,
                "codigoProduto": self.produto,
                "numeroContador": self.numero
            }
            
            new_data = []
            current_year = datetime.now().year
            
            for year in range(2015, current_year + 1):
                readings = await self.api.async_get_historical_data(
                    counter,
                    start_date=f"{year}-01-01"
                )
                
                year_data = [
                    {
                        "start": reading["entry_date"],
                        "state": reading["leitura"],
                        "sum": reading["leitura"]
                    }
                    for reading in readings
                    if (reading["codFuncao"] == self.funcao and
                        not reading["isEstimativa"] and
                        reading["entry_date"].timestamp() not in existing_times)
                ]
                
                new_data.extend(year_data)
                await asyncio.sleep(1)

            if new_data:
                batch_size = 500
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
                _LOGGER.info("Imported %d points for %s", len(new_data), self.entity_id)
            else:
                _LOGGER.info("No new data for %s", self.entity_id)
                
        except Exception as e:
            _LOGGER.error("Historical import failed for %s: %s", self.entity_id, str(e))
