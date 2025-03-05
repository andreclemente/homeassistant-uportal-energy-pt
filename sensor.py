import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import async_add_external_statistics
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
    
    # Register historical import service
    async def async_import_history(call):
        """Import historical data for all entities."""
        _LOGGER.info("Starting historical data import")
        for sensor in sensors:
            await sensor.async_import_historical_data()
    
    hass.services.async_register(DOMAIN, "import_history", async_import_history)

class UportalEnergyPtApiClient:
    def __init__(self, hass, config_entry):
        self.hass = hass
        self.config_entry = config_entry
        self.session = async_get_clientsession(hass)
        self.data = {}
        self.last_update = None
        self.auth_data = {
            "token": config_entry.data.get("token"),
            "expiry": config_entry.data.get("expiry", 0)
        }

    async def async_initialize(self):
        """Initialize auth state."""
        await self.async_refresh_token()

    async def async_refresh_token(self):
        """Handle token refresh with improved error tracking."""
        try:
            if not self.auth_data.get("token") or \
               datetime.now().timestamp() > self.auth_data.get("expiry", 0) - 300:
                
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
                    "token": data.get("token", {}).get("token"),
                    "expiry": data.get("token", {}).get("expirationDate", 0)
                })
                _LOGGER.info("Token refreshed successfully")

        except aiohttp.ClientResponseError as e:
            self.auth_data["token"] = None
            _LOGGER.error("Token refresh failed (HTTP %s): %s", e.status, e.message)
            raise
        except Exception as e:
            self.auth_data["token"] = None
            _LOGGER.error("Token refresh error: %s", str(e))
            raise

    async def async_update_data(self, counter):
        """Update sensor data with enhanced error handling."""
        retries = 3
        for attempt in range(retries):
            try:
                if not self.auth_data.get("token"):
                    await self.async_refresh_token()

                counter_num = counter.get("numeroContador")
                if not counter_num:
                    _LOGGER.error("Missing counter number in request")
                    return

                params = {
                    "codigoMarca": counter.get("codigoMarca"),
                    "codigoProduto": counter.get("codigoProduto"),
                    "dataFim": datetime.now().strftime("%Y-%m-%d"),
                    "dataInicio": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                    "numeroContador": counter_num,
                    "subscriptionId": self.config_entry.data.get("subscription_id")
                }
                
                _LOGGER.debug("Requesting data with params: %s", params)
                
                async with self.session.get(
                    f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                    headers={"X-Auth-Token": self.auth_data.get("token", "")},
                    params=params,
                    timeout=30
                ) as response:
                    content_type = response.headers.get('Content-Type', '').lower()

                    # Handle non-JSON responses
                    if 'application/json' not in content_type:
                        response_text = await response.text()
                        if response.status == 409:
                            _LOGGER.error("HTTP 409 Conflict: Invalidating token and retrying")
                            self.auth_data["token"] = None
                            continue
                            
                        _LOGGER.error("Non-JSON response (Status: %s, Content-Type: %s): %s", 
                                    response.status, content_type, response_text[:200])
                        continue

                    response.raise_for_status()
                    data = await response.json()

                    # Validate response structure
                    if not isinstance(data, list):
                        _LOGGER.error("Invalid data format received: %s", type(data))
                        continue

                    processed_readings = []
                    for entry in data:
                        try:
                            entry_date = datetime.fromisoformat(entry.get('data', '').replace('Z', '+00:00'))
                            for r in entry.get("leituras", []):
                                if "codFuncao" in r and "leitura" in r:
                                    processed_readings.append({
                                        "codFuncao": r["codFuncao"],
                                        "leitura": r["leitura"],
                                        "entry_date": entry_date,
                                        "isEstimativa": r.get("isEstimativa", False),
                                        "origem": r.get("origem", "Unknown")
                                    })
                        except (KeyError, ValueError) as e:
                            _LOGGER.debug("Error processing entry: %s", str(e))
                            continue

                    self.data[counter_num] = processed_readings
                    self.last_update = datetime.now()
                    return

            except aiohttp.ClientResponseError as e:
                _LOGGER.error("HTTP error %s: %s", e.status, e.message)
                if e.status in [401, 403, 409]:
                    self.auth_data["token"] = None
                if attempt < retries - 1:
                    await asyncio.sleep(2)
                    continue
                raise
            except asyncio.TimeoutError:
                _LOGGER.error("Request timed out")
                if attempt < retries - 1:
                    await asyncio.sleep(5)
                    continue
                raise
            except Exception as e:
                _LOGGER.error("Update failed: %s", str(e))
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                    continue
                raise

    async def async_get_historical_data(self, counter, full_history=False):
        """Retrieve historical data with extended date range."""
        try:
            params = {
                "codigoMarca": counter["codigoMarca"],
                "codigoProduto": counter["codigoProduto"],
                "numeroContador": counter["numeroContador"],
                "subscriptionId": self.config_entry.data.get("subscription_id")
            }
            
            if full_history:
                params.update({
                    "dataFim": datetime.now().strftime("%Y-%m-%d"),
                    "dataInicio": "2000-01-01"  # Get all available history
                })
            else:
                params.update({
                    "dataFim": datetime.now().strftime("%Y-%m-%d"),
                    "dataInicio": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
                })
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers={"X-Auth-Token": self.auth_data.get("token", "")},
                params=params,
                timeout=30
            ) as response:
                content_type = response.headers.get('Content-Type', '').lower()
                
                if response.status == 500:
                    _LOGGER.error("Server error 500 for %s", counter["numeroContador"])
                    return []

                if 'application/json' not in content_type:
                    return []

                response.raise_for_status()
                data = await response.json()

                processed_readings = []
                for entry in data:
                    try:
                        entry_date = datetime.fromisoformat(entry.get('data', '').replace('Z', '+00:00'))
                        for r in entry.get("leituras", []):
                            processed_readings.append({
                                "codFuncao": r.get("codFuncao"),
                                "leitura": r.get("leitura"),
                                "entry_date": entry_date,
                                "isEstimativa": r.get("isEstimativa", False)
                            })
                    except (KeyError, ValueError):
                        continue

                return processed_readings

        except Exception as e:
            _LOGGER.error("Historical data fetch failed: %s", str(e))
            return []

class UportalEnergyPtSensor(SensorEntity):
    """Entity class with historical data support."""
    
    def __init__(self, api, marca, numero, produto, funcao, descricao):
        self.api = api
        self.marca = marca
        self.numero = numero
        self.produto = produto
        self.funcao = funcao
        self.descricao = descricao
        self._attr_native_value = 0  # Default to 0 instead of Unknown
        
        # Validate required fields
        if not all([self.numero, self.produto, self.funcao]):
            _LOGGER.error("Invalid sensor initialization parameters")
            raise ValueError("Missing required sensor parameters")

        # Entity configuration
        self._attr_name = f"uPortal {PRODUCT_NAMES.get(produto, 'Utility')} {descricao} ({numero})"
        self._attr_unique_id = f"uportal_energy_pt_{produto}_{numero}_{funcao}"
        self._attr_has_entity_name = True
        self._attr_icon = self._get_icon()
        self._attr_native_unit_of_measurement = UNIT_MAP.get(produto)
        self._attr_state_class = "total_increasing"
        self._attr_device_class = {
            "EB": "energy",
            "GP": "gas",
            "AG": "water"
        }.get(produto)

    def _get_icon(self):
        """Return appropriate icon based on utility type."""
        return {
            "EB": "mdi:flash",
            "GP": "mdi:fire",
            "AG": "mdi:water"
        }.get(self.produto, "mdi:gauge")

    async def async_update(self):
        """Update sensor state with default to 0."""
        try:
            await self.api.async_update_data({
                "codigoMarca": self.marca,
                "numeroContador": self.numero,
                "codigoProduto": self.produto
            })
            
            all_readings = self.api.data.get(self.numero, [])
            self._attr_extra_state_attributes = {
                "counter_number": self.numero,
                "utility_type": PRODUCT_NAMES.get(self.produto, "Unknown"),
                "available_readings": len(all_readings)
            }

            if not all_readings:
                _LOGGER.debug("No data available for %s", self.numero)
                return  # Keep default 0 value
                
            # Find the latest valid reading
            latest_reading = None
            for reading in sorted(
                [r for r in all_readings if r.get("codFuncao") == self.funcao],
                key=lambda x: x.get("entry_date", datetime.min),
                reverse=True
            ):
                if not reading.get("isEstimativa", True):
                    latest_reading = reading
                    break
            
            if latest_reading:
                self._attr_native_value = latest_reading.get("leitura", 0)
                self._attr_last_updated = latest_reading.get("entry_date")

        except Exception as e:
            _LOGGER.error("Update failed for %s: %s", self.entity_id, str(e))
            # Maintain current value on error

    async def async_import_historical_data(self):
        """Import historical data into HA statistics."""
        _LOGGER.info("Starting historical import for %s", self.entity_id)
        
        # Get existing statistics
        recorder = get_instance(self.hass)
        stats = await recorder.async_add_executor_job(
            recorder.statistics_meta_get,
            self.entity_id
        )
        last_stat = stats[0].get("start") if stats else None
        
        # Get historical data from API
        counter = {
            "codigoMarca": self.marca,
            "codigoProduto": self.produto,
            "numeroContador": self.numero
        }
        readings = await self.api.async_get_historical_data(counter, full_history=True)
        
        # Prepare statistics data
        stats_data = []
        for reading in readings:
            if reading["codFuncao"] != self.funcao:
                continue
                
            start = reading["entry_date"]
            if last_stat and start.timestamp() <= last_stat:
                continue
                
            stats_data.append({
                "start": start,
                "state": reading["leitura"],
                "sum": reading["leitura"]
            })
        
        if stats_data:
            # Add statistics in batches
            batch_size = 100
            for i in range(0, len(stats_data), batch_size):
                await async_add_external_statistics(
                    self.hass,
                    {
                        "source": DOMAIN,
                        "name": self.name,
                        "statistic_id": self.entity_id,
                        "unit_of_measurement": self._attr_native_unit_of_measurement,
                        "has_sum": True,
                    },
                    stats_data[i:i+batch_size]
                )
            _LOGGER.info("Imported %d historical points for %s", len(stats_data), self.entity_id)