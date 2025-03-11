import logging
import re
import aiohttp
import asyncio
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.util import dt as dt_util
from homeassistant.exceptions import ConfigEntryNotReady
from json.decoder import JSONDecodeError
from .const import DOMAIN, CONF_BASE_URL, UNIT_MAP, PRODUCT_NAMES

_LOGGER = logging.getLogger(__name__)

def sanitize_stat_id(input_str):
    return re.sub(r'[^a-z0-9_]', '_', input_str.lower())

async def async_setup_entry(hass, config_entry, async_add_entities):
    if not config_entry.data.get("counters"):
        _LOGGER.error("No counters configured in config entry")
        return True
    try:
        api = UportalEnergyPtApiClient(hass, config_entry)
        await api.async_initialize()
        if not api.auth_data["token"]:  # Add validation for successful auth
            raise ConfigEntryNotReady("Failed to obtain initial authentication token")
    except Exception as e:
        raise ConfigEntryNotReady(f"API initialization failed: {str(e)}") from e
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
                config_entry
            )
            sensors.append(sensor)
    async_add_entities(sensors, True)
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][config_entry.entry_id] = {
        "sensors": sensors,
        "api": api
    }
    if not hass.services.has_service(DOMAIN, "import_history"):
        async def import_history(call):
            _LOGGER.info("Initiating full historical data import")
            all_sensors = []
            for entry_id in hass.data[DOMAIN]:
                entry_data = hass.data[DOMAIN][entry_id]
                all_sensors.extend(entry_data["sensors"])
            for sensor in all_sensors:
                if sensor.api is None:  # Add safety check
                    _LOGGER.error("Sensor API unavailable: %s", sensor.entity_id)
                    continue
                try:
                    await sensor.async_import_historical_data()
                except Exception as e:
                    _LOGGER.error("Failed to import history for %s: %s", 
                                sensor.entity_id, str(e))
        hass.services.async_register(DOMAIN, "import_history", import_history)
    return True

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
        await self.async_refresh_token(force=True)

    async def async_refresh_token(self, force=False):
        try:
            current_time = dt_util.utcnow().timestamp()
            if (force or 
                not self.auth_data["token"] or 
                current_time > self.auth_data["expiry"] - 300):  # Refresh 5 minutes early
                _LOGGER.debug("Performing token refresh")
                for attempt in range(3):  # Retry on server errors
                    try:
                        async with self.session.post(
                            f"{self.config_entry.data[CONF_BASE_URL]}login",
                            json={
                                "username": self.config_entry.data["username"],
                                "password": self.config_entry.data["password"]
                            },
                            timeout=30
                        ) as response:
                            # Check for HTML responses (critical fix)
                            if "text/html" in response.headers.get("Content-Type", ""):
                                _LOGGER.error("HTML response during token refresh - invalid credentials?")
                                self.auth_data["token"] = None
                                raise ValueError("Invalid credentials or server error")
                            response.raise_for_status()
                            data = await response.json()
                            # Validate token structure
                            if not data.get("token") or not data["token"].get("token"):
                                raise ValueError("Invalid token response format")
                            expiry_value = data["token"]["expirationDate"]
                            try:
                                if isinstance(expiry_value, (int, float)):
                                    expiry_date = dt_util.utc_from_timestamp(expiry_value / 1000)
                                else:
                                    expiry_date = parse_datetime(expiry_value.replace("Z", "+00:00"))
                            except Exception as e:
                                _LOGGER.error("Invalid expiry date format: %s. Using 1 hour default.", str(e))
                                expiry_date = dt_util.utcnow() + timedelta(hours=1)
                            self.auth_data.update({
                                "token": data["token"]["token"],
                                "expiry": expiry_date.timestamp()
                            })
                            _LOGGER.info("Token refresh successful")
                            break  # Exit retry loop on success
                    except aiohttp.ClientResponseError as e:
                        if e.status >= 500 and attempt < 2:
                            await asyncio.sleep(5)
                            continue
                        _LOGGER.error("Authentication failed (HTTP %s): %s", e.status, e.message)
                        self.auth_data["token"] = None
                        raise
                    except Exception as e:
                        _LOGGER.error("Token refresh error (attempt %d): %s", attempt+1, str(e))
                        if attempt == 2:
                            raise
                        await asyncio.sleep(3)
        except Exception as e:
            _LOGGER.error("Token refresh critical error: %s", str(e))
            self.auth_data["token"] = None
            raise

    async def async_update_data(self, counter_params):
        counter_id = counter_params["numeroContador"]
        try:
            if not self.auth_data["token"]:
                await self.async_refresh_token(force=True)
            # Validate parameters to prevent None values
            required_params = {
                "codigoMarca": str,
                "codigoProduto": str,
                "numeroContador": str,
            }
            for param, param_type in required_params.items():
                value = counter_params.get(param)
                if not isinstance(value, param_type):
                    raise ValueError(f"Invalid type for {param}. Expected {param_type}, got {type(value)}")
            # Get subscription_id from config and enforce string type
            subscription_id = self.config_entry.data.get("subscription_id")
            if not subscription_id:
                raise ValueError("subscription_id is missing in configuration")
            params = {
                "codigoMarca": counter_params["codigoMarca"],
                "codigoProduto": counter_params["codigoProduto"],
                "numeroContador": counter_id,
                "subscriptionId": subscription_id,  # Now explicitly a string
                "dataFim": dt_util.as_local(dt_util.now()).strftime("%Y-%m-%d"),
                "dataInicio": dt_util.as_local(dt_util.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
            }
            # Add request tracing for conflict errors
            request_id = f"{counter_id}-{int(dt_util.utcnow().timestamp())}"
            _LOGGER.debug("Making request %s with params: %s", request_id, params)
            for attempt in range(5):  # Increased retry attempts
                try:
                    async with self.session.get(
                        f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                        headers={"X-Auth-Token": self.auth_data["token"]},
                        params=params,
                        timeout=40
                    ) as response:
                        response_text = await response.text()
                        # Check for HTML responses
                        if "text/html" in response.headers.get("Content-Type", ""):
                            _LOGGER.error("Received HTML response instead of JSON. Token may be invalid.")
                            await self.async_refresh_token(force=True)
                            raise ValueError("Invalid API response format")
                        if response.status == 409:
                            _LOGGER.warning("Conflict error on request %s. Details: %s", 
                                          request_id, response_text)
                            await asyncio.sleep(10)  # Longer delay for conflicts
                            continue
                        response.raise_for_status()
                        # Attempt JSON parsing
                        try:
                            raw_data = await response.json()
                        except (JSONDecodeError, aiohttp.ContentTypeError):
                            _LOGGER.error("Failed to parse JSON response: %s", response_text)
                            raw_data = []
                        processed = self._process_historical_data(raw_data)
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
                        break
                except aiohttp.ClientResponseError as e:
                    _LOGGER.warning("Request %s failed (attempt %d): %s", 
                                  request_id, attempt+1, str(e))
                    if e.status == 401:
                        await self.async_refresh_token(force=True)
                    await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
        except Exception as e:
            _LOGGER.error("Data update failed for %s: %s", counter_id, str(e))
            self.data[counter_id] = []

    def _process_historical_data(self, data):
        processed = []
        if not data:
            return processed
        for entry in data:
            try:
                date_str = entry.get("data")
                if isinstance(date_str, (int, float)):
                    entry_date = dt_util.utc_from_timestamp(date_str / 1000)
                else:
                    date_str = date_str.replace("Z", "+00:00")
                    entry_date = dt_util.parse_datetime(date_str)
                for reading in entry["leituras"]:
                    processed.append({
                        "codFuncao": reading["codFuncao"],
                        "leitura": float(reading["leitura"]),
                        "entry_date": entry_date,
                        "isEstimativa": reading.get("isEstimativa", False)
                    })
            except (KeyError, ValueError, TypeError) as e:
                _LOGGER.warning("Invalid data entry: %s", str(e))
        return processed

    async def async_get_historical_data(self, counter, start_date=None, headers=None):
        """Guaranteed to return list even with API errors"""
        try:
            await self.async_refresh_token()
            if not start_date:
                start_date = self._calculate_smart_start_date(counter)
            
            # Ensure all parameters are strings
            params = {
                "codigoMarca": str(counter["codigoMarca"]),
                "codigoProduto": str(counter["codigoProduto"]),
                "numeroContador": str(counter["numeroContador"]),
                "subscriptionId": self.config_entry.data["subscription_id"],
                "dataFim": dt_util.as_local(dt_util.now()).strftime("%Y-%m-%d"),
                "dataInicio": start_date,
            }
            
            # Use provided headers or fallback to default
            final_headers = headers or {"X-Auth-Token": self.auth_data["token"]}
            
            async with self.session.get(
                f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                headers=final_headers,
                params=params,
                timeout=60
            ) as response:
                response_text = await response.text()
                
                # Check for HTML responses
                if "text/html" in response.headers.get("Content-Type", ""):
                    _LOGGER.error("Received HTML response instead of JSON. Token may be invalid.")
                    await self.async_refresh_token(force=True)
                    raise ValueError("Invalid API response format")
                
                if response.status in (404, 500, 409):
                    _LOGGER.warning("Server error %s: %s", response.status, response_text)
                    return []
                
                try:
                    data = await response.json()
                except (JSONDecodeError, aiohttp.ContentTypeError):
                    _LOGGER.error("Non-JSON response (truncated): %.200s", response_text)
                    return []
                
                return self._process_historical_data(data) or []
        except Exception as e:
            _LOGGER.error("API failure: %s", str(e))
            return []

    def _calculate_smart_start_date(self, counter):
        try:
            if counter["codigoProduto"] == "GP":
                start_date = dt_util.now() - timedelta(days=365)
            else:
                install_date_str = self.config_entry.data["installation_date"]
                install_date = datetime.strptime(install_date_str, "%Y-%m-%d")
                start_date = max(
                    install_date,
                    dt_util.now() - timedelta(days=365*10)
                )
            return start_date.strftime("%Y-%m-%d")
        except Exception as e:
            _LOGGER.error("Invalid installation date: %s", str(e))
            return (dt_util.now() - timedelta(days=365)).strftime("%Y-%m-%d")

class UportalEnergyPtSensor(SensorEntity):
    def __init__(self, api, marca, numero, produto, funcao, descricao, config_entry):
        if not api or not isinstance(api, UportalEnergyPtApiClient):
            raise ValueError("Invalid API client provided to sensor")
        self.api = api
        self.marca = marca
        self.numero = numero
        self.produto = produto
        self.funcao = funcao
        self.descricao = descricao
        self.config_entry = config_entry
        self.hass = api.hass  # Critical fix: Add reference to hass
        self._attr_has_entity_name = True
        self._attr_name = f"{PRODUCT_NAMES[produto]} {descricao}"
        safe_entry_id = sanitize_stat_id(config_entry.entry_id)
        self._attr_unique_id = f"uportal_{safe_entry_id}_{produto.lower()}_{numero}_{funcao.lower()}"
        self._attr_statistic_id = f"{DOMAIN}:{self._attr_unique_id}"
        self._attr_native_unit_of_measurement = UNIT_MAP[produto]
        self._attr_state_class = "total_increasing"
        self._attr_device_class = "energy" if produto == "EB" else "gas" if produto == "GP" else "water"
        self._attr_available = False
        self._attr_native_value = None

    async def async_update(self):
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
        try:
            from homeassistant.components.recorder import get_instance
            from homeassistant.components.recorder.statistics import (
                async_add_external_statistics,
                statistics_during_period,
            )
            # Add pre-flight checks
            if not self.api or not hasattr(self.api, 'async_get_historical_data'):
                _LOGGER.error("API handler not properly initialized for %s", self.entity_id)
                return
            if not self.api.auth_data.get("token"):
                _LOGGER.debug("Refreshing token before historical import for %s", self.entity_id)
                await self.api.async_refresh_token(force=True)
            # Add connection test
            try:
                await self.api.session.head(self.api.config_entry.data[CONF_BASE_URL], timeout=10)
            except Exception as e:
                _LOGGER.error("Network connection unavailable for %s: %s", self.entity_id, str(e))
                return
            recorder_instance = get_instance(self.hass)
            if not recorder_instance:
                _LOGGER.error("Recorder not available for %s", self.entity_id)
                return
            start_time = datetime(1970, 1, 1, tzinfo=dt_util.UTC)
            end_time = dt_util.now()
            existing_stats = await recorder_instance.async_add_executor_job(
                statistics_during_period,
                self.hass,
                start_time,
                end_time,
                [self._attr_statistic_id],
                "day",
                None,
                {"state", "sum"}
            )
            existing_times = set()
            for stat in existing_stats.get(self._attr_statistic_id, []):
                start_value = stat.get("start")
                parsed_time = None
                try:
                    if isinstance(start_value, str):
                        parsed_time = dt_util.parse_datetime(start_value)
                    elif isinstance(start_value, (int, float)):
                        parsed_time = dt_util.utc_from_timestamp(start_value)
                    elif isinstance(start_value, datetime):
                        parsed_time = start_value
                    else:
                        continue
                    if parsed_time:
                        existing_times.add(parsed_time.timestamp())
                except Exception as e:
                    continue
            counter = {
                "codigoMarca": self.marca,
                "codigoProduto": self.produto,
                "numeroContador": self.numero
            }
            new_data = []
            current_year = datetime.now().year  # Define current_year
            for year in range(2015, current_year + 1):
                for attempt in range(3):  # Increased retry attempts
                    try:
                        if not self.api.auth_data.get("token"):
                            await self.api.async_refresh_token(force=True)
                        # Ensure headers use the refreshed token
                        headers = {"X-Auth-Token": self.api.auth_data["token"]}
                        readings = await self.api.async_get_historical_data(
                            counter,
                            f"{year}-01-01",
                            headers=headers
                        ) or []  # Ensure readings is always a list
                        _LOGGER.debug("Fetched %d entries for %s year %s", len(readings), self.entity_id, year)
                        break
                    except aiohttp.ClientResponseError as e:
                        if e.status == 401 and attempt == 0:
                            await self.api.async_refresh_token(force=True)
                            continue
                        else:
                            _LOGGER.error("API error for %s (year %s): %s", self.entity_id, year, str(e))
                            readings = []
                            break
                    except Exception as e:
                        _LOGGER.error("Temporary failure in %s (year %s): %s", self.entity_id, year, str(e))
                        readings = []
                        break
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
                await asyncio.sleep(3)  # Increased delay between years to avoid rate limiting
            if new_data:
                # Use StatisticMetaData class for strict type validation
                from homeassistant.components.recorder.statistics import StatisticMetaData
                metadata = StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    name=self.name,
                    source=DOMAIN,
                    statistic_id=self._attr_statistic_id,
                    unit_of_measurement=self._attr_native_unit_of_measurement
                )
                await async_add_external_statistics(self.hass, metadata, new_data)
                _LOGGER.info("Imported %d points for %s", len(new_data), self.entity_id)
            else:
                _LOGGER.info("No new data for %s", self.entity_id)
        except Exception as e:
            _LOGGER.error("Historical import failed for %s: %s (Parameters: marca=%s, numero=%s, produto=%s)",
                        self.entity_id, str(e), self.marca, self.numero, self.produto)
