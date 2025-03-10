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
from .const import DOMAIN, CONF_BASE_URL, UNIT_MAP, PRODUCT_NAMES

_LOGGER = logging.getLogger(__name__)

# Sanitize statistic_id - remove all non-safe characters
def sanitize_stat_id(input_str):
    # Convert to lowercase FIRST, then replace invalid characters
    return re.sub(r'[^a-z0-9_]', '_', input_str.lower())

async def async_setup_entry(hass, config_entry, async_add_entities):
    """Set up integration entry point."""
    if not config_entry.data.get("counters"):
        _LOGGER.error("No counters configured in config entry")
        return True
    try:
        api = UportalEnergyPtApiClient(hass, config_entry)
        await api.async_initialize()
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
                try:
                    await sensor.async_import_historical_data()
                except Exception as e:
                    _LOGGER.error("Failed to import history for %s: %s", 
                                sensor.entity_id, str(e))
        hass.services.async_register(DOMAIN, "import_history", import_history)
    return True  # Explicit return statement

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
                    # Handle expiration date (supports Unix timestamp or ISO string)
                    expiry_value = data["token"]["expirationDate"]
                    if isinstance(expiry_value, (int, float)):
                        # Unix timestamp in milliseconds
                        expiry_date = dt_util.utc_from_timestamp(expiry_value / 1000)
                    else:
                        # ISO string
                        expiry_date = parse_datetime(expiry_value)
                    self.auth_data.update({
                        "token": data["token"]["token"],
                        "expiry": expiry_date.timestamp()
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
            for attempt in range(2):
                try:
                    async with self.session.get(
                        f"{self.config_entry.data[CONF_BASE_URL]}History/getHistoricoLeiturasComunicadas",
                        headers={"X-Auth-Token": self.auth_data["token"]},
                        params=params,
                        timeout=40
                    ) as response:
                        # Check content type
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' not in content_type:
                            _LOGGER.error("Unexpected content type %s for counter %s", content_type, counter_id)
                            raise aiohttp.ClientResponseError(
                                response.request_info,
                                response.history,
                                status=500,
                                message=f"Unexpected content type: {content_type}",
                                headers=response.headers
                            )
                        response.raise_for_status()
                        raw_data = await response.json()
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
                        break  # Exit retry loop on success
                except aiohttp.ClientResponseError as e:
                    if e.status == 401 and attempt == 0:
                        _LOGGER.debug("Token expired, refreshing and retrying")
                        await self.async_refresh_token(force=True)
                        continue
                    else:
                        raise
        except Exception as e:
            _LOGGER.error("Data update failed for %s: %s", counter_id, str(e))
            self.data[counter_id] = []

    def _process_historical_data(self, data):
        """Handle both ISO strings and Unix timestamps."""
        processed = []
        for entry in data:
            try:
                # Handle different date formats
                date_str = entry.get("data")
                if isinstance(date_str, (int, float)):
                    # Unix timestamp in milliseconds
                    entry_date = dt_util.utc_from_timestamp(date_str / 1000)
                else:
                    # ISO string with Zulu time
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

    async def async_get_historical_data(self, counter, start_date=None):
        """Complete historical data retrieval."""
        for attempt in range(2):
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
                    if response.status in (404, 500):
                        _LOGGER.warning("Server returned %s for %s", response.status, counter["numeroContador"])
                        return []
                    # Check content type
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' not in content_type:
                        _LOGGER.error("Unexpected content type: %s", content_type)
                        return []
                    response.raise_for_status()
                    data = await response.json()
                    return self._process_historical_data(data)
            except aiohttp.ClientResponseError as e:
                if e.status == 401 and attempt == 0:
                    await self.async_refresh_token(force=True)
                    continue
                else:
                    _LOGGER.error("HTTP error %s for counter %s: %s", e.status, counter["numeroContador"], e.message)
                    return []
            except Exception as e:
                _LOGGER.error("Historical fetch failed for %s: %s", counter["numeroContador"], str(e))
                return []
        return []

    def _calculate_smart_start_date(self, counter):
        """Improved date handling with validation."""
        try:
            if counter["codigoProduto"] == "GP":
                start_date = dt_util.now() - timedelta(days=365)
            else:
                install_date_str = self.config_entry.data["installation_date"]
                install_date = datetime.strptime(install_date_str, "%Y-%m-%d")
                start_date = max(
                    install_date,
                    dt_util.now() - timedelta(days=365*10)  # 10 years max
                )
            return start_date.strftime("%Y-%m-%d")
        except Exception as e:
            _LOGGER.error("Invalid installation date: %s", str(e))
            return (dt_util.now() - timedelta(days=365)).strftime("%Y-%m-%d")

class UportalEnergyPtSensor(SensorEntity):
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
        # Enhanced ID sanitization
        safe_entry_id = sanitize_stat_id(config_entry.entry_id)
        self._attr_unique_id = f"uportal_{safe_entry_id}_{produto.lower()}_{numero}_{funcao.lower()}"
        self._attr_statistic_id = f"{DOMAIN}:{self._attr_unique_id}"  # Prefix with integration domain
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
        """Improved historical import with recorder checks."""
        if not recorder.is_connected():
            _LOGGER.error("Recorder is not connected. Cannot import history.")
            return
        
        try:
            recorder_instance = get_instance(self.hass)
            if not recorder_instance:
                raise RuntimeError("Recorder instance not available")
        
            # Validate counter configuration
            if not all([self.marca, self.numero, self.produto]):
                raise ValueError("Invalid counter configuration")
            from homeassistant.components.recorder import get_instance
            from homeassistant.components.recorder.statistics import async_add_external_statistics, statistics_during_period
            _LOGGER.info("Starting historical import for %s", self.entity_id)
            # Use valid start date (January 1, 1970)
            start_time = datetime(1970, 1, 1, tzinfo=dt_util.UTC)
            end_time = dt_util.now()
            existing_stats = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                start_time,
                end_time,
                [self._attr_statistic_id],  # <- Use statistic_id instead of entity_id
                "day",
                None,
                {"state", "sum"}
            )
            # Handle invalid dates in existing stats
            existing_times = set()
            for stat in existing_stats.get(self._attr_statistic_id, []):  # <- Use statistic_id here
                start_value = stat.get("start")
                parsed_time = None
                try:
                    # Handle multiple data types
                    if isinstance(start_value, str):
                        parsed_time = dt_util.parse_datetime(start_value)
                    elif isinstance(start_value, (int, float)):
                        parsed_time = dt_util.utc_from_timestamp(start_value)
                    elif isinstance(start_value, datetime):
                        parsed_time = start_value
                    else:
                        _LOGGER.warning("Unsupported start type: %s", type(start_value))
                        continue
                    if parsed_time:
                        existing_times.add(parsed_time.timestamp())
                except Exception as e:
                    _LOGGER.warning("Skipped invalid date in stats: %s", str(e))
                    continue
            counter = {
                "codigoMarca": self.marca,
                "codigoProduto": self.produto,
                "numeroContador": self.numero
            }
            new_data = []
            current_year = datetime.now().year
            for year in range(2015, current_year + 1):
                # Retry logic for historical fetch
                for attempt in range(2):
                    try:
                        readings = await self.api.async_get_historical_data(
                            counter,
                            start_date=f"{year}-01-01"
                        )
                        break
                    except aiohttp.ClientResponseError as e:
                        if e.status == 401 and attempt == 0:
                            await self.api.async_refresh_token(force=True)
                            continue
                        else:
                            raise
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
                await async_add_external_statistics(
                    self.hass,
                    {
                        "source": DOMAIN,
                        "name": self.name,
                        "statistic_id": self._attr_statistic_id,  # Directly use the attribute
                        "unit_of_measurement": self._attr_native_unit_of_measurement,
                    },
                    new_data
                )
                _LOGGER.info("Imported %d points for %s", len(new_data), self.entity_id)
            else:
                _LOGGER.info("No new data for %s", self.entity_id)
        except Exception as e:
            _LOGGER.error("Historical import failed for %s: %s (Parameters: marca=%s, numero=%s, produto=%s)",
                        self.entity_id, str(e), self.marca, self.numero, self.produto)
