import voluptuous as vol
import aiohttp
import logging
import asyncio
from datetime import datetime, timedelta
from homeassistant import config_entries
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from .const import DOMAIN, CONF_BASE_URL, UNIT_MAP, PRODUCT_NAMES

_LOGGER = logging.getLogger(__name__)

class UportalEnergyPtConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1
    subscription_data = {}
    counters_data = {}

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            try:
                # Validate and store base URL
                base_url = user_input[CONF_BASE_URL].strip().rstrip("/") + "/"
                if not base_url.startswith(("http://", "https://")):
                    raise ValueError("Invalid URL format")
                
                self.base_url = base_url
                return await self.async_step_credentials()

            except Exception as e:
                errors["base"] = "invalid_url"
                _LOGGER.error("URL validation failed: %s", str(e))

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_BASE_URL): str
            }),
            errors=errors
        )

    async def async_step_credentials(self, user_input=None):
        errors = {}
        if user_input is not None:
            try:
                session = async_get_clientsession(self.hass)
                
                # Authenticate
                auth_response = await session.post(
                    f"{self.base_url}login",
                    json={"username": user_input["username"], "password": user_input["password"]}
                )
                auth_response.raise_for_status()
                auth_data = await auth_response.json()
                
                # Get subscriptions
                subs_response = await session.get(
                    f"{self.base_url}Subscription/listSubscriptions",
                    headers={"X-Auth-Token": auth_data["token"]["token"]}
                )
                subs_response.raise_for_status()
                subscriptions = await subs_response.json()
                
                # Store temporary data
                self.subscription_data = {
                    "base_url": self.base_url,
                    "auth": auth_data,
                    "subscriptions": subscriptions,
                    "credentials": user_input
                }
                
                return await self.async_step_select_subscription()

            except aiohttp.ClientResponseError as e:
                errors["base"] = "invalid_auth" if e.status == 401 else "api_error"
            except Exception as e:
                errors["base"] = "connection_error"
                _LOGGER.exception("Authentication failed: %s", str(e))

        return self.async_show_form(
            step_id="credentials",
            data_schema=vol.Schema({
                vol.Required("username"): str,
                vol.Required("password"): str
            }),
            errors=errors
        )

    async def async_step_select_subscription(self, user_input=None):
        subscriptions = self.subscription_data.get("subscriptions", [])
        if not subscriptions:
            return self.async_abort(reason="no_subscriptions")
            
        if len(subscriptions) == 1:
            user_input = {"subscription": "0"}
            return await self.async_step_select_counter(user_input)

        options = {
            str(i): f"{sub.get('description', 'Unknown')} ({sub['subscriptionId']})" 
            for i, sub in enumerate(subscriptions)
        }
        
        return self.async_show_form(
            step_id="select_subscription",
            data_schema=vol.Schema({
                vol.Required("subscription"): vol.In(options)
            })
        )

    async def async_step_select_counter(self, user_input=None):
        if user_input is None:
            return self.async_abort(reason="invalid_subscription")

        try:
            session = async_get_clientsession(self.hass)
            sub_index = int(user_input["subscription"])
            selected_sub = self.subscription_data["subscriptions"][sub_index]
            
            # Get counters
            counters_response = await session.get(
                f"{self.base_url}leituras/getContadores",
                headers={"X-Auth-Token": self.subscription_data["auth"]["token"]["token"]},
                params={"subscriptionId": selected_sub["subscriptionId"]}
            )
            counters_response.raise_for_status()
            counters = await counters_response.json()
            
            self.counters_data = {
                "selected_sub": selected_sub,
                "counters": counters,
                "auth": self.subscription_data["auth"],
                "credentials": self.subscription_data["credentials"]
            }
            
            return await self.async_step_select_counters()

        except Exception as e:
            _LOGGER.error("Counter selection failed: %s", str(e))
            return self.async_abort(reason="config_error")

    async def async_step_select_counters(self, user_input=None):
        counters = self.counters_data.get("counters", [])
        options = {
            str(i): f"{PRODUCT_NAMES.get(c['chaveContador']['codigoProduto'], 'Unknown')} ({c['chaveContador']['numeroContador']})"
            for i, c in enumerate(counters)
        }
        
        if user_input is None:
            return self.async_show_form(
                step_id="select_counters",
                data_schema=vol.Schema({
                    vol.Required("counters"): cv.multi_select(options)
                })
            )
            
        try:
            selected_counters = [counters[int(idx)] for idx in user_input["counters"]]
            return self.async_create_entry(
                title=f"uPortal {self.counters_data['selected_sub']['subscriptionId']}",
                data={
                    CONF_BASE_URL: self.base_url,
                    **self.counters_data["credentials"],
                    "token": self.counters_data["auth"]["token"]["token"],
                    "expiry": self.counters_data["auth"]["token"]["expirationDate"],
                    "subscription_id": self.counters_data["selected_sub"]["subscriptionId"],
                    "counters": [
                        {
                            "codigoMarca": c["chaveContador"]["codigoMarca"],
                            "numeroContador": c["chaveContador"]["numeroContador"],
                            "codigoProduto": c["chaveContador"]["codigoProduto"],
                            "functions": [
                                {
                                    "codigoFuncao": f["codigoFuncao"],
                                    "descFuncao": f["descFuncao"].strip()
                                } 
                                for f in c["funcoesContador"]
                            ]
                        }
                        for c in selected_counters
                    ]
                }
            )
        except Exception as e:
            _LOGGER.error("Final configuration failed: %s", str(e))
            return self.async_abort(reason="config_error")