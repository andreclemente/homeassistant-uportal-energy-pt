from datetime import timedelta

DOMAIN = "uportal_energy_pt"
CONF_BASE_URL = "base_url"
SCAN_INTERVAL = timedelta(hours=12)

UNIT_MAP = {
    "EB": "kWh",  # Electricity
    "GP": "m³",   # Gas
    "AG": "m³"    # Water
}

PRODUCT_NAMES = {
    "EB": "Electricity",
    "GP": "Gas",
    "AG": "Water"
}