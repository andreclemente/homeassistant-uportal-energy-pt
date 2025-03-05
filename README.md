# uPortal Energy PT

## Overview
**uPortal Energy PT** is a Home Assistant custom integration for managing and analyzing energy consumption data in Portugal. It provides an interface to process, visualize, and extract insights from smart meter readings, enabling better energy efficiency and consumption tracking.

## Features
- Retrieve real-time and historical energy consumption data
- Integrate seamlessly with Home Assistant
- Support for Portuguese energy providers
- Custom sensors for tracking and automation
- Generate reports and export data

## Installation
### Requirements
- Home Assistant (latest version recommended)
- HACS (Home Assistant Community Store) for easier installation (optional but recommended)

### Steps
#### Installation via HACS (Recommended)
1. Open Home Assistant and go to **HACS**.
2. Search for **uPortal Energy PT**.
3. Click **Install** and restart Home Assistant.

#### Manual Installation
1. Navigate to the `custom_components` directory in your Home Assistant configuration folder:
   ```sh
   cd ~/.homeassistant/custom_components
   ```
2. Clone the repository:
   ```sh
   git clone https://github.com/andreclemente/homeassistant-uportal-energy-pt.git uportal_energy_pt
   ```
3. Restart Home Assistant.

## Configuration
1. In Home Assistant, go to **Settings > Integrations**.
2. Click **Add Integration** and search for **uPortal Energy PT**.
3. Follow the setup instructions to connect to your energy provider.
Here's an example for the url: "https://portal.ucloud.cgi.com/uPortal2/smasleiria/"

## Usage
- Access the integration's sensors in **Developer Tools > States**.
- Create automation rules based on energy consumption.
- View energy trends and reports in Home Assistant dashboards.

## Contributing
Feel free to submit issues, suggestions, or pull requests.

## License
MIT License

