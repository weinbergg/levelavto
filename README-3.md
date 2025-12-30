# Encar Parser – Official Python Client Parser for Carapis Encar API

[![PyPI version](https://badge.fury.io/py/encar.svg)](https://pypi.org/project/encar/)
[![API Docs](https://img.shields.io/badge/API%20Docs-Carapis%20Encar%20API-blue)](https://carapis.com/docs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Carapis Catalog](https://img.shields.io/badge/Live%20Catalog-Carapis.com-green)](https://carapis.com/catalog)

**Encar Parser** is the official Python client for the **Carapis Encar API**, acting as a powerful parser to provide seamless programmatic access to real-time Korean used car data from Encar.com. With the `encar` parser library, you can easily query, filter, and analyze vehicle listings, manufacturers, models, and more – all powered by the robust **Carapis Encar API** provided by Carapis.com.

Explore a live catalog powered by this **Carapis Encar API**: [Carapis Catalog](https://carapis.com/catalog)

## Features

- Easy access and parsing of real-time Encar.com vehicle data via the **Carapis Encar API** client parser.
- List, filter, and retrieve detailed car listings using the **Carapis Encar API** parser features.
- Fetch manufacturer, model, and vehicle details programmatically (using slugs for catalog items)
- Supports advanced search queries for the **Carapis Encar API**
- Free tier available for testing the **Carapis Encar API** (up to 1,000 vehicles)

## Installation

Install the `encar` library parser using pip. Dependencies are handled automatically.

```bash
pip install encar
```

## Configuration

1.  **API Key (Optional)**: For full access to the **Carapis Encar API**, an API key is recommended. Get yours at [Carapis.com Pricing](https://carapis.com/pricing).

    *If an API key is **not** provided, the client parser will operate in **Free Tier mode**, limited to accessing the latest 1,000 vehicles.*
    *Retrieve your API key from a secure location, such as environment variables, if you use one.*

## How to use Encar API (Python Client Parser)

Initialize the client parser and make **Carapis Encar API** calls.

```python
import os
from encar import CarapisClient, CarapisClientError

# --- Option 1: Initialize with API Key (Recommended for full access) ---
API_KEY = os.getenv("CARAPIS_API_KEY") # Or get from a secure source
client_with_key = CarapisClient(api_key=API_KEY)

# --- Option 2: Initialize without API Key (Free Tier - Limited Access) ---
client_free_tier = CarapisClient() # No api_key provided


# --- Proceed with Encar API calls using either client parser ---

# Example API call using the free tier client:
# vehicles = client_free_tier.list_vehicles(limit=3)
# Process vehicles['results'] (limited to latest 1000)

```

---

## Encar API Python Usage Examples (Parser)

Below are examples for querying the **Carapis Encar API** using this client parser.

### List Vehicles via Encar API Parser

Retrieve a list of vehicles with filtering. Uses slugs for manufacturer/model group/model.

```python
# Assuming 'client' is an initialized CarapisClient instance
vehicles_response = client.list_vehicles(
    limit=5,
    min_year=2021,
    fuel_type='gasoline',
    manufacturer_slug='hyundai',
    model_group_slug='sonata',
    max_mileage=50000,
    ordering='-created_at'
)
# Process vehicles_response['results'] (list of vehicles)
# vehicles_response also contains 'count', 'page', 'pages', 'limit'
```

### Get Vehicle Details via Encar API Parser

Retrieve details for a specific vehicle by its `vehicle_id`.

```python
# Assuming 'client' is an initialized CarapisClient instance
vehicle_id_to_get = 38481829 # Replace with a valid ID
vehicle_details = client.get_vehicle(vehicle_id=vehicle_id_to_get)
# Process vehicle_details (dictionary with vehicle data)
```

### List Manufacturers via Encar API Parser

Retrieve a list of vehicle manufacturers.

```python
# Assuming 'client' is an initialized CarapisClient instance
manufacturers_response = client.list_manufacturers(country='KR', limit=10)
# Process manufacturers_response['results'] (list of manufacturers)
# manufacturers_response also contains 'count', 'page', 'pages', 'limit'
```

### Get Manufacturer Details via Encar API Parser

Retrieve details for a specific manufacturer by its `slug`.

```python
# Assuming 'client' is an initialized CarapisClient instance
manufacturer_slug = 'hyundai' # Example slug
manufacturer_info = client.get_manufacturer(slug=manufacturer_slug)
# Process manufacturer_info (dictionary with manufacturer data)
```

### Get Manufacturer Stats via Encar API Parser

Retrieve overall statistics about manufacturers.

```python
# Assuming 'client' is an initialized CarapisClient instance
mfr_stats = client.get_manufacturer_stats()
# Process mfr_stats (dictionary with statistics)
```

### List Model Groups via Encar API Parser

Retrieve a list of model groups, filtered by manufacturer's `slug`.

```python
# Assuming 'client' is an initialized CarapisClient instance
manufacturer_slug_for_groups = 'hyundai' # Example slug
model_groups_response = client.list_model_groups(manufacturer__slug=manufacturer_slug_for_groups, search='Sonata', limit=5)
# Process model_groups_response['results'] (list of model groups)
# model_groups_response also contains 'count', 'page', 'pages', 'limit'
```

### Get Model Group Details via Encar API Parser

Retrieve details for a specific model group by its `slug`.

```python
# Assuming 'client' is an initialized CarapisClient instance
model_group_slug = 'sonata' # Example slug
model_group_info = client.get_model_group(slug=model_group_slug)
# Process model_group_info (dictionary with model group data)
```

### List Models via Encar API Parser

Retrieve a list of specific vehicle models, filtered by model group's `slug`.

```python
# Assuming 'client' is an initialized CarapisClient instance
model_group_slug_for_models = 'sonata' # Example slug
models_response = client.list_models(model_group__slug=model_group_slug_for_models, search='DN8', limit=5)
# Process models_response['results'] (list of models)
# models_response also contains 'count', 'page', 'pages', 'limit'
```

### Get Model Details via Encar API Parser

Retrieve details for a specific vehicle model by its `slug`.

```python
# Assuming 'client' is an initialized CarapisClient instance
model_slug = 'sonata-dn8' # Example slug
model_info = client.get_model(slug=model_slug)
# Process model_info (dictionary with model data)
```
