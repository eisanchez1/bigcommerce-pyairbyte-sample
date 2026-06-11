import requests
from airbyte_cdk.sources import Source
from airbyte_cdk.models import SyncMode, AirbyteStream, AirbyteCatalog, AirbyteMessage, Type

class SourceBigCommerce(Source):

    # API Path: https://api.bigcommerce.com/stores/{store hash}/v3/

    def spec(self, *args, **kwargs):
        return {
            "client_id": "string",
            "access_token": "string",
            "store_hash": "string"
        }

    
    def check(self, config):
        url = f"https://api.bigcommerce.com/stores/{config['store_hash']}/v3/catalog/products"
        headers = {
            "X-Auth-Token": config["access_token"],
            "Accept": "application/json"
        }
        resp = requests.get(url, headers=headers)
        print("Status:", resp.status_code)
        
        return resp.status_code == 200

    def discover(self, config):
        return AirbyteCatalog(streams=[
            AirbyteStream(
                name="products",
                json_schema={
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "price": {"type": "string"},
                        "type": {"type": "string"},
                        "categories": {"type": "array"}
                    }
                },
                supported_sync_modes=[SyncMode.full_refresh]
            ),
            AirbyteStream(
                name="customers",
                json_schema={
                    "properties": {
                        "id": {"type": "integer"},
                        "email": {"type": "string"},
                        "first_name": {"type": "string"},
                        "last_name": {"type": "string"},
                        "address": {"type": "object"}
                    }
                },
                supported_sync_modes=[SyncMode.full_refresh]
            ),
            AirbyteStream(
                name="orders",
                json_schema={
                    "properties": {
                        "id": {"type": "integer"},
                        "status": {"type": "string"},
                        "date_created": {"type": "string"},
                        "total_inc_tax": {"type": "string"}
                    }
                },
                supported_sync_modes=[SyncMode.full_refresh]
            ),
            AirbyteStream(
                name="categories",
                json_schema={
                    "properties": {
                        "id": {"type": "integer"},
                        "parent_id": {"type": "integer"},
                        "tree_id": {"type": "integer"},
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "is_visible": {"type": "boolean"}
                    }
                },
                supported_sync_modes=[SyncMode.full_refresh]
            )
        ])

    def read(self, config, catalog, state=None):
        endpoints = {
            "products": "v3/catalog/products",
            "customers": "v3/customers",
            "orders": "v2/orders",
            "categories": "v3/catalog/categories"
        }

        headers = {
            "X-Auth-Token": config["access_token"],
            "Accept": "application/json"
        }

        selected_streams = {s.stream.name for s in catalog.streams}

        for stream, endpoint in endpoints.items():
            if stream not in selected_streams:
                continue

            page = 1
            print(f"Extracting {stream}...")
            while True:
                url = f"https://api.bigcommerce.com/stores/{config['store_hash']}/{endpoint}"

                params = {"limit": 250, "page": page}
                if stream == "orders":
                    params["is_deleted"] = "false"

                resp = requests.get(url, headers=headers, params=params)
                
                # Defensive checks
                if resp.status_code == 204:  # No content
                    break
                if resp.status_code != 200:
                    print(f"Error fetching {stream}: {resp.status_code} {resp.text[:200]}")
                    break

                resp_json = resp.json()
                
                if stream == "orders":
                    # v2 Orders → list of objects
                    meta = {}
                    records = resp_json
                else:
                    # v3 endpoints → dict with meta + data
                    meta = resp_json.get("meta", {})
                    records = resp_json.get("data", [])
                
                # Yield meta per page
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record={"stream": stream, "data": {"__meta__": meta}}
                )

                # Yield records
                for record in records:
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record={"stream": stream, "data": record}
                    )

                # Pagination guard
                pagination = meta.get("pagination", {})
                total_pages = pagination.get("total_pages", 1)
                if page >= total_pages or not records:
                    break
                page += 1

