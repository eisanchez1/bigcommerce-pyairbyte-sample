import requests
import csv
from airbyte_source_mybigcommerce import SourceBigCommerce
from airbyte_cdk.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteStream, SyncMode
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive.file']

# --- Helper: get remaining quota ---
def get_remaining_quota(config):
    url = f"https://api.bigcommerce.com/stores/{config['store_hash']}/v3/customers"
    headers = {
        "X-Auth-Token": config["access_token"],
        "X-Auth-Client": config["client_id"],
        "Accept": "application/json"
    }
    resp = requests.get(url, headers=headers, params={"limit":1})
    if "X-Rate-Limit-Requests-Left" in resp.headers:
        return int(resp.headers["X-Rate-Limit-Requests-Left"])
    return None

# --- Helper: build single-stream catalog ---
def single_stream_catalog(full_catalog, stream_name):
    for s in full_catalog.streams:
        if s.name == stream_name:
            return ConfiguredAirbyteCatalog(
                streams=[
                    ConfiguredAirbyteStream(
                        stream=s,
                        sync_mode=SyncMode.full_refresh,
                        destination_sync_mode="overwrite"
                    )
                ]
            )
    raise ValueError(f"Stream {stream_name} not found in catalog")

# --- Stream Export Functions ---
# Customer
def export_customers(source, config, full_catalog, limit=5):
    remaining = get_remaining_quota(config)
    if remaining is not None and limit > remaining:
        print(f"⚠️ Adjusting customer limit to {remaining} due to rate limit.")
        limit = remaining

    catalog = single_stream_catalog(full_catalog, "customers")
    messages = list(source.read(config, catalog))

    # Check metadata
    meta_msg = next((m for m in messages if "__meta__" in m.record["data"]), None)
    if meta_msg and meta_msg.record["data"]["__meta__"].get("pagination", {}).get("total", 0) == 0:
        print("No customers found — skipping CSV export.")
        return

    filename = "customers.csv"
    print(f"Creating {filename}...")
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id","email","last_name","first_name","address"])
        writer.writeheader()

        count = 0
        records = [m for m in messages if "__meta__" not in m.record["data"]]

        for message in records:
            data = message.record["data"]
            if "__meta__" in data:
                continue
            writer.writerow({
                "id": data.get("id"),
                "email": data.get("email"),
                "last_name": data.get("last_name"),
                "first_name": data.get("first_name"),
                "address": data.get("address")
            })
            count += 1

            if count >= limit:
                break

    print(f"✅ Exported {count} customers to {filename}")

def export_products(source, config, full_catalog, limit=5):
    remaining = get_remaining_quota(config)
    if remaining is not None and limit > remaining:
        print(f"⚠️ Adjusting product limit to {remaining} due to rate limit.")
        limit = remaining

    catalog = single_stream_catalog(full_catalog, "products")
    messages = list(source.read(config, catalog))

    meta_msg = next((m for m in messages if "__meta__" in m.record["data"]), None)
    if meta_msg and meta_msg.record["data"]["__meta__"].get("pagination", {}).get("total", 0) == 0:
        print("No products found — skipping CSV export.")
        return

    filename = "products.csv"
    print(f"Creating {filename}...")
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id","name","type","categories","price"])
        writer.writeheader()

        count = 0
        records = [m for m in messages if "__meta__" not in m.record["data"]]

        for message in records:
            data = message.record["data"]
            if "__meta__" in data:
                continue
            writer.writerow({
                "id": data.get("id"),
                "name": data.get("name"),
                "type": data.get("type"),
                "categories": data.get("categories"),
                "price": data.get("price")
            })
            count += 1

            if count >= limit:
                break

    print(f"✅ Exported {count} products to {filename}")

def export_categories(source, config, full_catalog, limit=5):
    remaining = get_remaining_quota(config)
    if remaining is not None and limit > remaining:
        print(f"⚠️ Adjusting categories limit to {remaining} due to rate limit.")
        limit = remaining

    catalog = single_stream_catalog(full_catalog, "categories")
    messages = list(source.read(config, catalog))

    meta_msg = next((m for m in messages if "__meta__" in m.record["data"]), None)
    if meta_msg and meta_msg.record["data"]["__meta__"].get("pagination", {}).get("total", 0) == 0:
        print("No categories found — skipping CSV export.")
        return

    filename = "categories.csv"
    print(f"Creating {filename}...")
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id","name","parent_id","tree_id","description","is_visible"])
        writer.writeheader()

        count = 0
        records = [m for m in messages if "__meta__" not in m.record["data"]]

        for message in records:
            data = message.record["data"]
            if "__meta__" in data:
                continue
            writer.writerow({
                "id": data.get("id"),
                "name": data.get("name"),
                "parent_id": data.get("parent_id"),
                "tree_id": data.get("tree_id"),
                "description": data.get("description"),
                "is_visible": data.get("is_visible")
            })
            count += 1
            if count >= limit:
                break

    print(f"✅ Exported {count} categories to {filename}")

def export_orders(source, config, full_catalog, limit=5):
    remaining = get_remaining_quota(config)
    if remaining is not None and limit > remaining:
        print(f"⚠️ Adjusting order limit to {remaining} due to rate limit.")
        limit = remaining

    catalog = single_stream_catalog(full_catalog, "orders")
    messages = list(source.read(config, catalog))

    # For v2 orders, no meta message — just check if we got any records
    records = [m for m in messages if "__meta__" not in m.record["data"]]
    if not records:
        print("No orders found — skipping CSV export.")
        return

    filename = "orders.csv"
    print(f"Creating {filename}...")
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id","status","date_created","total_inc_tax"])
        writer.writeheader()

        count = 0
        for message in records:
            data = message.record["data"]
            writer.writerow({
                "id": data.get("id"),
                "status": data.get("status"),
                "date_created": data.get("date_created"),
                "total_inc_tax": data.get("total_inc_tax")
            })
            count += 1
            if count >= limit:
                break

    print(f"✅ Exported {count} orders to {filename}")


# Google drive services functions
def get_drive_service():
    creds = None
    # Load saved token.json if it exists
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If no valid creds, refresh or run OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the new token for next runs
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(filename):
    if not os.path.exists(filename):
        print(f"⚠️ File {filename} does not exist — skipping upload.")
        return None

    service = get_drive_service()

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")  # read from env
    file_metadata = {'name': filename}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(filename, mimetype='text/csv')
    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print(f"✅ Uploaded {filename} to Google Drive (file ID: {uploaded_file.get('id')})")
    return uploaded_file.get('id')

# --- Main Runner ---
def main():
    source = SourceBigCommerce()
    config = {
        "client_id": os.getenv("BIGCOMMERCE_CLIENT_ID"),
        "access_token": os.getenv("BIGCOMMERCE_ACCESS_TOKEN"),
        "store_hash": os.getenv("BIGCOMMERCE_STORE_HASH")
    }

    print("Check:", source.check(config))
    catalog = source.discover(config)
    print("Streams:", [s.name for s in catalog.streams])

    # create CSVs
    export_customers(source, config, catalog, limit=3000)
    export_products(source, config, catalog, limit=500)
    export_orders(source, config, catalog, limit=3000)
    export_categories(source,config,catalog, limit=34)

    #upload CSVs to drive
    print("Uploading CSVs to Google Drive...")
    upload_to_drive("customers.csv")
    upload_to_drive("products.csv")
    upload_to_drive("orders.csv")


if __name__ == "__main__":
    main()
