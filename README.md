# BigCommerce ETL Pipeline using PyAirbyte (Airbyte + Python)

This repository contains a working example of an ETL pipeline that extracts data from BigCommerce, transforms it into CSV files, and uploads them to Google Drive.

## Features
- Extracts **orders (v2)**, **customers (v3)**, **products (v3)**, and **categories (v3)**.
- Handles BigCommerce API differences (list vs dict responses).
- Exports CSV files with clean headers.
- Uploads CSVs to Google Drive using OAuth credentials.
- Includes defensive error handling and rate-limit tracking.

## Requirements
- Python 3.9+
- Virtual environment recommended
- BigCommerce API token and store hash
- Google Drive API credentials (OAuth client ID/secret)

## Setup
1. Clone the repo:
   ```bash
   git clone https://github.com/YOUR_USERNAME/bigcommerce-etl.git
   cd bigcommerce-etl
2. Create a virtual environment:

bash
python -m venv venv
source venv/bin/activate
Install dependencies:

bash
pip install -r requirements.txt
Configure environment variables in .env (do not commit this file):

Create environment variables:

BIGCOMMERCE_STORE_HASH=your_store_hash
BIGCOMMERCE_ACCESS_TOKEN=your_access_token
GOOGLE_CLIENT_ID=your_client_id
GOOGLE_CLIENT_SECRET=your_client_secret

Usage
Run the pipeline:

bash
python3 bigcommerce_to_googledrive.py

CSV files will be generated locally and uploaded to Google Drive.
Sample output:

Code
✅ Uploaded customers.csv to Google Drive (file ID: 1GwzR-KtFqLaDN_0fPJOpNnBvkpB5znmR)
✅ Uploaded products.csv to Google Drive (file ID: 1QuwOmko0ylmHiSNPVsqGaRjyNiAz0nWv)
✅ Uploaded orders.csv to Google Drive (file ID: 1EU40ut7UORXM6eAI2PB0tQd7-4TkXaEx)
Notes
BigCommerce API enforces rate limits. Use batching + pacing (time.sleep) to avoid hitting quotas.

Orders (v2) return a list of objects; other endpoints (v3) return dicts with meta + data.

Never commit your .env file or credentials.

License
MIT