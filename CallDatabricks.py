from dotenv import load_dotenv
import os
import requests
import pandas as pd
from databricks import sql

load_dotenv()

HOST_D = os.getenv('HOST')
ACCOUNT_ID_D = os.getenv('ACCOUNT_ID')
CLIENT_ID_D = os.getenv('CLIENT_ID')
CLIENT_SECRET_D = os.getenv('CLIENT_SECRET')
WORKSPACE_HOST_D = os.getenv('WORKSPACE_HOST')
SQL_HTTP_PATH_D = os.getenv('SQL_HTTP_PATH')
START_DATE = "2024-10-01"
END_DATE = "2025-10-31"
OUTPUT_PATH = "output_files/"
OUTPUT_FILE = "{}databricks_usage_{}_{}".format(OUTPUT_PATH,START_DATE, END_DATE)


# Databricks AWS OAuth 2.0 token endpoint
token_url = "https://{}/oidc/accounts/{}/v1/token".format(HOST_D, ACCOUNT_ID_D)

# Step 1: Request an access token using client credentials
payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID_D,
    "client_secret": CLIENT_SECRET_D,
    "scope": "all-apis"
}

response = requests.post(token_url, data=payload)
response.raise_for_status()

access_token = response.json()["access_token"]
print("Access Token retrieved successfully")

# Step 2: Connect to Databricks SQL Warehouse using the token
server_hostname = WORKSPACE_HOST_D   # e.g. 'dbc-1234.cloud.databricks.com'
http_path = SQL_HTTP_PATH_D     # from SQL warehouse settings

with sql.connect(
    server_hostname=server_hostname,
    http_path=http_path,
    access_token=access_token
) as connection:
    query = f"""
        SELECT
            date_format(usage_date, 'yyyy-MM'),
            workspace_id,
            sbu.sku_name,
            custom_tags,
            SUM(usage_quantity),
            sbu.usage_unit,
            currency_code,
            SUM(usage_quantity * sbl.pricing.effective_list.default) AS cost
        FROM system.billing.usage sbu
        JOIN system.billing.list_prices sbl ON sbu.sku_name = sbl.sku_name
        WHERE usage_date BETWEEN '{START_DATE}' AND '{END_DATE}'
        AND sbu.usage_end_time >= sbl.price_start_time
        AND (sbl.price_end_time IS NULL OR sbu.usage_end_time < sbl.price_end_time)
        GROUP BY usage_date, workspace_id, sbu.sku_name, sbu.usage_unit, custom_tags, currency_code
        ORDER BY usage_date, workspace_id, sbu.sku_name
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

# Step 3: Save results to CSV
df = pd.DataFrame(rows, columns=["year_mon","workspace_id","sku_name","custom_tags","usage_quantity","usage_unit","currency_code","cost"])
df.to_csv(OUTPUT_FILE + ".csv", index=False)
