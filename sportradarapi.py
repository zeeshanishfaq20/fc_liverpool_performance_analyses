import requests
import json
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch data from Sportradar API
API_URL = os.getenv("API_URL")

# Fetch JSON data from the API
response = requests.get(API_URL)
if response.status_code == 200:
    json_data = response.json()  # Convert response to Python dictionary
else:
    raise Exception(f"API request failed with status code {response.status_code}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("user"),
    password=os.getenv("password"),
    account=os.getenv("account"),
    warehouse=os.getenv("warehouse"),
    database=os.getenv("database"),
    schema=os.getenv("schema")
)
    
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
        CREATE TABLE IF NOT EXISTS sportradar_data (
               data VARIANT
        );
""")

# Insert JSON data
insert_query = "INSERT INTO sportradar_data (data) SELECT PARSE_JSON(%s)"
cursor.execute(insert_query, (json.dumps(json_data),))

conn.commit()
cursor.close()
conn.close()
print("Data inserted successfully!")