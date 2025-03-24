import requests
import json
import snowflake.connector

# Fetch data from Sportradar API
API_URL = "https://api.sportradar.com/soccer-extended/trial/v4/en/competitors/sr%3Acompetitor%3A44/summaries.json?api_key=5qKS39A3oeSHA0HzXhlRXs89ciYKKHkiZEwWKEos"

# Fetch JSON data from the API
response = requests.get(API_URL)
if response.status_code == 200:
    json_data = response.json()  # Convert response to Python dictionary
else:
    raise Exception(f"API request failed with status code {response.status_code}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="zeeshanishfaq",
    password="Nauman@1879A#24",
    account="oelqagw-fm13400",
    warehouse="COMPUTE_WH",
    database="CLUB",
    schema="LIVERPOOL")
    
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