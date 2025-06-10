import json
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import SYNCHRONOUS
from utils.envvars import EnvVars


# Create InfluxDB client
client = InfluxDBClient(
    url=EnvVars().influx_url,
    token=EnvVars().influx_token,
    org=EnvVars().influx_org
)


# Create write API with synchronous mode
bucket = EnvVars().influx_bucket

# List buckets
buckets_api = client.buckets_api()
buckets_resp = buckets_api.find_buckets()
print("Buckets:")
for bucket in buckets_resp.buckets:
    print(f"Bucket: {bucket.name}, Org: {bucket.org_id}")

# List measurements in a bucket
flux_query = '''
import "influxdata/influxdb/schema"
schema.measurements(bucket:"crem3")
'''
query_api = client.query_api()
tables = query_api.query(flux_query)

measurements = []

if tables:
    for table in tables:
        df = table.to_pandas()
        if not df.empty and '_value' in df.columns:
            measurements.extend(df['_value'].tolist())

print(f"Measurements found: {measurements}")
