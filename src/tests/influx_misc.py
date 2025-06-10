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
buckets = buckets_api.find_buckets()
for bucket in buckets:
    print(f"Bucket: {bucket.name}, Org: {bucket.org_id}")

# List measurements in a bucket
flux_query = '''
import "influxdata/influxdb/schema"
schema.measurements(bucket:"your-bucket-name")
'''
query_api = client.query_api()
result = query_api.query(flux_query)
print(result)
# measurements_df = result.to_pandas()
# print("Available measurements:", measurements_df['_value'].tolist())