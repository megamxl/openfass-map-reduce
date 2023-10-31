from minio import Minio
import io
import json


client = Minio(
    endpoint="192.168.178.250:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

my_name = "test-bucket"
customers = {}

try:
    response = client.get_object(my_name,"2")
    # Read data from response.
    lines = json.loads(response.data.decode())
    for line in lines:
        line_parts = line.split(',')
        curr_custmoer = line_parts[6]
        if curr_custmoer not in customers: 
            customers[curr_custmoer] = 0
        customers[curr_custmoer] += 1
finally:
    print(customers)
    response.close()
    response.release_conn()