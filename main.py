from minio import Minio
import io
import json
import http.client
import time
import threading


with open('config.json') as config:
    cofing_contnet = config.read()

parsed_config = json.loads(cofing_contnet)

client = Minio(
    endpoint=parsed_config['minio-enpoint'],
    access_key=parsed_config['minio-access_key'],
    secret_key=parsed_config['minio-secret_key'],
    secure=False
)

def deploy_a_function(ip, port, functioname, image):
    conn = http.client.HTTPConnection(ip, port)
    payload = json.dumps({
        "service": functioname,
        "image": image,
        "envProcess": "",
        "labels": {"com.openfaas.scale.min":"5"},
        "annotations": {}
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic YWRtaW46WlcwVDNOODlrbG11'
    }
    conn.request("POST", "/system/functions", payload, headers)
    res = conn.getresponse()
    print(res.status)


def delete_a_function(ip, port, functioname, image):
    conn = http.client.HTTPConnection(ip, port)
    payload = json.dumps({
        "functionName": functioname,
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic YWRtaW46WlcwVDNOODlrbG11'
    }
    conn.request("DELETE", "/system/functions", payload, headers)
    res = conn.getresponse()
    print(res.status)

def invoke_a_function(ip, port, functionName, body):
    conn = http.client.HTTPConnection(ip, port)
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("GET", "/function/"+ functionName, body, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
## find out which customer purchaed the most things 

###
# 
#  First Step take the file and create chunks and put them into Minio
#
###

input_bucket = parsed_config['bucket-prefix'] + "-input"

client.make_bucket(bucket_name=input_bucket)

usedkeys = []

batch_size = parsed_config['input-bath-size']

print("start with data spliting")
with open("exampleData/data.csv") as fl:
    key = 0
    lines = []
    for index, line in enumerate(fl):
        if index > 0 and index % batch_size == 0:
            # put in object storage
            client.put_object(input_bucket, str(key),  io.BytesIO(json.dumps(lines).encode('utf-8')), content_type= "application/json", length=-1, part_size=10*1024*1024)
            lines.clear()
            usedkeys.append(key)
            key = key +1
            #TODO ROEMOVE
            if key == 11:
                break 
        lines.append(line)

mapper_function_name = parsed_config['bucket-prefix'] + "-mapper"


deploy_a_function(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], mapper_function_name, parsed_config['mapFunction'])
print("depolyed mapper")


intermidated_buket = parsed_config['bucket-prefix'] + "-intermidated"

client.make_bucket(bucket_name=intermidated_buket)

threads = []

for key in usedkeys:
    data = json.dumps({
        "bucketName": input_bucket,
        "key": str(key),
        "outputBucket": intermidated_buket
    })
    thread = threading.Thread(target=invoke_a_function, args=(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], mapper_function_name, data,))
    threads.append(thread)

print("starting all mapper calls")
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All HTTP calls completed.")   
 
threads.clear()

paths = client.list_objects(intermidated_buket, prefix="key", recursive=True)

intermediatekeys = []

reducer_function_name = parsed_config['bucket-prefix'] + "-reducer"

deploy_a_function(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], reducer_function_name, parsed_config['reduceFunction'])

output_bucket = parsed_config['bucket-prefix'] + "-output"

client.make_bucket(bucket_name=output_bucket)


print("getting all intermediate keys")
for object in paths:
    intermediatekeys.append(object.object_name.split("/")[1])

for idx, key_1 in enumerate(intermediatekeys):
    data = json.dumps({
        "bucketName": intermidated_buket,
        "key": str(key_1),
        "outputBucket": output_bucket
    })
    if idx > 0 and idx % 50 == 0:
        for thread in threads:
            thread.start()

    # Wait for all threads to finish
        for thread in threads:
            thread.join()
        
        threads.clear()
    
    thread = threading.Thread(target=invoke_a_function, args=(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], reducer_function_name, data,))
    threads.append(thread)

print("All HTTP calls completed. reduce")