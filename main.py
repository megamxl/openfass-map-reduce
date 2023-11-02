from minio import Minio
import io
import json
import http.client
import time
import threading


client = Minio(
    endpoint="192.168.178.250:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

def deploy_a_function(ip, port, functioname, image):
    conn = http.client.HTTPConnection(ip, port)
    payload = json.dumps({
        "service": functioname,
        "image": image,
        "envProcess": "",
        "labels": {},
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

my_name = "dry-run"

#client.make_bucket(bucket_name=my_name)

usedkeys = []

print("start with data spliting")
with open("exampleData/data.csv") as fl:
    key = 0
    lines = []
    for index, line in enumerate(fl):
        if index > 0 and index % 300 == 0:
            # put in object storage
            client.put_object(my_name, str(key),  io.BytesIO(json.dumps(lines).encode('utf-8')), content_type= "application/json", length=-1, part_size=10*1024*1024)
            lines.clear()
            usedkeys.append(key)
            key = key +1
            #TODO ROEMOVE
            if key == 11:
                break 
        lines.append(line)

deploy_a_function("192.168.178.200", 8080, "map2", "megamaxl/customer-mapper:latest")
print("depolyed mapper")

#client.make_bucket(bucket_name="dry-run-inermediate")

threads = []


for key in usedkeys:
    data = json.dumps({
        "bucketName": my_name,
        "key": str(key),
        "outputBucket": "dry-run-inermediate"
    })
    thread = threading.Thread(target=invoke_a_function, args=("192.168.178.200", 8080, "map2", data,))
    threads.append(thread)

print("starting all mapper calls")
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All HTTP calls completed.")   
 
threads.clear()

#for key in usedkeys:
    #delete_a_function("192.168.178.200", 8080, "automaticmapper-"+ str(key), "megamaxl/customer-mapper:latest")

paths = client.list_objects("dry-run-inermediate", prefix="key", recursive=True)

intermediatekeys = []

deploy_a_function("192.168.178.200", 8080, "reduce", "megamaxl/customer-reducer:latest")

print("getting all intermediate keys")
for object in paths:
    intermediatekeys.append(object.object_name.split("/")[1])

for idx, key_1 in enumerate(intermediatekeys):
    data = json.dumps({
        "bucketName": "dry-run-inermediate",
        "key": str(key_1),
        "outputBucket": "dry-run-done"
    })
    if idx > 0 and idx % 10 == 0:
        for thread in threads:
            thread.start()

    # Wait for all threads to finish
        for thread in threads:
            thread.join()
        
        threads.clear()
    
    thread = threading.Thread(target=invoke_a_function, args=("192.168.178.200", 8080, "reduce", data,))
    threads.append(thread)

print("starting all reducer calls")
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All HTTP calls completed. reduce")