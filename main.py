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
    time.sleep(20)


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
    #TODO Error handling if function dose not get run append it to error lsit and do it again
    if res.code != 200 and res.code != 202:
        failed_req.append([ip,port,functionName,body])
    print(data.decode("utf-8"))


def reDoingRequests(req):
    localtread = []
    for idx, key_1 in enumerate(req):
        if idx > 0 and idx % 20 == 0:
            for thread in localtread:
                thread.start()

            # Wait for all threads to finish
            for thread in localtread:
                thread.join()
            
            localtread.clear()
        
        thread = threading.Thread(target=invoke_a_function, args=(key_1[0], key_1[1], key_1[2], key_1[3],))
        localtread.append(thread)

## find out which customer purchaed the most things 

###
# 
#  First Step take the file and create chunks and put them into Minio
#
###

input_bucket = parsed_config['bucket-prefix'] + "-input"

client.make_bucket(bucket_name=input_bucket)

usedkeys = []
failed_req = []

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
            #if key == 100:
            #    break 
        lines.append(line)

###
# 
#  Second Step take all chunks and put them into http requests / doing the mapper calls
#
###

star_time = time.time()

mapper_function_name = parsed_config['bucket-prefix'] + "-mapper"

deploy_a_function(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], mapper_function_name, parsed_config['mapFunction'])
print("depolyed mapper")

intermidated_buket = parsed_config['bucket-prefix'] + "-intermidated"

client.make_bucket(bucket_name=intermidated_buket)

threads = []
print(f"Doing {len(usedkeys)} mapper calls ")
for idx, key_1 in enumerate(usedkeys):
    data = json.dumps({
        "bucketName": input_bucket,
        "key": str(key_1),
        "outputBucket": intermidated_buket
    })
    if idx > 0 and idx % 50 == 0:
        for thread in threads:
            thread.start()

    # Wait for all threads to finish
        for thread in threads:
            thread.join()
        
        threads.clear()
        print("")
    
    thread = threading.Thread(target=invoke_a_function, args=(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], mapper_function_name, data,))
    threads.append(thread)

for thread in threads:
            thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

threads.clear()

print(f"there were {len(failed_req)} failed request doing them again")

if len(failed_req) != 0:
    reDoingRequests(failed_req)
    
failed_req.clear()

###
# 
#  Third Step take all intermidiate keys an do the reducer calls
#
###

paths = client.list_objects(intermidated_buket, prefix="key", recursive=True)

intermediatekeys = []

reducer_function_name = parsed_config['bucket-prefix'] + "-reducer"

deploy_a_function(parsed_config['gateway-Ip'], parsed_config['gateway-Port'], reducer_function_name, parsed_config['reduceFunction'])

print("Deployed the reducer function")

output_bucket = parsed_config['bucket-prefix'] + "-output"

client.make_bucket(bucket_name=output_bucket)

print("getting all intermediate keys")
for object in paths:
    intermediatekeys.append(object.object_name.split("/")[1])

print(f"Doing {len(intermediatekeys)} reducer calls ")
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

print(f"there were {len(failed_req)} failed request doing them again")

if len(failed_req) != 0:
    reDoingRequests(failed_req)


end_time = time.time()

sec_between = end_time -star_time

print("All Done Here are your statistics nows")
print("")
print("")
print(f"The time elapesd between the first fuction deploy until now are {sec_between}" )
