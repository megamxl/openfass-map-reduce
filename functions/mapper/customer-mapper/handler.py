def handle(req):

    from minio import Minio
    import json
    import io
    import random
    import string
    import time

    context = json.loads(req)

    client = Minio(
        endpoint="192.168.178.250:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    my_name = context["bucketName"]
    my_key = context["key"]
    outputbucket = context["outputBucket"]
    #my_name = "test-bucket"
    #my_key = "2"
    #outputbucket = "test-bucket-output"
    customers = {}
    start_time = time.time()

    try:
        response = client.get_object(my_name,my_key)
        lines = json.loads(response.data.decode())
        for line in lines:
            line_parts = line.split(',')
            if len(line_parts) == 8:   
                curr_custmoer = line_parts[6]
                if curr_custmoer == "":
                    continue
                if curr_custmoer not in customers: 
                    customers[curr_custmoer] = 0
                customers[curr_custmoer] += 1
        for key in customers:
            assambledKey = "/key/" +  key  + "/" + ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
            value = json.dumps({"value" : json.dumps(customers[key])}).encode('utf-8')
            client.put_object(outputbucket, assambledKey, io.BytesIO(value), length=-1, part_size=10*1024*1024 , content_type= "application/json")
    finally:
        response.close()
        response.release_conn()

    end_time = time.time()
    return json.dumps({
        "key" : str(my_key),
        "time" : end_time - start_time,
    })


def emitValues(outputDict, client):
   return True
