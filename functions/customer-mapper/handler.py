def handle(req):

    from minio import Minio
    import json
    import io
    import random
    import string

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

    try:
        response = client.get_object(my_name,my_key)
        lines = json.loads(response.data.decode())
        for line in lines:
            line_parts = line.split(',')
            curr_custmoer = line_parts[2]
            if curr_custmoer not in customers: 
                customers[curr_custmoer] = 0
            customers[curr_custmoer] += 1
        for key in customers:
            assambledKey = "/key/" +  key  + "/" + ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
            client.put_object(outputbucket, assambledKey, io.BytesIO(json.dumps((customers[key])).encode('utf-8')), length=-1, part_size=10*1024*1024 , content_type= "application/json")
    finally:
        response.close()
        response.release_conn()

    return json.dumps(customers)


def emitValues(outputDict, client):
   return True
