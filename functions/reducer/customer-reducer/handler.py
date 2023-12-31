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
    data = []
    amount = 0
    start_time = time.time()


    input_data_list = client.list_objects(my_name, prefix="key/"+my_key, recursive=True)
    for obj in input_data_list:
        response = client.get_object(my_name, obj.object_name)
        line = json.loads(response.data.decode())
        amount = amount + int(line["value"])    
                
    client.put_object(outputbucket, my_key, io.BytesIO(json.dumps({my_key : amount}).encode('utf-8')), length=-1, part_size=10*1024*1024 , content_type= "application/json")

    end_time = time.time()
    return json.dumps({
        "key" : my_key,
        "time" : end_time - start_time
    })


def emitValues(outputDict, client):
    return True
