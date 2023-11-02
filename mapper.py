from minio import Minio
import io
import json
import http.client


client = Minio(
    endpoint="192.168.178.250:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

intermediatekeys = []

intermidated_keys = client.list_objects("dry-run-inermediate", prefix="key", recursive=True)

#for object in intermidated_keys:
    #print(object.object_name.split("/")[1])

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


for i in range(30):
    delete_a_function("192.168.178.200", 8080, "reduce-"+str(i), "")