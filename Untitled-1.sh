virtualenv virtualenv --python="/usr/bin/python3.7"
source virtualenv/bin/activate
pip install minio
zip -r reducertest.zip virtualenv __main__.py
wsk action create reducertest --kind python:3 --main handle reducertest.zip -i
