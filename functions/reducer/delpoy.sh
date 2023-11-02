faas-cli build -f ./customer-reducer.yml 
faas-cli push -f ./customer-reducer.yml 
faas-cli deploy -f ./customer-reducer.yml --gateway http://192.168.178.200:8080/