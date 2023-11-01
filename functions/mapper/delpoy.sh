faas-cli build -f ./customer-mapper.yml 
faas-cli push -f ./customer-mapper.yml 
faas-cli deploy -f ./customer-mapper.yml --gateway http://192.168.178.200:8080/