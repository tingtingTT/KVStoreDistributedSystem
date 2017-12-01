import requests
import json
import os
from time import sleep

# case A: get Views
#r = requests.get('http://localhost:8081/View')
#print('status code: ',r)
#j = r.json()
#print(j)
#print('views',j['views'])

# case B: Now, consider another system. If N=6, K=2, we should have three partitions.
# If one node is removed, that partition can no longer operate,
# since the required replication factor (K) has not been maintained.
# Therefore, we must now drop to 2 partitions with 1 node behaving as proxy.
os.system("docker run -p 8081:8080 --ip=10.0.0.21 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.21:8080 -d app")
os.system("docker run -p 8082:8080 --ip=10.0.0.22 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.22:8080 -d app")
os.system("docker run -p 8083:8080 --ip=10.0.0.23 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.23:8080 -d app")
os.system("docker run -p 8084:8080 --ip=10.0.0.24 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.24:8080 -d app")
os.system("docker run -p 8085:8080 --ip=10.0.0.25 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.25:8080 -d app")
os.system("docker run -p 8086:8080 --ip=10.0.0.26 --net=mynet -e K=2 -e VIEW=10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080 -e IPPORT=10.0.0.26:8080 -d app")


# case B.1: n == 6, parition == 3
r = requests.get('http://localhost:8081/view')
print('port 8081')
print('status code: ',r)
j = r.json()
print(j)
sleep(0.5)
r = requests.get('http://localhost:8082/view')
print('port 8082')
print('status code: ',r)
j = r.json()
print(j)
sleep(0.5)
r = requests.get('http://localhost:8083/view')
print('port 8083')
print('status code: ',r)
j = r.json()
print(j)
sleep(0.5)
r = requests.get('http://localhost:8084/view')
print('port 8084')
print('status code: ',r)
j = r.json()
print(j)
sleep(0.5)
r = requests.get('http://localhost:8085/view')
print('port 8085')
print('status code: ',r)
j = r.json()
print(j)
sleep(0.5)
r = requests.get('http://localhost:8086/view')
print('port 8086')
print('status code: ',r)
j = r.json()
print(j)

#Case B.1: If one node is removed, that partition can no longer operate,

# Case B.2: A GET request on "/kv-store/get_partition_id" returns the partition id where the node belongs to.
# For example, the following curl request curl -X GET http://localhost:8083/kv-store/get_partition_id returns
# the id of the node that we can access via localhost:8083. A successful response looks like:
r = requests.get('http://localhost:8081/kv-store/get_partition_id')
j = r.json()
print('0',j)
sleep(0.5)
r = requests.get('http://localhost:8082/kv-store/get_partition_id')
j = r.json()
print('0',j)
sleep(0.5)
r = requests.get('http://localhost:8083/kv-store/get_partition_id')
j = r.json()
print('1',j)
sleep(0.5)
r = requests.get('http://localhost:8084/kv-store/get_partition_id')
j = r.json()
print('1',j)
sleep(0.5)
r = requests.get('http://localhost:8085/kv-store/get_partition_id')
j = r.json()
print('2',j)
sleep(0.5)
r = requests.get('http://localhost:8086/kv-store/get_partition_id')
j = r.json()
print('2',j)
sleep(0.5)





os.system(" docker kill $(docker ps -q)")
