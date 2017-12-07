import requests
import json
import time
print('add .7')
r = requests.put('http://localhost:8083/kv-store/update_view?type=add',data={'ip_port':'10.0.0.7:8080'})
print(r.json())
print('add .8')
r = requests.put('http://localhost:8085/kv-store/update_view?type=add',data={'ip_port':'10.0.0.8:8080'})
print(r.json())
print('add .9')
r = requests.put('http://localhost:8087/kv-store/update_view?type=add',data={'ip_port':'10.0.0.9:8080'})
print(r.json())
# print('get node state on .5')
# r = requests.get('http://localhost:8085/getNodeState')
# print('my_part_id',r.json()['my_part_id'])
# print('part_dic',r.json()['part_dic'])
# print('world_proxy',r.json()['world_proxy'])
# print(json.dumps(r.json(),indent=2))
# print('remove .3')
# r = requests.put('http://localhost:8089/kv-store/update_view?type=remove',data={'ip_port':'10.0.0.3:8080'})
# print(r.json())
# print()
# print('get node state on .4')
# r = requests.get('http://localhost:8084/getNodeState')
# print('my_part_id',r.json()['my_part_id'])
# print('part_dic',r.json()['part_dic'])
# print('world_proxy',r.json()['world_proxy'])
# print('get node state on .5')
# r = requests.get('http://localhost:8085/getNodeState')
# print('my_part_id',r.json()['my_part_id'])
# print('part_dic',r.json()['part_dic'])
# print('world_proxy',r.json()['world_proxy'])
#
# r = requests.put('http://localhost:8089/kv-store/update_view?type=remove',data={'ip_port':'10.0.0.5:8080'})
# print(r.json())
#
# print('get node state on .4')
# r = requests.get('http://localhost:8084/getNodeState')
# print('my_part_id',r.json()['my_part_id'])
# print('part_dic',r.json()['part_dic'])
# print('world_proxy',r.json()['world_proxy'])
#
# print('get node state on .7')
# r = requests.get('http://localhost:8087/getNodeState')
# print(r.json()['my_part_id'])
# print(r.json()['part_dic'])
# print(r.json()['world_proxy'])
#
#
# print('remove on .8')
# r = requests.put('http://localhost:8089/kv-store/update_view?type=remove',data={'ip_port':'10.0.0.8:8080'})
# print(r.json())
#
# print('get node state on .7')
# r = requests.get('http://localhost:8087/getNodeState')
# print(r.json()['my_part_id'])
# print(r.json()['part_dic'])
# print(r.json()['world_proxy'])
import random
import string
def generate_random_keys(n):
    alphabet = string.ascii_lowercase
    keys = []
    for i in range(n):
        key = ''
        for _ in range(10):
            key += alphabet[random.randint(0, len(alphabet) - 1)]
        keys.append(key)
    return keys

keys = generate_random_keys(60)

counter = 3
responses = []
for key in keys:
    if(counter>7):
        counter = 3
    r = requests.put("http://localhost:" +'808'+str(counter)+ "/kv-store/" + key, data = {'val': key, 'causal_payload': ''})
    t=(r.status_code,counter)
    responses.append(t)
    time.sleep(1)
    counter += 1
print(responses)
