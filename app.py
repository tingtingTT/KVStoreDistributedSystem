#######################################################
from flask import Flask, abort, request, jsonify, make_response, current_app
from flask_restful import Resource, Api
import json
from sys import getsizeof #for input check
import re, os, socket, time
import requests
import random
import threading

############################################
# initialize variables
######################################
app = Flask(__name__)
api = Api(app)

class BaseClass():
    def __init__(self):
        self.kv_store={} # key:[value, time_stamp]
        self.node_ID_dic={} # ip_port: node_ID
        self.part_dic={} # my_part_id: replica_array
        self.partition_view = [] # change in initView
        self.kv_store_vector_clock=[0]*8 # is the pay load
        self.part_clock = 0
        self.my_part_id = -1
        self.world_proxy = {}
        # get variables form ENV variables
        self.my_IP = os.environ.get('IPPORT', None)
        self.IP = socket.gethostbyname(socket.gethostname())
        self.K_init = os.getenv('K', None)
        self.K = -1
        if self.K_init is not None:
            self.K = int(self.K_init)
        self.VIEW_init = os.environ.get('VIEW', None)
        self.VIEW_list = []
        if self.VIEW_init is not None:
            self.VIEW_list = self.VIEW_init.split(',')

b = BaseClass()

###########################################
# thread running for heartbeat
#####################################
@app.before_first_request
def activate_job():
    def run_job():
        while True:
            with app.app_context():
                time.sleep(1)
                heartbeat()
    thread = threading.Thread(target=run_job)
    thread.start()

############################################
# init partition_view using VIEW
######################################
def update(add_node_ip_port, part_id):
    # update view
    # check if the ip is already in the dictionary or not,if not, add new ID. if so, do nothing
    if b.node_ID_dic.get(add_node_ip_port) is None:
        b.node_ID_dic[add_node_ip_port] = len(b.node_ID_dic)

    # initialize part_dic
    if part_id in b.part_dic:
        if len(b.part_dic[part_id][0]) < b.K:
            b.part_dic[part_id][0].append(add_node_ip_port)
        else:
            b.part_dic[part_id][1].append(add_node_ip_port)
            b.world_proxy[add_node_ip_port] = part_id
            b.part_clock += 1

    else:
        b.part_dic[part_id] = ([add_node_ip_port], [])



###########################################################
# functon to init world view using user input VIEW
#####################################################
def initVIEW():
    if len(b.VIEW_list) > 0:
        N = len(b.VIEW_list)
        numPart = N/b.K
        numProx = N%b.K

        for i in range(0, b.K * numPart):
            part_id = i/b.K
            if b.VIEW_list[i] == b.my_IP:
                b.my_part_id = part_id
            update(b.VIEW_list[i], part_id)

        if numProx > 0:
            proxies = b.VIEW_list[b.K * numPart:]
            b.part_dic[0][1] = proxies
            # TODO: always add to first partition when init view???
            for i in range (0, numProx):
                current_proxy = b.VIEW_list[b.K * numPart + 1 + i]
                b.world_proxy[current_proxy] = 0

        b.partition_view = getProxyArr() + getReplicaArr()

    return


###########################################################
# functon called in the heartbeat for syncing the kvstores
###########################################################
def gossip(IP):
    for key in b.kv_store:
        response = requests.get('http://'+IP+'/getKeyDetails/' + key, data = {'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'val':b.kv_store[key][0], 'timestamp': b.kv_store[key][1], 'nodeID':b.node_ID_dic[b.my_IP]})
        res = response.json()
        # return response
        b.kv_store[key] = (res['value'], res['timestamp'])

#################################################################
# functon called intermitantly to sync up the partition_view and the kv_stores
###########################################################
def heartbeat():
    # gossip with a random IP in the replicas array
    for node in getReplicaArr():
        if(node != b.my_IP):
            gossip(node)
    # worldSync()
    #partitionChange()
    time.sleep(.050) #seconds

####################################################################################
# function to check which node is up and down with ping, then promode and demote nodes
######################################################################################
def worldSync():
    while(True):
        tryNode = b.partition_view[random.randint(0, len(b.partition_view)-1)]
        if tryNode != b.my_IP:
            try:
                response = requests.get('http://'+tryNode+"/availability", timeout=5)
                res = response.json()
                break
            except requests.exceptions.ConnectionError:
                pass
            except requests.exceptions.Timeout:
                pass

    for node in b.partition_view:
        if res[node] != 0: #if the ping result is saying the node is down
            if node in b.part_dic[b.my_part_id][0]:
                (b.part_dic[b.my_part_id][0]).remove(node)
                if len(b.part_dic[b.my_part_id][0])<b.K and len(b.part_dic[b.my_part_id][1])>0:
                    promoteNode(b.part_dic[b.my_part_id][1][0])
            if node in (b.part_dic[b.my_part_id][1]):
                (b.part_dic[b.my_part_id][1]).remove(node)
        else:
            if node not in b.part_dic[b.my_part_id][0] and node not in b.part_dic[b.my_part_id][1]:
                if len(b.part_dic[b.my_part_id][0])<b.K:
                    promoteNode(node)
                elif len(b.part_dic[b.my_part_id][0])==b.K:
                    # response = requests.get('http://'+node+"/getNodeState")
                    # res = response.json()
                    # node_kv_store = res['kv_store']
                    demoteNode(node)
            # case when a node is removed from replica array, we are not pinging it anyway cause it is not in our view
            elif node in b.part_dic[b.my_part_id][1] and len(b.part_dic[b.my_part_id][0])<b.K:
                    promoteNode(node)


# def partitionChange():
#
#     # if len(getProxyArr()>=K):
#     #     # // form new partition
#     #     # // assign first K proxies to new partition
#     #     # // update part_id_dic for all partitions
#     #     # // redistribute keys
#     #
#     # elif len(getReplicaArr() < K):
#     #     # // reassign part_dic ids
#     #     # // assign leftover nodes to partition 0
#
# def redistributeKeys():
#




def isProxy():
    return (b.my_IP in getProxyArr())

######################################
# for retrieving the rep and prox arrs
######################################
def getReplicaArr():
    if b.part_dic[b.my_part_id][0] is not None:
        return b.part_dic[b.my_part_id][0]
    else:
        return []

def getProxyArr():
    if b.part_dic[b.my_part_id][1] is not None:
        return b.part_dic[b.my_part_id][1]
    else:
        return []

######################################
# class for GET key and PUT key
######################################
class BasicGetPut(Resource):
    # get key with data field "causal_payload = causal_payload"
    def get(self, key):
        # invalid key input
        if keyCheck(key) == False:
            return invalidInput()

        # Get causal_payload info
        # converting unicode to list of ints
        data = request.form.to_dict() # turn the inputted into [key:causal_payload]
        #####################################
        # This client knows nothing. No value for you!
        #####################################
        try:
            sender_kv_store_vector_clock = data['causal_payload']
        except KeyError:
            return cusError('causal_payload key not provided',404)

        if sender_kv_store_vector_clock == '':
            return cusError('empty causal_payload',404)

        if isProxy():
            for node in getReplicaArr():
                try:
                    response = requests.get('http://'+ node + '/kv-store/' + key, data=request.form)
                    return make_response(jsonify(response.json()), response.status_code)
                except:
                    pass

        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
        #####################################
        #if senders causal_payload is less than or equal to mine, I am as, or more up to date
        #####################################
        if checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
            # Check if key is in kvStore before returning
            if key in b.kv_store:
                value = b.kv_store[key][0]
                my_time = b.kv_store[key][1]
                return getSuccess(value, my_time)
            # My payload is more up to date, but I dont have the key, so error
            else:
                return getValueForKeyError()
        else:
            for node in getReplicaArr():
                response = requests.get('http://'+node+'/getNodeState')
                res = response.json()
                # TODO: we will return stale data cause we are promising availability
                if key in res['kv_store']:
                    value = res['kv_store'][key][0]
                    my_time = res['kv_store'][key][1]
                    return getSuccess(value, my_time)
    # put key with data fields "val = value" and "causal_payload = causal_payload"
    def put(self, key):
        # Check for valid input
        if keyCheck(key) == False:
            return invalidInput()
        # Get request data
        data = request.form.to_dict()

        try:
            sender_kv_store_vector_clock = data['causal_payload']
        except KeyError:
            return cusError('causal_payload key not provided',404)

        try:
            value = data['val']
        except KeyError:
            return cusError('val key not provided',404)

        if isProxy():
            for node in getReplicaArr():
                try:
                    response = requests.put('http://'+ node + '/kv-store/' + key, data=request.form)
                    return make_response(jsonify(response.json()), response.status_code)
                except:
                    pass

        ########################################
        # Check for edge case where causal_payload is empty string
        # In this case, its the client's first write, so do it
        ########################################
        if sender_kv_store_vector_clock == '':
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            return putNewKey(my_time)
        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))

        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))

        ########################################
        # Check if their causal payload is strictly greater than or equal to mine, or if the key is new to me
        # If it is, do the write
        ########################################
        #return jsonify({'kv-store vector clock':kv_store_vector_clock,'sender_kv_store_vector_clock':sender_kv_store_vector_clock})
        if (checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock)) or key not in b.kv_store:
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            # this will help debugging
            # response = jsonify({'key':kv_store[key]})
            # return response
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
            return putNewKey(my_time)

        ########################################
        # If neither causal payload is less than or equal to the other, or if they are checkEqual
        # Then the payloads are concurrent, so don't do the write
        ########################################
        if not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
            return cusError('payloads are concurrent',404)

############################################
# class for GET node details
######################################
class GetNodeDetails(Resource):
    # check if the node is a replica or not
    def get(self):
        if (b.my_IP in getProxyArr()):
            return getNodeDetailsNotReplica()
        else:
            return getNodeDetailsReplica()


############################################
# class for GET all replicas
######################################
class GetAllReplicas(Resource):
    # get the list of replicas for current node
    def get(self):
        return getAllReplicasSuccess()

###################################
# class for update view for node
#######################################
class ChangeView(Resource):
    def put(self):
        data = request.form.to_dict()
        b.partition_view = list(set(data['partition_view'].split(','))|set(b.partition_view))
        b.part_dic[b.my_part_id][0] = data['replica_array'].split(',')
        if data['proxy_array'].split(',') == ['']:
            b.part_dic[b.my_part_id][1] = []
        else:
            b.part_dic[b.my_part_id][1] = data['proxy_array'].split(',')
        b.node_ID_dic = json.loads(data['node_ID_dic'])
        return jsonify({'partition_view': b.partition_view})

#########################################
# class for adding a node
#####################################
class AddNode(Resource):
    def put(self):
        data = request.form.to_dict()
        add_node_ip_port = data['ip_port']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
        if add_node_ip_port not in b.partition_view:
            # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
            update(add_node_ip_port, b.my_part_id)
        return jsonify({'node': b.my_IP, 'ip_port': add_node_ip_port})

#########################################
# class for removing a node
#####################################
class RemoveNode(Resource):
    def put(self):
    #    if(isProxy() is True):
        #    return proxy_forward(request.url_rule,request.method,request.form.to_dict(),'')
        data = request.form.to_dict()
        remove_node_ip_port = data['ip_port']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
        if remove_node_ip_port in b.partition_view:
            b.partition_view.remove(remove_node_ip_port)
            b.world_proxy.pop(remove_node_ip_port)
            b.part_clock += 1
            if remove_node_ip_port in getReplicaArr():
                b.part_dic[b.my_part_id][0].remove(remove_node_ip_port)
            elif remove_node_ip_port in getProxyArr():
                b.part_dic[b.my_part_id][1].remove(remove_node_ip_port)
        return jsonify({'node': b.my_IP, 'remove_ip_port': remove_node_ip_port})

############################################
# class for add a node into view
######################################
class UpdateView(Resource):
        # get type is add or remove
    def put(self):
        type = request.args.get('type','')
        if type == None:
            return cusError('No type was specificed',404)

        data = request.form.to_dict()

        try:
            add_node_ip_port = data['ip_port']
        except KeyError:
            return cusError('ip_port key not provided',404)

        if add_node_ip_port == '':
            return cusError('empty ip_port',404)

        if type == 'add':
            # automatically add node as proxy
            if add_node_ip_port not in b.partition_view:
                update(add_node_ip_port, b.my_part_id)
                # give the brand new node its attributes using current node's data
                requests.put('http://'+ add_node_ip_port +'/update_datas',data={
                'part_id':b.my_part_id,
                'world_proxy':json.dumps(b.world_proxy),
                'partition_view':','.join(b.partition_view),
                'part_dic':json.dumps(b.part_dic),
                'kv_store':'{}',
                'node_ID_dic':json.dumps(b.node_ID_dic),
                'kv_store_vector_clock':'.'.join(map(str,b.kv_store_vector_clock)),
                })
                # not already added
                # tell all nodes in view, add the new node
                for node in b.partition_view:
                    if node != add_node_ip_port and node != b.my_IP:
                        try:
                            requests.put('http://'+node+'/addNode', data = {'ip_port': add_node_ip_port})
                        except requests.exceptions.ConnectionError:
                            pass
                # add successfully, update your clock
                return addNodeSuccess(b.node_ID_dic[add_node_ip_port])
            else:
                return addSameNode()
        # remove a node
        elif type == 'remove':
            if add_node_ip_port not in (getReplicaArr() + getProxyArr()) and add_node_ip_port not in b.partition_view:
                return removeNodeDoesNotExist()
            else:
                b.part_clock += 1
                if add_node_ip_port in getReplicaArr():
                    b.part_dic[b.my_part_id][0].remove(add_node_ip_port)
                elif add_node_ip_port in getProxyArr():
                    b.part_dic[b.my_part_id][1].remove(add_node_ip_port)
                    b.world_proxy.pop(add_node_ip_port)

                for node in b.partition_view:
                    if node != add_node_ip_port and node != b.my_IP:
                        try:
                            requests.put('http://'+ node +'/removeNode', data = {'ip_port': add_node_ip_port})
                        except requests.exceptions.ConnectionError:
                            pass

                return removeNodeSuccess()

############################################
# class for updating datas
######################################
class UpdateDatas(Resource):
    def put(self):
        data = request.form.to_dict()
        b.my_part_id = data['part_id']
        b.partition_view = data['partition_view'].split(',')
        b.kv_store = json.loads(data['kv_store'])
        b.node_ID_dic = json.loads(data['node_ID_dic'])
        b.part_dic = json.loads(data['part_dic'])
        b.part_clock = data['part_clock']
        b.world_proxy = json.loads(data['world_proxy'])
        b.kv_store_vector_clock = map(int,data['kv_store_vector_clock'].split('.'))

##############################################
# class for reset the node if node is removed
################################################
class ResetData(Resource):
    def put(self):
        # if(isProxy() is True):
        #     return proxy_forward(request.url_rule,request.method,request.form.to_dict(),request.args)
        b.kv_store={} # key:[value, time_stamp]
        b.node_ID_dic={} # ip_port: node_ID
        b.partition_view=[]
        b.kv_store_vector_clock=[0]*8 # is the pay load
        b.part_dic[b.my_part_id][0]=[] # a list of current replicas IP:Port
        b.part_dic[b.my_part_id][1]=[] # a list of current proxies  IP:Port
        b.part_clock = 0

#######################################
# class for getting key in gossip --> helper
# Always returns the more up to date value for the key
# Overwrites the value of the key if the provided info is newer
######################################
class GetKeyDetails(Resource):
    def get(self, key):
        data = request.form.to_dict() # turn the inputted into [key:causal_payload]
        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
        sender_timestamp = data['timestamp']
        sender_node_id = int(data['nodeID'])
        sender_key_value = data['val']
        # return jsonify({'value': sender_kv_store_vector_clock, 'timestamp': b.kv_store_vector_clock})
        if checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock):
            # return our value
            return getSuccess(b.kv_store[key][0], b.kv_store[key][1])
        elif checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock):
            # write the more up to date value to our kv_store_vector_clock
            b.kv_store[key] = (sender_key_value, sender_timestamp)
            b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
            # b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            # retrun the same value
            return getSuccess(b.kv_store[key][0], b.kv_store[key][1])
        else:
            # break tie with timestamp
            my_key_timestamp = b.kv_store[key][1]
            if my_key_timestamp > sender_timestamp:
                # return my value
                return getSuccess(b.kv_store[key][0], b.kv_store[key][1])
            elif sender_timestamp > my_key_timestamp:
                # write the more up to date value to our kv_store_vector_clock
                b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                # b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                b.kv_store[key] = (sender_key_value, sender_timestamp)
                # retrun the same value
                return getSuccess(b.kv_store[key][0], b.kv_store[key][1])
            else: # they are equal somehow...
                # break tie with nodeID
                if b.node_ID_dic[b.my_IP] > sender_node_id:
                    # return my value
                    return getSuccess(b.kv_store[key][0], b.kv_store[key][1])
                else:
                    # write the more up to date value to our kv_store
                    b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                    # b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                    b.kv_store[key] = (sender_key_value, sender_timestamp)
                    # retrun the same value
                    return getSuccess(b.kv_store[key][0], b.kv_store[key][1])

#######################################
# class for get node state --> helper
######################################
class GetNodeState(Resource):
    def get(self):
        return jsonify({'my_part_id':b.my_part_id, 'part_dic': b.part_dic, 'world_proxy': b.world_proxy, 'partition_members': getReplicaArr() + getProxyArr(), 'proxy_array': getProxyArr(), 'replica_array': getReplicaArr(),
                'kv_store': b.kv_store, 'node_ID_dic': b.node_ID_dic, 'part_clock': b.part_clock,
                'kv_store_vector_clock': '.'.join(map(str,b.kv_store_vector_clock)), 'node_ID': b.node_ID_dic[b.my_IP], 'is_proxy': isProxy(), 'my_IP': b.my_IP})
        # return:
############################################
# class for GET view details -- helper
######################################
class Views(Resource):
    def get(self):
        return jsonify({'VIEW': b.VIEW_init,
                 'myIP': b.my_IP,
                  'IP' : b.IP})
############################################
# class for GET Availability -- helper
######################################
class Availability(Resource):
    def get(self):
        return ping(b.partition_view)

############################################
# promote proxy to a replica
######################################
def promoteNode(promote_node_IP):
    # only proxy nodes come into this func
    # update own things
    (b.part_dic[b.my_part_id][0]).append(promote_node_IP) # add node to rep list
    if promote_node_IP in b.part_dic[b.my_part_id][1]:
        (b.part_dic[b.my_part_id][1]).remove(promote_node_IP) # remove node in prx list
    # ChangeView on node
    res = requests.put("http://"+promote_node_IP+"/changeView", data={'partition_view':','.join(b.partition_view),
    'replica_array':','.join(b.part_dic[b.my_part_id][0]),
    'proxy_array':','.join(b.part_dic[b.my_part_id][1]),
    'node_ID_dic': json.dumps(b.node_ID_dic),
    'part_clock': b.part_clock})
    resp = res.json()
    # Update d.partition_view
    b.partition_view = resp['partition_view']
    return
############################################
# demote replica to a proxy
######################################
#NOTE: took out kv_store from paramaters
def demoteNode(demote_node_IP):
    # only replica nodes come into this func
    # remove me iff my clock is behind others
    # if checkLessEq(node_kv_clock, b.kv_store_vector_clock):
    if demote_node_IP in b.part_dic[b.my_part_id][0]:
        (b.part_dic[b.my_part_id][0]).remove(demote_node_IP)
    (b.part_dic[b.my_part_id][1]).append(demote_node_IP)
    res = requests.put("http://"+demote_node_IP+"/changeView", data={'partition_view':','.join(b.partition_view),
    'replica_array':','.join(b.part_dic[b.my_part_id][0]),
    'proxy_array':','.join(b.part_dic[b.my_part_id][1]),
    'part_clock': b.part_clock,
    'node_ID_dic': json.dumps(b.node_ID_dic)})
    resp = res.json()
    # Update b.partition_view
    b.partition_view = resp['partition_view']

    return
    # else, since I'm newer than others, when it comes my turn to ping others,
    # I'll eventually demote someone else. Therefore, do nothing

############################################
# all messaging functions
######################################

# num_nodes_in_view is the number of nodes in world view
# get a node info successfully
def getSuccess(value, time_stamp):
    num_nodes_in_view = len(b.partition_view)
    response = jsonify({'result': 'success', 'value': value, 'partition_id': b.my_part_id, 'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'timestamp': time_stamp})
    response.status_code = 200
    return response

# put value for a key successfullys
def putNewKey(time_stamp):
    response = jsonify({'result': 'success', 'partition_id': b.my_part_id, 'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'timestamp': time_stamp})
    response.status_code = 201
    return response

# GetNodeDetails message when node is a replica
def getNodeDetailsReplica():
    response = jsonify({'result': 'success', 'replica': 'Yes'})
    response.status_code = 200
    return response

# GetNodeDetails message when node is a replica
def getNodeDetailsNotReplica():
    response = jsonify({'result': 'success', 'replica': 'No'})
    response.status_code = 200
    return response

# there are replicas for the node
def getAllReplicasSuccess():
    response = jsonify({'result': 'success', 'replicas': getReplicaArr()})
    response.status_code = 200
    return response

# error messages
def getValueForKeyError():
    response = jsonify({'result': 'error', 'error': 'no value for key'})
    response.status_code = 404
    return response

# add same node that already in view
def addSameNode():
    response = jsonify({'result': 'error','error': 'you are adding the same node'})
    response.status_code = 404
    return response
# add node successful
def addNodeSuccess(node_ID):
    response = jsonify({'msg': 'success', 'node_id': node_ID, 'number_of_nodes': len(getProxyArr()) + len(getReplicaArr())})
    response.status_code = 200
    return response

# remove node success
def removeNodeSuccess():
    response = jsonify({'result': 'success', 'number_of_partitions': len(b.part_dic)})
    response.status_code = 200
    return response

# remove a node doesn't exist
def removeNodeDoesNotExist():
    response = jsonify({'result': 'error','error': 'you are removing a node that does not exist'})
    response.status_code = 404
    return response

# call get on dead node
def onDeadNode():
    response = jsonify({'result': 'error','error':'Dead Node'})
    response.status_code = 404
    return response

# invalid inputs
def invalidInput():
    response = jsonify({'result':'error','error':'invalid key format'})
    response.status_code = 404
    return response

def putSuccess():
    response = jsonify({'msg':'PUT success!'})
    response.status_code = 201
    return response

def putError():
    response = jsonify({'result': 'error','error':'invalid PUT'})
    response.status_code = 404
    return response
def cusError(message,code):
    response = jsonify({'result':'error','error':message})
    response.status_code = code
    return response
####################################################################
# merges current vector clock with sender's vector clock
##############################################################
def merge(c1,c2):
    if(len(c1) != len(c2)):
        raise CustomException("Vector clocks are not the same length")
    if checkLessEq(c1, c2) or checkLessEq(c2, c1):
        for i in range(0,len(c1)):
            c1[i] = max(c1[i],c2[i])
    return c1

####################################################################
# function to check if vector clocks are concurrent or not
##############################################################
# Returns true if the left array is <= to the right array
# If false is returned for both , then events are concurrent
def checkLessEq(l_arr, r_arr):
    isLessEq = False
    for i in range(0, len(l_arr)):
        if l_arr[i] <= r_arr[i]:
            isLessEq = True
        else:
            isLessEq = False
            break
    return isLessEq


####################################################################
# function to check if the vector clocks are equal
##############################################################
# Return true if all pairwise elements in the array are equal
# Only merge vector clocks if this returns false.
# If it returns true, then the events are concurrent.
def checkEqual(l_arr, r_arr):
    isEq = False
    for i in range(0, len(l_arr)):
        if l_arr[i] == r_arr[i]:
            isEq = True
        else:
            isEq = False
            break
    return isEq

############################################
# Key restriciton checking
######################################
def keyCheck(key):
    char = re.match(r'[a-zA-Z0-9_]+$', key)
    if char:
        charset = True
    else:
        charset = False
    if (charset and len(key)>0 and len(key)<201):
        return True
    else:
        return False

############################################
# Value restriction checking
######################################
def valueCheck(value):
    mb = 1000000 # 1MB
    if getsizeof(value) <= mb:
        return True
    else:
        return False

########################################
# ping node
####################################
def ping(hosts):
    with app.app_context():
        IPs= [host.split(':')[0] for host in hosts]
        responses = [os.system("ping -c 1 "+IP+" -W 1") for IP in IPs]
        return dict(zip(hosts, responses))
###############################################################

class GetPartitionId(Resource):
    #A GET request on "/kv-store/get_partition_id"
    # "result":"success",
    # "partition_id": 3,
    def get(self):
        return jsonify({'result':'success','partition_id': b.my_part_id})

class GetAllPartitionIds(Resource):
    #A GET request on "/kv-store/get_all_partition_ids"
    # "result":"success",
    # "partition_id_list": [0,1,2,3]
    def get(self):
        part_keys = [key for key in b.part_dic]
        return jsonify({'result':'success','partition_id_list': part_keys})

class GetPartitionMembers(Resource):
    #A GET request on "/kv-store/get_partition_members" with data payload "partition_id=<partition_id>"
    # returns a list of nodes in the partition. For example the following curl request curl -X GET
    # http://localhost:8083/kv-store/get_partition_members -d 'partition_id=1' will return a list of nodes in the partition with id 1.
    def get(self):
        data = request.form.to_dict()
        try:
            part_id = data['partition_id']
        except KeyError:
            return cusError('no partition_id key provided',404)

        if(part_id == ''):
            return cusError('empty partition_id',404)

        try:
            id_list = b.part_dic[int(part_id)]
        except KeyError:
            return cusError('partition dictionary does not have key '+part_id,404)

        return jsonify({"result":"success","partition_members":id_list[0]})

# resource method called
api.add_resource(BasicGetPut, '/kv-store/<string:key>')
api.add_resource(GetNodeDetails, '/kv-store/get_node_details')
api.add_resource(GetAllReplicas, '/kv-store/get_all_replicas')
api.add_resource(UpdateView, '/kv-store/update_view')
api.add_resource(UpdateDatas, '/update_datas')
api.add_resource(ResetData, '/reset_data')
api.add_resource(GetPartitionId,'/kv-store/get_partition_id')
api.add_resource(GetAllPartitionIds,'/kv-store/get_all_partition_ids')
api.add_resource(GetPartitionMembers,'/kv-store/get_partition_members')
# helper API calls
api.add_resource(GetNodeState, '/getNodeState')
api.add_resource(AddNode, '/addNode')
api.add_resource(RemoveNode, '/removeNode')
api.add_resource(Views, '/views')
api.add_resource(Availability, '/availability')
api.add_resource(GetKeyDetails, '/getKeyDetails/<string:key>')
api.add_resource(ChangeView, '/changeView')


if __name__ == '__main__':
    initVIEW()
    app.run(host=b.IP, port=8080, debug=True)
