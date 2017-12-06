#######################################################
from flask import Flask, abort, request, jsonify, make_response, current_app
from flask_restful import Resource, Api
import json
from sys import getsizeof #for input check
import re, os, socket, time
import requests
import random
import threading
import logging
from logging.handlers import RotatingFileHandler

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
        self.my_part_id = "-1"
        self.world_proxy = {} # node: id
        self.down_nodes = []
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
        if len(b.part_dic[part_id]) < b.K:
            b.part_dic[part_id].append(add_node_ip_port)
        else:
            b.world_proxy[add_node_ip_port] = part_id

    else:
        b.part_dic[part_id] = [add_node_ip_port]


###########################################################
# functon to init world view using user input VIEW
#####################################################
def initVIEW():
    if len(b.VIEW_list) > 0:
        N = len(b.VIEW_list)
        numPart = N/b.K
        numProx = N%b.K

        for i in range(0, b.K * numPart):
            part_id = str(i/b.K)
            if b.VIEW_list[i] == b.my_IP:
                b.my_part_id = part_id

            update(b.VIEW_list[i], part_id)

        if numProx > 0:
            proxies = b.VIEW_list[b.K * numPart:]
            for proxy in proxies:
                b.world_proxy[proxy] = "0"

    return

######################################################################
# functon called in the heartbeat for syncing the kvstores
###############################################################
def gossip(IP):
    for key in b.kv_store:
        response = requests.get('http://'+IP+'/getKeyDetails/' + key, data = {'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'val':b.kv_store[key][0], 'timestamp': b.kv_store[key][1], 'nodeID':b.node_ID_dic[b.my_IP]})
        res = response.json()
        # return response
        b.kv_store[key] = (res['value'], res['timestamp'])

######################################################################################
# functon called intermitantly to sync up the partition_view and the kv_stores
#################################################################################
def heartbeat():
    # gossip with a random IP in the replicas array
    for node in getReplicaArr():
        if(node != b.my_IP):
            gossip(node)
    if b.my_IP in getReplicaArr():
        worldSync()
    #partitionChange()
    time.sleep(.050) #seconds

###########################################################################################
# function to check which node is up and down with ping, then promode and demote nodes
######################################################################################
def worldSync():
    if len(getReplicaArr())<b.K and len(getProxyArr()) > 0:
        promoteNode(getProxyArr()[0])
    #####################################################################
        # Sync everything in our partition. promote or demote as Necessary
    #####################################################################
    while(True):
        tryNode = getReplicaArr()[random.randint(0, len(getReplicaArr())-1)]
        if tryNode != b.my_IP:
            try:
                response = requests.get('http://'+tryNode+"/availability", timeout=5)
                res = response.json()
                break
            except requests.exceptions.ConnectionError:
                pass
            except requests.exceptions.Timeout:
                pass

    for node in getPartitionView() + b.down_nodes:
        if res[node] != 0: #if the ping result is saying the node is down
            if node in getReplicaArr():
                # TODO: chang to promoting any proxy in word_proxy
                b.part_dic[b.my_part_id].remove(node)
                if len(getProxyArr())>0:
                    promoteNode(getProxyArr()[0])
                else:
                    #TODO:redistribute Keys, exclude my part_id
                    for node in getReplicaArr():
                        demoteNode(node)
                if node not in down_nodes:
                    b.down_nodes.append(node)

            # This means you're only responsible for removing nodes that forward to you.
            if node in (getProxyArr()):
                del b.world_proxy[node]
        else:
            # Review this! will the # of reps ever be less than K in this case???
            if node not in getReplicaArr() and node not in getProxyArr():
                if len(getReplicaArr())<b.K:
                    promoteNode(node)
                elif len(getReplicaArr())==b.K:
                    # response = requests.get('http://'+node+"/getNodeState")
                    # res = response.json()
                    # node_kv_store = res['kv_store']
                    nodeInPartition = False
                    for value in b.part_dic.itervalues():
                        if node in value:
                            nodeInPartition = True
                    if nodeInPartition == False:
                        demoteNode(node)
                if node in b.down_nodes:
                    b.down_nodes.remove(node)
            # case when a node is removed from replica array, we are not pinging it anyway cause it is not in our view
            elif node in getProxyArr() and len(getReplicaArr())<b.K:
                promoteNode(node)

        # Sync world_proxy arrays across clusters
        ##########################################
        syncAll()
        partitionChange()

def syncAll():
    syncWorldProx()
    previousWorldProx = b.world_proxy
    for partition in b.part_dic.keys():
        replicas = b.part_dic[partition]
        for replica in replicas:
            if replica != b.my_IP:
                response = requests.get('http://'+replica+'/getWorldProx')
                res = response.json()
                their_world_prox = json.loads(res['world_proxy'])
                if cmp(previousWorldProx, their_world_prox) == 0:
                    previousWorldProx = their_world_prox
                else:
                    time.sleep(1)
                    syncAll()

def syncWorldProx():
   # TODO: might break. Who knows

    for node in b.world_proxy.keys():
        if b.world_proxy[node] == "-1":
            b.node_ID_dic[node] = len(node_ID_dic)
            b.world_proxy[node] = b.my_part_id


    for partition_id in b.part_dic.keys():
        # if partition_id != b.my_part_id:
        # make API call to first replica in that id

        replicas = b.part_dic[partition_id]

        for node in replicas:
            if node != b.my_IP:
                requests.put('http://' + node + '/updateWorldProxy', data = {
                'proxy_array': ','.join(getProxyArr()),
                'part_id': b.my_part_id,
                'world_proxy_arr': json.dumps(b.world_proxy),
                'my_ip': b.my_IP,
                'part_clock': b.part_clock})

def partitionChange():
    if len(b.world_proxy.keys()) >= b.K:
        numNewPartition = len(b.world_proxy) / b.K
        numLeftProxy = len(b.world_proxy) % b.K

        if numNewPartition > 0:
            noDuplicates = None

            # get ip_port from world_proxy
            world_proxy_arr = b.world_proxy.keys()
            # make new partition using K nodes at a time
            temp_dic = b.part_dic
            new_id = str(len(b.part_dic))
            for i in range (0, numNewPartition):
                current_proxy_arr = world_proxy_arr[b.K*i : b.K*(i+1)]
                noDuplicates = noDuplicatePartitions(current_proxy_arr)
                if noDuplicates:
                    if temp_dic.get(new_id) == None:
                        temp_dic[new_id] = []
                    for node in current_proxy_arr:
                        temp_dic[new_id].append(node)

            b.world_proxy = {}

            if noDuplicates:
                for node in current_proxy_arr:
                    # time.sleep(20)
                    requests.put("http://"+node+"/changeView", data={
                    'part_id': new_id,
                    'part_dic':json.dumps(temp_dic),
                    'node_ID_dic': json.dumps(b.node_ID_dic),
                    'part_clock': b.part_clock,
                    'world_proxy': '{}'})

            b.part_dic = temp_dic

            #redistributeKeys

# returns true if there are no duplicate partitions
def noDuplicatePartitions(proxies):

    for part_id in b.part_dic.keys():
        replicas = b.part_dic[part_id]
        if replicas == proxies:
            return False
    b.part_clock += 1
    return True
############################################
# re-distribute keys among partitions
###############################################
def reDistributeKeys():
    tempkv = b.kv_store
    b.kv_store = {}
    # reset kv for all replicas
    for rep in getReplicaArr():
        requests.put('http://'+rep+'/resetKv')
    for key in tempkv:
        randID = random.randint(0,len(b.part_dic)-1)
        requests.put('http://'+b.part_dic[randID][0]+'/writeKey', data={'key':key, 'val':tempkv[key][0], 'timestamp': tempkv[key][1]})






def isProxy():
    return (b.my_IP in getProxyArr())

###############################################
# for retrieving the rep and prox arrs
########################################
def getReplicaArr():
    if b.my_part_id != "-1":
        if len(b.part_dic[str(b.my_part_id)]) > 0:
            return b.part_dic[str(b.my_part_id)]
    else:
        return []

def getProxyArr():
    if len(b.world_proxy) > 0:
        proxy_nodes = []
        for node in b.world_proxy:
            if b.world_proxy[node] == b.my_part_id:
                proxy_nodes.append(node)
        return proxy_nodes
    else:
        return []

def getPartitionView():
    return getReplicaArr() + getProxyArr()

######################################################
# class for PUT key after random node is chosen
####################################################
class PartitionView(Resource):
    def get(self,key):
        if keyCheck(key) == False:
            return invalidInput()
        else:
            if(key in b.kv_store):
                found = 'True'
            else:
                found = 'False'
            return jsonify({'key':found})

    def put(self,key):
        # Check for valid input
        if keyCheck(key) == False:
            return invalidInput()

        data = request.form.to_dict()
        try:
            sender_kv_store_vector_clock = data['causal_payload']
        except KeyError:
            return cusError('causal_payload key not provided',404)

        try:
            value = data['val']
        except KeyError:
            return cusError('val key not provided',404)
        if((key in b.kv_store) and (sender_kv_store_vector_clock == '')):
            return cusError('duplicated key, causal_payload cannot be empty',404)
        if sender_kv_store_vector_clock == '':
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            return putNewKey(my_time)

        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
        if (checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock)) or key not in b.kv_store:
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
            return putNewKey(my_time)
        if not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
            return cusError('payloads are concurrent',404)


#########################################################
# for proxies to easily get its array of reps
################################################
def getNodesToForwardTo(id):
    return b.part_dic[ipPort]

#####################################
# class for put a key when distribute
#####################################
class WriteKey(Resource):
    def put(self):
        data = request.form.to_dict()
        key = data['key']
        val = data['val']
        timestamp = data['timestamp']
        b.kv_store[key] = (val, timestamp)

#####################################
# class for reset kv
#####################################
class ResetKv(Resource):
    def put(self):
        b.kv_store = {}

######################################
# class for GET key and PUT key
######################################
class BasicGetPut(Resource):
    ### get key with data field "causal_payload = causal_payload"###
    def get(self, key):
        # invalid key input
        if keyCheck(key) == False:
            return invalidInput()
        data = request.form.to_dict() # turn into [key:causal_payload]
        # This client knows nothing. No value for you!
        try:
            sender_kv_store_vector_clock = data['causal_payload']
        except KeyError:
            return cusError('causal_payload key not provided',404)
        if sender_kv_store_vector_clock == '':
            return cusError('empty causal_payload',404)
        if isProxy():
            the_partition_id = b.world_proxy[b.my_IP]
            my_replicas = getNodesToForwardTo(the_partition_id)
            for node in my_replicas:
                try:
                    response = requests.get('http://'+ node + '/kv-store/' + key, data=request.form)
                    return make_response(jsonify(response.json()), response.status_code)
                except:
                    pass

        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
        # if senders causal_payload is less than or equal to mine, I am as, or more up to date
        if (key not in b.kv_store):
            for partnum in b.part_dic:
                node = b.part_dic[partnum][0] # 1st node in that partition
                if node != b.my_IP:
                    r = requests.get('http://'+node+'/partition_view/'+key)
                    a = r.json()
                    if (a['key'] == 'True'):
                        r = requests.get('http://'+node+'/kv-store/' + key, data=request.form)
                        return make_response(jsonify(r.json()), r.status_code)
            return cusError('Key does not exist',404)

        if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
            value = b.kv_store[key][0]
            my_time = b.kv_store[key][1]
            return getSuccess(value, my_time)

        elif not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
            return cusError('payloads are concurrent',404)
        else:
            return cusError('Invalid causal_payload',404)

    ### put key with data fields "val = value" and "causal_payload = causal_payload" ###
    def put(self, key):
        if keyCheck(key) == False:
            return invalidInput()
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

        # partition --> 1st node --> key?
        for partnum in b.part_dic:
            node = b.part_dic[partnum][0] # 1st node in that partition
            if node == b.my_IP:
                if(key in b.kv_store):
                    if(sender_kv_store_vector_clock == ''):
                        return cusError('duplicated key, causal_payload cannot be empty',404)

                    sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
                    if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
                        my_time = time.time()
                        b.kv_store[key] = (value, my_time)
                        b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                        b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                        return putNewKey(my_time)

                    elif not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
                        return cusError('payloads are concurrent',404)
                    else:
                        return cusError('Invalid causal_payload',404)

            else: # if 1st node in partition is NOT me
                app.logger.info(node)
                app.logger.info(key)
                r = requests.get('http://'+node+'/partition_view/'+key)
                if(r.status_code == 404):
                    return make_response(jsonify(r.json()), r.status_code)
                else:
                    a = r.json()
                    if (a['key'] == 'True'):
                        r = requests.put('http://'+node+'/partition_view/' + key, data=request.form)
                        return make_response(jsonify(r.json()), r.status_code)

        # key was never found in the system! Means new key! Add to random partiton!
        random_part_id = random.randint(0,len(b.part_dic)-1)
        nodeID = random.randint(0, len(b.part_dic[str(random_part_id)])-1)
        node = b.part_dic[str(random_part_id)][nodeID]
        if node == b.my_IP:
            # if happened to be me!
            if sender_kv_store_vector_clock == '':
                my_time = time.time()
                b.kv_store[key] = (value, my_time)
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                return putNewKey(my_time)

            sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
            if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
                my_time = time.time()
                b.kv_store[key] = (value, my_time)
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                return putNewKey(my_time)

            elif not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
                return cusError('payloads are concurrent',404)

            else:
                return cusError('Invalid causal_payload',404)
        else:
            r = requests.put('http://'+node+'/partition_view/' + key, data=request.form)
            return make_response(jsonify(r.json()), r.status_code)


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

############################################
# class for update view for node
######################################
class ChangeView(Resource):
    def put(self):
        data = request.form.to_dict()
        their_part_clock = int(data['part_clock'])
        if their_part_clock > b.part_clock:
            b.part_clock = int(data['part_clock'])
            b.my_part_id = data['part_id']
            b.part_dic = json.loads(data['part_dic'])
            b.node_ID_dic = json.loads(data['node_ID_dic'])
            b.world_proxy = json.loads(data['world_proxy'])

        return

class SetPartID(Resource):
    def put(self):
        data=request.form.to_dict()
        their_part_clock = int(data['part_clock'])
        their_part_id = data['part_id']
        if their_part_clock >= b.part_clock:
            b.my_part_id = their_part_id
            b.part_clock = their_part_clock

###################################
# class for updating world_proxy with other clusters
#######################################
class UpdateWorldProxy(Resource):
    def put(self):
        data = request.form.to_dict()
        their_proxies = data['proxy_array'].split(',')
        their_world_prox = json.loads(data['world_proxy_arr'])
        their_id = data['part_id']
        their_part_clock = int(data['part_clock'])
        their_IP = data['my_ip']

        if cmp(b.world_proxy, their_world_prox) == 0:
            return
        # elif b.my_IP in their_proxies:
        #     return
        elif their_part_clock > b.part_clock:
            response = requests.get('http://'+their_IP+'/getPartDic')
            res = response.json()
            b.part_dic = json.loads(res['part_dic'])
            b.world_proxy = their_world_prox
            b.part_clock+=1
            return
        elif their_part_clock == b.part_clock and cmp(b.world_proxy, their_world_prox) != 0:

            for value in b.part_dic.itervalues():
                if any(their_proxies) in value:
                    return

            # Take out everything we know about thier proxies
            for node in b.world_proxy.keys():
                if b.world_proxy[node] == their_id:
                    del b.world_proxy[node]

            if their_proxies[0] != "":
                # Reload world_proxy with new data
                for node in their_proxies:
                    b.world_proxy[node] = their_id
            return

#########################################
# class for adding a node
#####################################
class AddNode(Resource):
    def put(self):
        data = request.form.to_dict()
        add_node_ip_port = data['ip_port']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
        if add_node_ip_port not in getPartitionView():
            # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
            update(add_node_ip_port, b.my_part_id)
        return jsonify({'node': b.my_IP, 'ip_port': add_node_ip_port})

#########################################
# class for removing a node
#####################################
class RemoveNode(Resource):
    def put(self):
        data = request.form.to_dict()
        remove_node_ip_port = data['ip_port']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
            # b.part_clock += 1
        if remove_node_ip_port in getReplicaArr():
            b.part_dic[b.my_part_id].remove(remove_node_ip_port)
        elif remove_node_ip_port in getProxyArr():
            del b.world_proxy[remove_node_ip_port]
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
            # check if the node you're trying to add is alive in the world
            for partNum in b.part_dic:
                listOfReps = b.part_dic[partNum]
                for node in listOfReps:
                    if node == add_node_ip_port:
                        return addSameNode()
            # exit loop if node is not in any partition

            # Is node in world_proxy?
            for prox in b.world_proxy:
                if prox == add_node_ip_port:
                    return addSameNode()
            # exit loop if node is not a proxy neither

            # automatically add node as proxy
            update(add_node_ip_port, b.my_part_id)
            # give the brand new node its attributes using current node's data
            requests.put('http://'+ add_node_ip_port +'/update_datas',data={
            'part_id':b.my_part_id,
            'world_proxy':json.dumps(b.world_proxy),
            'part_dic':json.dumps(b.part_dic),
            'part_clock': b.part_clock,
            'kv_store':'{}',
            'node_ID_dic':json.dumps(b.node_ID_dic),
            'kv_store_vector_clock':'.'.join(map(str,b.kv_store_vector_clock)),
            })
            # not already added
            # tell all nodes in view, add the new node

            for node in getPartitionView():
                if node != add_node_ip_port and node != b.my_IP:
                    try:
                        requests.put('http://'+node+'/addNode', data = {'ip_port': add_node_ip_port})
                    except requests.exceptions.ConnectionError:
                        pass
            time.sleep(1)
            return addNodeSuccess(b.node_ID_dic[add_node_ip_port])


        # remove a node
        elif type == 'remove':
            if add_node_ip_port in getReplicaArr():
                b.part_dic[b.my_part_id].remove(add_node_ip_port)
            elif add_node_ip_port in getProxyArr():
                del b.world_proxy[add_node_ip_port]

            for node in getPartitionView():
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
        b.kv_store = json.loads(data['kv_store'])
        b.node_ID_dic = json.loads(data['node_ID_dic'])
        b.part_dic = json.loads(data['part_dic'])
        b.world_proxy = json.loads(data['world_proxy'])
        b.kv_store_vector_clock = map(int,data['kv_store_vector_clock'].split('.'))
        return

############################################
# class for getting part_dic
######################################
class GetPartDic(Resource):
    def get(self):
        return jsonify({'part_dic': json.dumps(b.part_dic)})
class GetWorldProx(Resource):
    def get(self):
        return jsonify({'world_proxy': json.dumps(b.world_proxy), 'part_clock': b.part_clock})

######################################################
# class for reset the node if node is removed
################################################
class ResetData(Resource):
    def put(self):
        # if(isProxy() is True):
        #     return proxy_forward(request.url_rule,request.method,request.form.to_dict(),request.args)
        b.kv_store={} # key:[value, time_stamp]
        b.node_ID_dic={} # ip_port: node_ID
        b.kv_store_vector_clock=[0]*8 # is the pay load
        b.part_dic[b.my_part_id]=[] # a list of current replicas IP:Port
        b.world_proxy[b.my_part_id]=[] # a list of current proxies  IP:Port
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
        sender_node_id = data['nodeID']
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
        return jsonify({'my_part_id':b.my_part_id, 'part_dic': b.part_dic, 'world_proxy': b.world_proxy, 'partition_view': getPartitionView(), 'proxy_array': getProxyArr(), 'replica_array': getReplicaArr(),
                'kv_store': b.kv_store, 'node_ID_dic': b.node_ID_dic, 'part_clock': b.part_clock,
                'kv_store_vector_clock': '.'.join(map(str,b.kv_store_vector_clock)), 'is_proxy': isProxy(), 'my_IP': b.my_IP})
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
        return ping(getPartitionView()+b.down_nodes)

############################################
# promote proxy to a replica
######################################
def promoteNode(promote_node_IP):
    # only proxy nodes come into this func
    # update own things
    b.part_dic[b.my_part_id].append(promote_node_IP) # add node to rep list
    if promote_node_IP in getProxyArr():
        del b.world_proxy[promote_node_IP] # remove node in prx list
    # ChangeView on node
    requests.put("http://"+promote_node_IP+"/changeView", data={
    'part_id': b.my_part_id,
    'part_dic':json.dumps(b.part_dic),
    'node_ID_dic': json.dumps(b.node_ID_dic),
    'part_clock': b.part_clock,
    'world_proxy': json.dumps(b.world_proxy)})
    return
############################################
# demote replica to a proxy
######################################
#NOTE: took out kv_store from paramaters
def demoteNode(demote_node_IP):
    # only replica nodes come into this func
    # remove me iff my clock is behind others
    # if checkLessEq(node_kv_clock, b.kv_store_vector_clock):
    if demote_node_IP in getReplicaArr():
        del b.world_proxy[demote_node_IP]
    b.world_proxy[demote_node_IP] = b.my_part_id
    requests.put("http://"+demote_node_IP+"/changeView", data={
    'part_id': b.my_part_id,
    'part_dic':json.dumps(b.part_dic),
    'part_clock': b.part_clock,
    'node_ID_dic': json.dumps(b.node_ID_dic),
    'world_proxy': json.dumps(b.world_proxy)})


    return
    # else, since I'm newer than others, when it comes my turn to ping others,
    # I'll eventually demote someone else. Therefore, do nothing

####################################################################
# merges current vector clock with sender's vector clock
##############################################################
class GetPartitionId(Resource):
    #A GET request on "/kv-store/get_partition_id"
    # "result":"success",
    # "partition_id": 3,
    def get(self):
        return jsonify({'result':'success','partition_id': b.my_part_id})

####################################################################
# merges current vector clock with sender's vector clock
##############################################################
class GetAllPartitionIds(Resource):
    #A GET request on "/kv-store/get_all_partition_ids"
    # "result":"success",
    # "partition_id_list": [0,1,2,3]
    def get(self):
        part_keys = [key for key in b.part_dic]
        return jsonify({'result':'success','partition_id_list': part_keys})

####################################################################
# merges current vector clock with sender's vector clock
##############################################################
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

        if(part_id == ""):
            return cusError('empty partition_id',404)

        try:
            id_list = b.part_dic[part_id]
        except KeyError:
            return cusError('partition dictionary does not have key '+part_id,404)

        return jsonify({"result":"success","partition_members":id_list[0]})

#########################################################################################
# Sync the Partition Dictionaries between partitions. Take in part_clock and part_dic
#############################################################################
class SyncPartDic(Resource):
    def put(self):
        data = request.form.to_dict()
        their_part_clock = int(data['part_clock'])
        their_part_dic = json.loads(data['part_dic'])


        if b.part_clock < their_part_clock:
            b.part_clock += 1
            b.part_dic = their_part_dic
            for replica in getReplicaArr():
                if replica != b.my_IP:
                    requests.put("http://"+replica+"/changeView", data={
                    'part_id': b.my_part_id,
                    'part_dic':json.dumps(b.part_dic),
                    'node_ID_dic': json.dumps(b.node_ID_dic),
                    'part_clock': b.part_clock,
                    'world_proxy': json.dumps(b.world_proxy)})

        return jsonify({'part_dic':json.dumps(b.part_dic)})

####################################################
# all messaging functions
##############################################
# num_nodes_in_view is the number of nodes in world view
# get a node info successfully
def getSuccess(value, time_stamp):
    num_nodes_in_view = len(getPartitionView())
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
    response = jsonify({'result': 'success', 'number_of_partitions': len(b.part_dic), 'partition_id' : b.my_part_id})
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
    response = jsonify({'result':'PUT success!'})
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
api.add_resource(GetPartDic, '/getPartDic')
api.add_resource(GetWorldProx, '/getWorldProx')
api.add_resource(AddNode, '/addNode')
api.add_resource(RemoveNode, '/removeNode')
api.add_resource(Views, '/views')
api.add_resource(Availability, '/availability')
api.add_resource(GetKeyDetails, '/getKeyDetails/<string:key>')
api.add_resource(ChangeView, '/changeView')
api.add_resource(UpdateWorldProxy, '/updateWorldProxy')
api.add_resource(PartitionView,'/partition_view/<string:key>')
api.add_resource(SyncPartDic,'/syncPartDic') # part_id and part_clock
api.add_resource(WriteKey, '/writeKey')
api.add_resource(SetPartID, '/setPartID')
api.add_resource(ResetKv, '/resetKv')

if __name__ == '__main__':
    handler = RotatingFileHandler('foo.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    initVIEW()
    app.run(host=b.IP, port=8080, debug=True)
