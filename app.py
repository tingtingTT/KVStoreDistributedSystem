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
import copy
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
        response = requests.get('http://'+IP+'/getKeyDetails/' + key, data = {
        'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)),
        'val':b.kv_store[key][0],
        'timestamp': b.kv_store[key][1],
        'nodeID':b.node_ID_dic[b.my_IP]})
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
    time.sleep(0.050) #seconds

###########################################################################################
# function to check which node is up and down with ping, then promode and demote nodes
######################################################################################
def worldSync():
    app.logger.info('I AM IN WORLD SYNC')
    if len(getReplicaArr())<b.K:
        # promote your own shit first
        if len(getProxyArr()) > 0:
            promoteNode(getProxyArr()[0])
            app.logger.info('calling promote .......1')

            # Need to sync part dics now
            for partID in b.part_dic.keys():
                if partID != b.my_part_id:
                    replicas = b.part_dic[partID]
                    for rep in replicas:
                        requests.put('http://'+rep+'/syncPartDicProx', data={
                        'part_clock': b.part_clock,
                        'part_dic': json.dumps(b.part_dic),
                        'world_proxy': json.dumps(b.world_proxy)
                        })
        # start asking other people
        elif len(b.world_proxy.keys())>0:
            proxies = b.world_proxy.keys()
            partID = b.world_proxy[proxies[0]]
            replicas = b.part_dic[partID]
            #promote the node
            app.logger.info('calling promote .......2')
            promoteNode(proxies[0])
            b.part_clock += 1
            # make put request to the parition with the proxy to delete it
#             for rep in replicas:
#                 requests.put('http://'+rep+'/deleteProxy', data={
#                 'proxy_node': proxies[0]
#                 })
# #
# #???????????????????????????????????????????????????????????????????????????????????
# might raise issues with tyhe newly created demoteSync function
            # Need to sync part dics now
            for partID in b.part_dic.keys():
                if partID != b.my_part_id:
                    replicas = b.part_dic[partID]
                    for rep in replicas:
                        requests.put('http://'+rep+'/syncPartDicProx', data={
                        'part_clock': b.part_clock,
                        'part_dic': json.dumps(b.part_dic),
                        'world_proxy': json.dumps(b.world_proxy)
                        })
        else:
            app.logger.info('I KNOW I SHOULD DO SHIT')
            demoteAllNodes()
            syncDemote()
            reDistributeKeys()



#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # app.logger.info('PART DIC.....' + str(b.part_dic))
    # app.logger.info('ID.....' + str(b.my_part_id))
    # replicas = b.part_dic[b.my_part_id]
    # for node in replicas:
    #     if node != b.my_IP:
    #         # check world proxy and dic
    #         app.logger.info('I AM CALLING GET PART DIC ON' + str(node))
    #         response = requests.get('http://'+node+'/getPartDic')
    #         res = response.json()
    #         their_part_dic = json.loads(res['part_dic'])
    #         if cmp(b.part_dic, their_part_dic) != 0:


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    #####################################################################
        # Sync everything in our partition. promote or demote as Necessary
    #####################################################################
    while(True):
        tryNode = getReplicaArr()[random.randint(0, len(getReplicaArr())-1)]
        app.logger.info('CALLING AVAILABILITY FROM ' + str(tryNode))
        if tryNode != b.my_IP:
            try:
                response = requests.get('http://'+tryNode+"/availability")
                res = response.json()
                app.logger.info("res = " + str(json.dumps(res)))
                app.logger.info("res keys = " + str(res.keys()))
                break
            except requests.exceptions.ConnectionError:
                pass

    for node in getPartitionView() + b.down_nodes:
        if node in res.keys():
            app.logger.info('PASSED')
            if res[node] != 0: #if the ping result is saying the node is down
                if node in getReplicaArr():
    #????????????????????????????????????????????????????????????????????????????????????????
                    b.part_dic[b.my_part_id].remove(node)
                    if len(getProxyArr())>0:
                        app.logger.info('calling promote .......2')
                        promoteNode(getProxyArr()[0])
    #??????????????????????????????????????????????????????????
    # promote other proxy if we don't have any
                    elif len(b.world_proxy.keys())>0:
                        proxies = b.world_proxy.keys()
                        partID = b.world_proxy[proxies[0]]
                        replicas = b.part_dic[partID]
                        #promote the node
                        app.logger.info('calling promote .......3')
                        promoteNode(proxies[0])
                        # make put request to the parition with the proxy to delete it
                        requests.put('http://'+replicas[0]+'/deleteProxy', data={
                        'proxy_node': proxies[0]
                        })

    #?????????????????????????????????????????????????????

                        # Need to sync part dics now
                        for partID in b.part_dic.keys():
                            if partID != b.my_part_id:
                                replicas = b.part_dic[partID]
                                requests.put('http://'+replicas[0]+'/syncPartDic', data={
                                'part_clock': b.part_clock,
                                'part_dic': json.dumps(b.part_dic)
                                })
                else:
    #????????????????????????????????????????????????????????
    # add to doen nodes forst then demote all nodes
                    for node in getReplicaArr():
                        if node not in down_nodes:
                            b.down_nodes.append(node)
                    demoteAllNodes()


#??????????????????????????????????????????????????????????????????????????????????????
# same thing to ensure part_dic
                app.logger.info('PART DIC.....' + str(b.part_dic))
                app.logger.info('ID.....' + str(b.my_part_id))
                replicas = b.part_dic[b.my_part_id]
                for node in replicas:
                    # check world proxy and dic
                    app.logger.info('I AM CALLING GET PART DIC ON' + str(node))
                    response = requests.get('http://'+node+'/getPartDic')
                    res = response.json()
                    their_part_dic = json.loads(res['part_dic'])
                    if cmp(b.part_dic, their_part_dic) != 0:
                        syncDemote()
                        reDistributeKeys()

                #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # This means you're only responsible for removing nodes that forward to you.
                if node in (getProxyArr()):
                    del b.world_proxy[node]
        else:
            # Review this! will the # of reps ever be less than K in this case???
            if node not in getReplicaArr() and node not in getProxyArr():

                if len(getReplicaArr())<b.K:
                    app.logger.info('calling promote .......4')
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
                        app.logger.info('calling DEmote .......1')
                        demoteNode(node)
                if node in b.down_nodes:
                    b.down_nodes.remove(node)
            # case when a node is removed from replica array, we are not pinging it anyway cause it is not in our view
            elif node in getProxyArr() and len(getReplicaArr())<b.K:
                app.logger.info('calling promote .......5')
                promoteNode(node)

        # Sync world_proxy arrays across clusters
        ##########################################
        syncAll()
        partitionChange()

##################################################################
# function to demote all notes if a partition replica number < K
##################################################################
def demoteAllNodes():
    #app.logger.info('I AM ABOUT TO DEMOTE EVERYONE')
    # my partition no longer hold, change my part_dic
    # No proxies to replace replica, so demote everyone
    new_part_dic = {}
    new_world_proxy = {}
    proxy_node = getReplicaArr()
    temp_part_dic = copy.deepcopy(b.part_dic)
    #app.logger.info('b DIC'+str(b.part_dic))
    del temp_part_dic[b.my_part_id]
    #app.logger.info('b DIC AFTER '+str(b.part_dic))
    new_part_dic = renewPartDic(temp_part_dic)
    temp_world_proxy = copy.deepcopy(b.world_proxy)
    for node in proxy_node:
        temp_world_proxy[node] = "0"
#


    app.logger.info('Setting my dictionary to ' + str(new_part_dic))
    b.world_proxy = temp_world_proxy
    b.part_dic = new_part_dic
    b.my_part_id = "0"
    b.part_clock += 1

    # give my stuff to the next one in the part_dic
    #app.logger.info('BEFORE CALLING SYNCPARTDIC: NEW PART DIC'+str(new_part_dic))
    for partID in new_part_dic.keys():
        #app.logger.info('DICTIONARY'+str(b.part_dic))
        #???????????????????????????????????????????
        # what went wrong earlier
        replicas = new_part_dic[partID]
        for node in replicas:
            app.logger.info('I AM CALLING SYNCPARTDIC ON' + str(node))
            requests.put('http://'+node+'/syncPartDicProxy', data={
            'part_clock': b.part_clock,
            'part_dic': json.dumps(new_part_dic),
            'world_proxy': json.dumps(temp_world_proxy)
            })
    # b.world_proxy = temp_world_proxy
    # b.part_dic = new_part_dic
    # b.my_part_id = "0"


##########################################
# Sync world view when partition is deleted
##########################################
def syncDemote():
    previousWorldProx = b.world_proxy
    previousDic = b.part_dic
    for index in b.part_dic.keys():
        replicas = b.part_dic[index]
        allNodes = replicas + getThierProxies(index)
        for node in allNodes:
            if node != b.my_IP:
                # check world proxy and dic
                response = requests.get('http://'+node+'/getPartDic')
                res = response.json()
                their_part_dic = json.loads(res['part_dic'])
                response = requests.get('http://'+node+'/getWorldProx')
                res = response.json()
                their_world_proxy = json.loads(res['world_proxy'])
                if cmp(b.part_dic, their_part_dic) == 0 and cmp(b.world_proxy, their_world_proxy)==0:
                    previousWorldProx = their_world_proxy
                    previousDic = their_part_dic
                else:
                    syncDemote()

#################################
# sync all world proxy array
#################################
def syncAll():
    syncWorldProx()
    previousWorldProx = b.world_proxy
    for partition in b.part_dic.keys():
        replicas = b.part_dic[partition]
        allNodes = replicas + b.world_proxy.keys()
        for replica in replicas:
            if replica != b.my_IP:
                response = requests.get('http://'+replica+'/getWorldProx')
                res = response.json()
                their_world_prox = json.loads(res['world_proxy'])

                # response = requests.get('http://'+replica+'/getReplicaArr')
                # res = response.json()
                #  their_replica = res['proxy_array'].split(',')

                if cmp(previousWorldProx, their_world_prox) == 0:
                    previousWorldProx = their_world_prox
                else:
                    time.sleep(1)
                    syncAll()

#################################
# check for partition agreement
#################################
def checkPartitionsAgree(checkNodes):
    app.logger.info('checkNodes = ' + str(checkNodes))
    app.logger.info('check we all agree')
    if len(checkNodes) == 0:
        return
    previousDic = b.part_dic
    loopNodes = checkNodes
    for node in loopNodes:
        # if node != b.my_IP:
        # check dic
        time.sleep(1)
        app.logger.info('calling getPartDic on = ' + str(node))
        response = requests.get('http://'+node+'/getPartDic')
        res = response.json()
        their_part_dic = json.loads(res['part_dic'])
        if cmp(b.part_dic, their_part_dic) == 0:
            checkNodes.remove(node)
            app.logger.info('after remove, checkNodes = ' + str(checkNodes))
            previousDic = their_part_dic
        else:
            checkParitionsAgree(checkNodes)

#########################################################
# sync all proxy in world proxy among partitions
######################################################
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


###########################################################
# change partitions if newly added node > K
#####################################################
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
###############################################################
# returns true if there are no duplicate partitions
###############################################################
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



######################################
# returns proxies that point to the part_id passed in
######################################
def getThierProxies(part_id):
    if len(b.world_proxy) > 0:
        proxy_nodes = []
        for node in b.world_proxy:
            if b.world_proxy[node] == part_id:
                proxy_nodes.append(node)
        return proxy_nodes
    else:
        return []



######################################
# for retrieving the rep and prox arrs
########################################
def getReplicaArr():
    if b.my_part_id != "-1":
        # app.logger.info('MY REPLICAS: '+ str(b.part_dic[b.my_part_id]))
        if len(b.part_dic[b.my_part_id]) > 0:
            return b.part_dic[b.my_part_id]
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
            time.sleep(.3)
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

        if sender_kv_store_vector_clock == '':
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            time.sleep(.2)
            return putNewKey(my_time)

        sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
        if (checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock)) or key not in b.kv_store:
            my_time = time.time()
            b.kv_store[key] = (value, my_time)
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
            b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
            time.sleep(.2)
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

        if isProxy():
            the_partition_id = b.world_proxy[b.my_IP]
            my_replicas = getNodesToForwardTo(the_partition_id)
            for node in my_replicas:
                try:
                    response = requests.get('http://'+ node + '/kv-store/' + key, data=request.form)
                    time.sleep(2)
                    return make_response(jsonify(response.json()), response.status_code)
                except:
                    pass

        if sender_kv_store_vector_clock != '':
            sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))

        # if senders causal_payload is less than or equal to mine, I am as, or more up to date
        if (key not in b.kv_store):
            for partID in b.part_dic:
                if(partID != b.my_part_id):
                    node = b.part_dic[partID][0] # 1st node in that partition
                    r = requests.get('http://'+node+'/partition_view/'+key)
                    a = r.json()
                    if (a['key'] == 'True'):
                        r = requests.get('http://'+node+'/kv-store/' + key, data=request.form)
                        return make_response(jsonify(r.json()), r.status_code)
            return cusError('Key does not exist',404)

        if(sender_kv_store_vector_clock == ''):
            value = b.kv_store[key][0]
            my_time = b.kv_store[key][1]
            time.sleep(3)
            return getSuccess(value, my_time)

        if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
            value = b.kv_store[key][0]
            my_time = b.kv_store[key][1]
            time.sleep(3)
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
        # check if key exist
        if(key in b.kv_store):
            if(sender_kv_store_vector_clock != ''):
                sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
                if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
                        my_time = time.time()
                        b.kv_store[key] = (value, my_time)
                        b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                        b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                        #EXTRA SHIT-------------------------------------------
                        for node in b.part_dic[b.my_part_id]:
                            if(node != b.my_IP):
                                requests.put('http://'+node+'/partition_view/' + key, data=request.form)
                        #EXTRA SHIT-------------------------------------------
                        time.sleep(.2)
                        return putNewKey(my_time)
                elif not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
                    return cusError('payloads are concurrent',404)
                else:
                    return cusError('old causal_payload',404)
            else:
                my_time = time.time()
                b.kv_store[key] = (value, my_time)
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                #EXTRA SHIT-------------------------------------------
                for node in b.part_dic[b.my_part_id]:
                    if(node != b.my_IP):
                        requests.put('http://'+node+'/partition_view/' + key, data=request.form)
                #EXTRA SHIT-------------------------------------------
                time.sleep(.2)
                return putNewKey(my_time)
        # check who else has it
        else:
            for partID in b.part_dic:
                if(partID != b.my_part_id):
                    node = b.part_dic[partID][0] # 1st node in that partition
                    r = requests.get('http://'+node+'/partition_view/'+key)
                    a = r.json()
                    if (a['key'] == 'True'):
                        r = requests.put('http://'+node+'/kv-store/' + key, data=request.form)
                        return make_response(jsonify(r.json()), r.status_code)

        # otherwise key does not exist, add new key
        random_part_id = random.randint(0,len(b.part_dic)-1)
        nodeID = random.randint(0, len(b.part_dic[str(random_part_id)])-1)
        node = b.part_dic[str(random_part_id)][nodeID]
        if node == b.my_IP:
            # if happened to be me!
            if sender_kv_store_vector_clock == '':
                my_time = time.time()
                b.kv_store[key] = (value, my_time)
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                #EXTRA SHIT-------------------------------------------
                for node in b.part_dic[b.my_part_id]:
                    if(node != b.my_IP):
                        requests.put('http://'+node+'/partition_view/' + key, data=request.form)
                #EXTRA SHIT-------------------------------------------
                time.sleep(.2)
                return putNewKey(my_time)

            sender_kv_store_vector_clock = map(int,data['causal_payload'].split('.'))
            if checkLessEq(b.kv_store_vector_clock,sender_kv_store_vector_clock):
                my_time = time.time()
                b.kv_store[key] = (value, my_time)
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                b.kv_store_vector_clock = merge(b.kv_store_vector_clock, sender_kv_store_vector_clock)
                #EXTRA SHIT-------------------------------------------
                for node in b.part_dic[b.my_part_id]:
                    if(node != b.my_IP):
                        requests.put('http://'+node+'/partition_view/' + key, data=request.form)
                #EXTRA SHIT-------------------------------------------
                time.sleep(.2)
                return putNewKey(my_time)

            elif not checkLessEq(b.kv_store_vector_clock, sender_kv_store_vector_clock) or not checkLessEq(sender_kv_store_vector_clock, b.kv_store_vector_clock) or not checkEqual(sender_kv_store_vector_clock, b.kv_store_vector_clock):
                return cusError('payloads are concurrent',404)

            else:
                return cusError('Invalid causal_payload',404)
        else:
            r = requests.put('http://'+node+'/partition_view/' + key, data=request.form)
            time.sleep(.2)
            return make_response(jsonify(r.json()), r.status_code)



####################################################################
# renew part_dic when some partition no longer holds
##############################################################
def renewPartDic(temp_part_dic):
    i = 0
    new_part_dic = {}
    for part_index in temp_part_dic.keys():
        replica_array = temp_part_dic[part_index]
        new_part_dic[str(i)] = replica_array
        i += 1
    #app.logger.info('NEW DICTIONARY'+str(new_part_dic))
    return new_part_dic

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
# class for update view when promoting or demoting
#######################################
class PromoteDemote(Resource):
    def put(self):
        data = request.form.to_dict()
        their_clock = int(data['part_clock'])
        if their_clock > b.part_clock:
            b.part_clock = int(data['part_clock'])
            app.logger.info('setting my partiton id to ' + str(data['part_id']))
            b.my_part_id = data['part_id']
            b.part_dic = json.loads(data['part_dic'])
            b.node_ID_dic = json.loads(data['node_ID_dic'])
            b.world_proxy = json.loads(data['world_proxy'])
        return

###################################
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
            # Check if node exists in SYSTEM
            for index in b.part_dic.keys():
                replicas = b.part_dic[index]
                allNodes = replicas + b.world_proxy.keys()
                for node in allNodes:
                    if node == add_node_ip_port:
                        return addSameNode()

            # automatically add node as proxy
            if add_node_ip_port not in getPartitionView():
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
                # time.sleep(5)
                # app.logger.info('my dic for agreement: ' + str(b.part_dic))
                # allNodes = getAllNodes()
                # allNodes.remove(b.my_IP)
                # checkPartitionsAgree(allNodes)
                return addNodeSuccess()
            else:
                return addSameNode()
        # remove a node
        elif type == 'remove':
            # TODO
            if add_node_ip_port not in (getReplicaArr() + getProxyArr()):
                # check if any other partiton has it
                # ?????????????????????????????????????????????????????????????????????????????????
                for index in b.part_dic.keys():
                    if index != b.my_part_id:
                        replicas = b.part_dic[index]
                        response = requests.get('http://'+replicas[0]+'/kv-store/get_partition_members', data={'partition_id': index})
                        data = response.json()
                        members = data['partition_members'].split(',')
                        if add_node_ip_port in members:
                            for member in members:
                                if member != add_node_ip_port:
                                    app.logger.info('PARTITION MEMBER' + str(member))
                                    if data['result'] == "success":
                                        # response = requests.put('http://'+member+'/kv-store/update_view?type=remove', data={'ip_port': add_node_ip_port})
                                        # return make_response(jsonify(response.json()), response.status_code)
                                        response = requests.put('http://'+member+'/kv-store/update_view?type=remove', data={'ip_port': add_node_ip_port})
                                        resp = response.json()
                                        numPartitions = resp['number_of_partitions']
                                        # allNodes = getAllNodes()
                                        # allNodes.remove(b.my_IP)
                                        # checkPartitionsAgree(allNodes)
                                        return removeNodeSuccess(numPartitions)


                else:
                    return removeNodeDoesNotExist()
            else:
                if add_node_ip_port in getReplicaArr():
                    app.logger.info('IM DELETING FROM MY PART DIC')
                    b.part_dic[b.my_part_id].remove(add_node_ip_port)
                elif add_node_ip_port in getProxyArr():
                    del b.world_proxy[add_node_ip_port]


                for node in getPartitionView():
                    if node != add_node_ip_port and node != b.my_IP:
                        try:
                            requests.put('http://'+ node +'/removeNode', data = {'ip_port': add_node_ip_port})
                        except requests.exceptions.ConnectionError:
                            pass
                if len(getReplicaArr())<b.K and len(b.world_proxy.keys())==0:
                    new_part_dic = {}
                    new_world_proxy = {}
                    proxy_node = getReplicaArr()
                    temp_part_dic = copy.deepcopy(b.part_dic)
                    del temp_part_dic[b.my_part_id]
                    new_part_dic = renewPartDic(temp_part_dic)
                    app.logger.info('temp dic = ' + str(json.dumps(temp_part_dic)))
                    b.part_clock += 1
                    requests.put('http://'+add_node_ip_port+'/reset_data')
                    # checkPartitionsAgree()
                    return removeNodeSuccess(len(temp_part_dic))
                else:
                    requests.put('http://'+add_node_ip_port+'/reset_data')
                    # checkPartitionsAgree()
                    return removeNodeSuccess(len(b.part_dic))

                # response =requests.get('http://'+b.my_IP+'/getNodeState')
                # return make_response(jsonify(response.json()), response.status_code)


############################################
# class for updating datas
######################################
class UpdateDatas(Resource):
    def put(self):
        data = request.form.to_dict()
        b.my_part_id = data['part_id']
        b.part_clock = int(data['part_clock'])
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


class GetReplicaArr(Resource):
    def get(self):
        return jsonify({'replica_array':','.join(getReplicaArr())})

class DeleteProxy(Resource):
    def put(self):
        data = request.form.to_dict()
        delete_proxy = data['proxy_node']
        del b.world_proxy[delete_proxy]

        b.part_clock += 1

        return

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
    # if promote_node_IP in worldProxy:
    if promote_node_IP in b.world_proxy.keys():
        app.logger.info('DELETING ' + str(promote_node_IP) + 'FROM WORLD PROX')
        del b.world_proxy[promote_node_IP] # remove node in prx list


    b.part_clock += 1
    # ChangeView on node
    requests.put("http://"+promote_node_IP+"/promoteDemote", data={
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
        b.part_dic[b.my_part_id].remove(demote_node_IP)
    b.world_proxy[demote_node_IP] = b.my_part_id
    requests.put("http://"+demote_node_IP+"/promoteDemote", data={
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
# add proxy nodes into my world proxy
##############################################################
class AddToWorldProxy(Resource):
    def put(self):
        data = request.form.to_dict()
        proxy_array = data['proxy_array'].split(',')
        their_clock = int(data['part_clock'])
        if their_clock > b.part_clock:
            for node in proxy_array:
                b.world_proxy[node] = b.my_part_id

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
            proxies = []
            for node in b.world_proxy.keys():
                if b.world_proxy[node] == part_id:
                    proxies.append(node)
            id_list = proxies + b.part_dic[part_id]
        except KeyError:
            return cusError('partition dictionary does not have key '+part_id,404)

#???????????????????????????????????????????????????????????????????
# changed return type
        return jsonify({"result":"success","partition_members":','.join(id_list)})

#########################################################################################
# Sync the Partition Dictionaries between partitions. Take in part_clock and part_dic
#############################################################################
class SyncPartDic(Resource):
    def put(self):
        #app.logger.info('REPLICA ARR AT START: ' + str(getReplicaArr()))
        data = request.form.to_dict()
        their_part_clock = int(data['part_clock'])
        their_part_dic = json.loads(data['part_dic'])


        if b.part_clock < their_part_clock:
            b.part_clock += 1
            b.part_dic = their_part_dic
            for node in getPartitionView():
                if node != b.my_IP:
                    requests.put("http://"+node+"/changeView", data={
                    'part_id': b.my_part_id,
                    'part_dic':json.dumps(b.part_dic),
                    'node_ID_dic': json.dumps(b.node_ID_dic),
                    'part_clock': b.part_clock,
                    'world_proxy': json.dumps(b.world_proxy)})

        return jsonify({'part_dic':json.dumps(b.part_dic)})


class SyncPartDicProxy(Resource):
    def put(self):
        data = request.form.to_dict()
        their_part_clock = int(data['part_clock'])
        their_part_dic = json.loads(data['part_dic'])
        their_world_proxy = json.loads(data['world_proxy'])
        #app.logger.info('I AM IN SyncPartDic')
        #app.logger.info('THEIR DICTIONARY'+str(their_part_dic))
        #app.logger.info('THEIR CLOCK'+str(their_part_clock))
        #app.logger.info('MY CLOCK'+str(b.part_clock))

        # if b.part_clock < their_part_clock:
        if b.part_clock < their_part_clock:
            b.part_clock += 1
            b.part_dic = their_part_dic
            b.world_proxy = their_world_proxy
            for part_id in b.part_dic.keys():
                if b.my_IP in b.part_dic[part_id]:
                    b.my_part_id = part_id
                    #app.logger.info('MY IP'+str(b.my_IP)+"...MY ID"+str(b.my_part_id))

            #app.logger.info('I AM BEFORE FOR LOOP...')
            for node in getPartitionView():
                #app.logger.info('I JUST GOT TO FOR LOOP...')
                if node != b.my_IP:
                    #app.logger.info('PART ID PASSING...'+str(b.my_part_id))
                    #app.logger.info('PART DIC PASSING...'+str(b.part_dic))
                    #app.logger.info('WORLD PROXY PASSING...'+str(b.world_proxy))
                    #app.logger.info('CLOCK PASSING...'+str(b.part_clock))
                    #app.logger.info('I AM CALLING CHANGEVIEW AT '+str(getReplicaArr()))
                    requests.put("http://"+node+"/changeView", data={
                    'part_id': b.my_part_id,
                    'part_dic':json.dumps(b.part_dic),
                    'node_ID_dic': json.dumps(b.node_ID_dic),
                    'part_clock': b.part_clock,
                    'world_proxy': json.dumps(b.world_proxy)})

    # return jsonify({'part_dic':json.dumps(b.part_dic)})


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
def addNodeSuccess():
    time.sleep(5)
    response = jsonify({'result': 'success', 'number_of_partitions': len(b.part_dic), 'partition_id' : b.my_part_id})
    response.status_code = 200
    return response

# remove node success
def removeNodeSuccess(numPartitions):
    time.sleep(5)
    response = jsonify({'result': 'success', 'number_of_partitions': numPartitions, 'partition_id' : b.my_part_id})
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
# get all nodes in the world
##############################################################
def getAllNodes():
    replicas = []
    for index in b.part_dic.keys():
        replicas += b.part_dic[index]
    return replicas + b.world_proxy.keys()

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
api.add_resource(GetReplicaArr, '/getReplicaArr')
api.add_resource(AddNode, '/addNode')
api.add_resource(RemoveNode, '/removeNode')
api.add_resource(Views, '/views')
api.add_resource(Availability, '/availability')
api.add_resource(GetKeyDetails, '/getKeyDetails/<string:key>')
api.add_resource(ChangeView, '/changeView')
api.add_resource(PromoteDemote, '/promoteDemote')
api.add_resource(UpdateWorldProxy, '/updateWorldProxy')
api.add_resource(PartitionView,'/partition_view/<string:key>')
api.add_resource(SyncPartDic,'/syncPartDic') # part_id and part_clock
api.add_resource(WriteKey, '/writeKey')
api.add_resource(SetPartID, '/setPartID')
api.add_resource(ResetKv, '/resetKv')
api.add_resource(DeleteProxy, '/deleteProxy')
api.add_resource(AddToWorldProxy, '/addToWorldProxy')
api.add_resource(SyncPartDicProxy, '/syncPartDicProxy')




if __name__ == '__main__':
    handler = RotatingFileHandler('foo.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    initVIEW()
    app.run(host=b.IP, port=8080, debug=True)
