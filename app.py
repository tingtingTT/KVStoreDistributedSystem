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
        self.world_view=[]
        self.view_vector_clock=[0]*8 # vector clock of the world. Used for gossip
        self.kv_store_vector_clock=[0]*8 # is the pay load
        self.replica_array=[] # a list of current replicas IP:Port
        self.proxy_array=[] # a list of current proxies  IP:Port
        # get variables form INV variables
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
                heartbeat()
                time.sleep(0.050)
    thread = threading.Thread(target=run_job)
    thread.start()

############################################
# init world_view using VIEW
######################################
def update(add_node_ip_port):
    # update view
    # b.world_view.append(add_node_ip_port)
    # check if the ip is already in the dictionary or not,if not, add new ID. if so, do nothing
    if b.node_ID_dic.get(add_node_ip_port) is None:
        b.node_ID_dic[add_node_ip_port] = len(b.node_ID_dic)
    if add_node_ip_port not in b.world_view:
        b.world_view.append(add_node_ip_port)
    # promote to be a replica
    if (len(b.replica_array) < b.K):
        b.replica_array.append(add_node_ip_port)
    else:
    # add the node as a proxy
        b.proxy_array.append(add_node_ip_port)


###########################################################
# functon to init world view using user input VIEW
#####################################################
def initVIEW():
    for node in b.VIEW_list:
        b.world_view.append(node)
        update(node)


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
# functon called intermitantly to sync up the world_view and the kv_stores
###########################################################
def heartbeat():
    # gossip with a random IP in the replicas array
    for node in b.replica_array:
        if(node != b.my_IP):
            gossip(node)
    worldSync()
    time.sleep(.050) #seconds

####################################################################################
# function to check which node is up and down with ping, then promode and demote nodes
######################################################################################
def worldSync():
    while(True):
        tempArr = list(set(b.replica_array) | set(b.proxy_array))
        tryNode = b.replica_array[random.randint(0, len(b.replica_array)-1)]
        if tryNode != b.my_IP:
            try:
                response = requests.get('http://'+tryNode+"/availability", timeout=5)
                res = response.json()
                break
            except requests.exceptions.ConnectionError:
                pass
            except requests.exceptions.Timeout:
                pass

    for node in b.world_view:
        if res[node] != 0: #if the ping result is saying the node is down
            if node in b.replica_array:
                b.replica_array.remove(node)
                if len(b.replica_array)<b.K and len(b.proxy_array)>0:
                    promoteNode(b.proxy_array[0])
            if node in b.proxy_array:
                b.proxy_array.remove(node)
        else:
            if node not in b.replica_array and node not in b.proxy_array:
                if len(b.replica_array)<b.K:
                    promoteNode(node)
                elif len(b.replica_array)==b.K:
                    # response = requests.get('http://'+node+"/getNodeState")
                    # res = response.json()
                    # node_kv_store = res['kv_store']
                    demoteNode(node)


def isProxy():
    return (b.my_IP in b.proxy_array)

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
            return getValueForKeyError()

        if sender_kv_store_vector_clock == '':
            return getValueForKeyError()

        if isProxy():
            for node in b.replica_array:
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
            for node in b.replica_array:
                response = requests.get('http://'+node+'/getNodeState')
                res = response.json()
                if key in res['kv_store']:
                    value = res['kv_store'][key][0]
                    my_time = res['kv_store'][key][1]
                    return getSuccess(value, my_time)
    # put key with data fields "val = value" and "causal_payload = causal_payload"
    def put(self, key):
        from flask import request

        # Check for valid input
        if keyCheck(key) == False:
            return invalidInput()
        # Get request data
        data = request.form.to_dict()

        try:
            value = data['val']
            sender_kv_store_vector_clock = data['causal_payload']
        except KeyError:
            return cusError('incorrect key for dict',404)

        if isProxy():
            for node in b.replica_array:
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
            return putError()

############################################
# class for GET node details
######################################
class GetNodeDetails(Resource):
    # check if the node is a replica or not
    def get(self):
        if (b.my_IP in b.proxy_array):
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
        b.world_view = list(set(data['world_view'].split(','))|set(b.world_view))
        b.replica_array = data['replica_array'].split(',')
        if data['proxy_array'].split(',') == ['']:
            b.proxy_array = []
        else:
            b.proxy_array = data['proxy_array'].split(',')
        b.node_ID_dic = json.loads(data['node_ID_dic'])
        return jsonify({'world_view': b.world_view})

#########################################
# class for adding a node
#####################################
class AddNode(Resource):
    def put(self):
        data = request.form.to_dict()
        add_node_ip_port = data['ip_port']
        add_node_view_clock = date['view_vector_clock']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
        if add_node_ip_port not in b.world_view:
            # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
            update(add_node_ip_port)
            b.view_vector_clock = merge(b.view_vector_clock, add_node_view_clock)
            b.view_vector_clock[b.node_ID_dic[add_node_ip_port]] += 1
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
        add_node_view_clock = date['view_vector_clock']
        # return jsonify({'node': my_IP, 'ip_port': add_node_ip_port})
        if remove_node_ip_port in b.world_view:
            b.view_vector_clock = merge(b.view_vector_clock, add_node_view_clock)
            b.view_vector_clock[b.node_ID_dic[remove_node_ip_port]] += 1
            b.world_view.remove(remove_node_ip_port)
            if remove_node_ip_port in b.replica_array:
                b.replica_array.remove(remove_node_ip_port)
            elif remove_node_ip_port in b.proxy_array:
                b.proxy_array.remove(remove_node_ip_port)
        return jsonify({'node': b.my_IP, 'remove_ip_port': remove_node_ip_port})

############################################
# class for add a node into view
######################################
class UpdateView(Resource):
        # get type is add or remove
    def put(self):
        from flask import request
        type = request.args.get('type','')
        data = request.form.to_dict()
        add_node_ip_port = data['ip_port']
        if type == 'add':
            if add_node_ip_port not in b.world_view:
                update(add_node_ip_port)
                # b.world_view.append(add_node_ip_port)
                # give the brand new node its attributes using current node's data
                if add_node_ip_port in b.replica_array:
                    requests.put('http://'+ add_node_ip_port +'/update_datas',data={
                    'world_view':','.join(b.world_view),
                    'replica_array':','.join(b.replica_array),
                    'proxy_array':','.join(b.proxy_array),
                    'kv_store':json.dumps(b.kv_store),
                    'node_ID_dic':json.dumps(b.node_ID_dic),
                    'view_vector_clock':'.'.join(map(str,b.view_vector_clock)),
                    'kv_store_vector_clock':'.'.join(map(str,b.kv_store_vector_clock)),
                    'num_live_nodes':str(len(b.replica_array) + len(b.proxy_array))
                    })
                else:
                    requests.put('http://'+ add_node_ip_port +'/update_datas',data={
                    'world_view':','.join(b.world_view),
                    'replica_array':','.join(b.replica_array),
                    'proxy_array':','.join(b.proxy_array),
                    'kv_store':'{}',
                    'node_ID_dic':json.dumps(b.node_ID_dic),
                    'view_vector_clock':'.'.join(map(str,b.view_vector_clock)),
                    'kv_store_vector_clock':'.'.join(map(str,b.kv_store_vector_clock)),
                    'num_live_nodes':str(len(b.replica_array) + len(b.proxy_array))
                    })
                # not already added
                # tell all nodes in view, add the new node
                for node in b.world_view:
                    if node != add_node_ip_port and node != b.my_IP:
                        try:
                            requests.put('http://'+node+'/addNode', data = {'ip_port': add_node_ip_port, 'view_vector_clock': b.view_vector_clock})
                        except requests.exceptions.ConnectionError:
                            pass
                # add successfully, update your clock
                b.view_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                return addNodeSuccess(b.node_ID_dic[add_node_ip_port])
            else:
                return addSameNode()
        # remove a node
        elif type == 'remove':
            if add_node_ip_port not in b.world_view:
                return removeNodeDoesNotExist()
            else:
                b.world_view.remove(add_node_ip_port)
                if add_node_ip_port in b.replica_array:
                    b.replica_array.remove(add_node_ip_port)
                elif add_node_ip_port in b.proxy_array:
                    b.proxy_array.remove(add_node_ip_port)

                # requests.put('http://'+add_node_ip_port+'/reset_data')
                for node in b.world_view:
                    if node != add_node_ip_port and node != b.my_IP:
                        try:
                            requests.put('http://'+ node +'/removeNode', data = {'ip_port': add_node_ip_port, 'view_vector_clock': b.view_vector_clock})
                        except requests.exceptions.ConnectionError:
                            pass
                b.view_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                return removeNodeSuccess()


############################################
# class for updating datas
######################################
class UpdateDatas(Resource):
    def put(self):
        data = request.form.to_dict()
        b.world_view = data['world_view'].split(',')
        b.replica_array = data['replica_array'].split(',')
        b.proxy_array = data['proxy_array'].split(',')
        b.kv_store = json.loads(data['kv_store'])
        b.node_ID_dic = json.loads(data['node_ID_dic'])
        b.view_vector_clock = map(int,data['view_vector_clock'].split('.'))
        b.kv_store_vector_clock = map(int,data['kv_store_vector_clock'].split('.'))
        b.num_live_nodes = int(data['num_live_nodes'])

##############################################
# class for reset the node if node is removed
################################################
class ResetData(Resource):
    def put(self):
        # if(isProxy() is True):
        #     return proxy_forward(request.url_rule,request.method,request.form.to_dict(),request.args)
        b.kv_store={} # key:[value, time_stamp]
        b.node_ID_dic={} # ip_port: node_ID
        b.world_view=[]
        b.view_vector_clock=[0]*8 # vector clock of the world. Used for gossip
        b.kv_store_vector_clock=[0]*8 # is the pay load
        b.replica_array=[] # a list of current replicas IP:Port
        b.proxy_array=[] # a list of current proxies  IP:Port

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
            b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
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
                b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
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
                    b.kv_store_vector_clock[b.node_ID_dic[b.my_IP]] += 1
                    b.kv_store[key] = (sender_key_value, sender_timestamp)
                    # retrun the same value
                    return getSuccess(b.kv_store[key][0], b.kv_store[key][1])

#######################################
# class for get node state --> helper
######################################
class GetNodeState(Resource):
    def get(self):
        return jsonify({'world_view': b.world_view, 'replica_array': b.replica_array, 'proxy_array': b.proxy_array,
                'kv_store': b.kv_store, 'node_ID_dic': b.node_ID_dic, 'view_vector_clock': '.'.join(map(str,b.view_vector_clock)),
                'kv_store_vector_clock': '.'.join(map(str,b.kv_store_vector_clock)), 'num_live_nodes': len(b.replica_array) + len(b.proxy_array), 'node_ID': b.node_ID_dic[b.my_IP], 'is_proxy': b.my_IP in b.proxy_array, 'my_IP': b.my_IP})
        # return:
        # num_live_nodes, replica_array, proxy_array, kv_store, node_ID_dic, view_vector_clock
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
        return ping(b.world_view)

####################################################################
# check if I win during a vector clock comparision
##############################################################
def if_I_Win(rep_node_ID, rep_time_stamp, client_node_ID, client_time_stamp):
    # pass time_stamp and node_ID from both the replica and the client
    # return True is I(replica) win, False if Client wins.
    # break tie!
    if rep_time_stamp < client_time_stamp:
        return False # do write!
    elif rep_time_stamp > client_time_stamp:
        return True # do not write!
    else:
        # if still tie, then use node_ID to break tie
        if rep_node_ID < client_node_ID:
            return False # do write!
        else:
            return True # do not write!
############################################
# promote proxy to a replica
######################################
def promoteNode(promote_node_IP):
    # only proxy nodes come into this func
    # update own things
    b.replica_array.append(promote_node_IP) # add node to rep list
    if promote_node_IP in b.proxy_array:
        b.proxy_array.remove(promote_node_IP) # remove node in prx list
    # ChangeView on node
    res = requests.put("http://"+promote_node_IP+"/changeView", data={'world_view':','.join(b.world_view),
    'replica_array':','.join(b.replica_array),
    'proxy_array':','.join(b.proxy_array),
    'num_live_nodes': str(len(b.replica_array) + len(b.proxy_array)),
    'node_ID_dic': json.dumps(b.node_ID_dic)})
    resp = res.json()
    # Update d.world_view
    b.world_view = resp['world_view']
    return
############################################
# demote replica to a proxy
######################################
#NOTE: took out kv_store from paramaters
def demoteNode(demote_node_IP):
    # only replica nodes come into this func
    # remove me iff my clock is behind others
    # if checkLessEq(node_kv_clock, b.kv_store_vector_clock):
    if demote_node_IP in b.replica_array:
        b.replica_array.remove(demote_node_IP)
    b.proxy_array.append(demote_node_IP)
    res = requests.put("http://"+demote_node_IP+"/changeView", data={'world_view':','.join(b.world_view),
    'replica_array':','.join(b.replica_array),
    'proxy_array':','.join(b.proxy_array),
    'num_live_nodes': len(b.replica_array) + len(b.proxy_array),
    'node_ID_dic': json.dumps(b.node_ID_dic)})
    resp = res.json()
    # Update b.world_view
    b.world_view = resp['world_view']

    return
    # else, since I'm newer than others, when it comes my turn to ping others,
    # I'll eventually demote someone else. Therefore, do nothing


############################################
# all messaging functions
######################################

# num_nodes_in_view is the number of nodes in world view
# get a node info successfully
def getSuccess(value, time_stamp):
    num_nodes_in_view = len(b.world_view)
    response = jsonify({'result': 'success', 'value': value, 'node_id': b.node_ID_dic[b.my_IP], 'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'timestamp': time_stamp})
    response.status_code = 200
    return response

# put value for a key successfullys
def putNewKey(time_stamp):
    response = jsonify({'result': 'success', 'node_id': b.node_ID_dic[b.my_IP], 'causal_payload': '.'.join(map(str,b.kv_store_vector_clock)), 'timestamp': time_stamp})
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
    response = jsonify({'result': 'success', 'replicas': b.replica_array})
    response.status_code = 200
    return response

# error messages
def getValueForKeyError():
    response = jsonify({'result': 'error', 'msg': 'no value for key'})
    response.status_code = 404
    return response

# add same node that already in view
def addSameNode():
    response = jsonify({'msg': 'you are adding the same node'})
    response.status_code = 404
    return response
# add node successful
def addNodeSuccess(node_ID):
    response = jsonify({'msg': 'success', 'node_id': node_ID, 'number_of_nodes': len(b.replica_array) + len(b.proxy_array)})
    response.status_code = 200
    return response

# remove node success
def removeNodeSuccess():
    response = jsonify({'result': 'success', 'number_of_nodes': len(b.replica_array) + len(b.proxy_array)})
    response.status_code = 200
    return response

# remove a node doesn't exist
def removeNodeDoesNotExist():
    response = jsonify({'msg': 'you are removing a node that does not exist'})
    response.status_code = 404
    return response

# call get on dead node
def onDeadNode():
    response = jsonify({'error':'Dead Node'})
    response.status_code = 404
    return response

# invalid inputs
def invalidInput():
    response = jsonify({'result':'error','msg':'invalid key format'})
    response.status_code = 404
    return response

def putSuccess():
    response = jsonify({'msg':'PUT success!'})
    response.status_code = 201
    return response

def putError():
    response = jsonify({'error':'invalid PUT'})
    response.status_code = 404
    return response
def cusError(message,code):
    response = jsonify({'error':message})
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
