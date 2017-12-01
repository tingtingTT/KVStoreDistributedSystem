from flask import Flask, abort, request, jsonify, make_response, current_app
import re, os, socket, time

class Node():
    def __init__(self):
        self.kv_store={} # key:[value, time_stamp]
        self.node_ID_dic={} # ip_port: node_ID
        self.replica_array={} # part_id: replica_array
        self.world_view= [] # change in initView
        self.view_vector_clock=[0]*8 # vector clock of the world. Used for gossip
        self.kv_store_vector_clock=[0]*8 # is the pay load
        self.proxy_array=[] # a list of current proxies  IP:Port
        self.part_id = -1
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
        self.num_nodes = len(self.VIEW_list)
        self.num_partitions =  self.num_nodes / self.K
        self.num_proxy = self.num_nodes % self.K
        self.partitions = []
        self.partition_ID = None
        self.partition()

    def partition(self):
        # divide view to n/k
        # map views to it's appropriate partition ID
        i = 0
        ID = 0
        while(i < len(self.VIEW_list)-1):
            self.partitions.append(self.VIEW_list[i:i+self.K])
            ID += 1
            i += self.K
        self.assign_partition_ID()

    def assign_partition_ID(self):
        i = 0
        for ips in self.partitions:
            if(self.my_IP in ips):
                self.partition_ID = i
                return
            i += 1
    def header(self):
        return {'IP Port':self.my_IP,'IP':self.IP,'k':self.K_init,'views':self.VIEW_init,
                'num_nodes':self.num_nodes,'num_paritions':self.num_partitions,'num_proxy':self.num_proxy,'partitions':self.partitions}
