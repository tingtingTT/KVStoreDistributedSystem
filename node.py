from flask import Flask, abort, request, jsonify, make_response, current_app
import re, os, socket, time

class Node():
    def __init__(self):
        # get variables form INV variables
        self.IP_Port = os.environ.get('IPPORT', None)
        self.IP = socket.gethostbyname(socket.gethostname())
        self.K = int(os.getenv('K', None))
        self.views = os.environ.get('VIEW', None)
        self.view_list = self.views.split(',')
        self.num_nodes = len(self.view_list)
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
        while(i < len(self.view_list)-1):
            self.partitions.append(self.view_list[i:i+self.K])
            ID += 1
            i += self.K
        self.assign_partition_ID()

    def assign_partition_ID(self):
        i = 0
        for ips in self.partitions:
            if(self.IP_Port in ips):
                self.partition_ID = i
                return
            i += 1
    def header(self):
        return {'IP Port':self.IP_Port,'IP':self.IP,'k':self.K,'views':self.views,
                'num_nodes':self.num_nodes,'num_paritions':self.num_partitions,'num_proxy':self.num_proxy,'partitions':self.partitions}
