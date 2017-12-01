#######################################################
from flask import Flask, abort, request, jsonify, make_response, current_app
from flask_restful import Resource, Api
import json
from sys import getsizeof #for input check
import re, os, socket, time
from node import Node
############################################
# initialize variables
######################################
app = Flask(__name__)
api = Api(app)



n = Node()
class View(Resource):
    def get(self):
        h = n.header()
        return jsonify(h)

class Kv_Store(Resource):
    class get_partition_id(Resource):
        def get(self):
            return jsonify({'partition ID': n.partition_ID})
api.add_resource(View, '/View')
api.add_resource(Kv_Store.get_partition_id,'/kv-store/get_partition_id')


if __name__ == '__main__':
    app.run(n.IP, port=8080, debug=True)
