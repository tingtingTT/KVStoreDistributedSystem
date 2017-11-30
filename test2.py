#!/usr/bin/python

import unittest
import subprocess
import requests
import sys
import random
import time
from time import sleep
import os
import re

hostname = '192.168.99.100'  # Windows and Mac users can change this to the docker vm ip
sudo = ''
container = 'hw4'

IP_ADDRESSES = ['10.0.0.20', '10.0.0.21', '10.0.0.22', '10.0.0.23']
HOST_PORTS = ['8083', '8084', '8085', '8086']

# SET THIS TO FALSE IF YOU WANT RICHER DEBUG PRINTING.
# Prints the response data for each test, at each step.
TEST_STATUS_CODES_ONLY = False


def stop_all_docker_containers(sudo):
    os.system(sudo + " docker kill $(" + sudo + " docker ps -q)")


class TestHW3(unittest.TestCase):
    """
    Creating a subnet:
        sudo docker network create --subnet 10.0.0.0/16 mynet
    """

    def __kill_node(self, idx):
        global sudo
        cmd_str = sudo + " docker kill %s" % self.node_ids[idx]
        print cmd_str
        os.system(cmd_str)

    @classmethod
    def __start_nodes(cls):
        global sudo, hostname, container
        exec_string_rep1 = sudo + " docker run -p 8083:8080 --net=mynet --ip=10.0.0.20 -e K=3 -e VIEW=10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080 -e IPPORT=10.0.0.20:8080 -d %s" % container
        exec_string_rep2 = sudo + " docker run -p 8084:8080 --net=mynet --ip=10.0.0.21 -e K=3 -e VIEW=10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080 -e IPPORT=10.0.0.21:8080 -d %s" % container
        exec_string_rep3 = sudo + " docker run -p 8085:8080 --net=mynet --ip=10.0.0.22 -e K=3 -e VIEW=10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080 -e IPPORT=10.0.0.22:8080 -d %s" % container
        exec_string_for1 = sudo + " docker run -p 8086:8080 --net=mynet --ip=10.0.0.23 -e K=3 -e VIEW=10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080 -e IPPORT=10.0.0.23:8080 -d %s" % container
        node_ids = []
        print exec_string_rep1
        node_ids.append(subprocess.check_output(exec_string_rep1, shell=True).rstrip('\n'))
        print exec_string_rep2
        node_ids.append(subprocess.check_output(exec_string_rep2, shell=True).rstrip('\n'))
        print exec_string_rep3
        node_ids.append(subprocess.check_output(exec_string_rep3, shell=True).rstrip('\n'))
        print exec_string_for1
        node_ids.append(subprocess.check_output(exec_string_for1, shell=True).rstrip('\n'))
        cls.node_ids = node_ids

        cls.port = {}
        cls.port['10.0.0.20'] = '8083'
        cls.port['10.0.0.21'] = '8084'
        cls.port['10.0.0.22'] = '8085'
        cls.port['10.0.0.23'] = '8086'

        cls.ip_nodeid = {}
        cls.ip_nodeid['10.0.0.20'] = 0
        cls.ip_nodeid['10.0.0.21'] = 1
        cls.ip_nodeid['10.0.0.22'] = 2
        cls.ip_nodeid['10.0.0.23'] = 3

        res = requests.get('http://' + hostname + ':8083/kv-store/get_all_replicas')
        d = res.json()
        replica_ports = d['replicas']
        cls.replicas = []
        for ip_port in replica_ports:
            m = re.match("([0-9\.]*):8080", ip_port)
            cls.replicas.append(m.group(1))

        cls.replica_address = ["http://" + hostname + ":" + cls.port[x] for x in cls.replicas]

        cls.all_nodes = ['10.0.0.20', '10.0.0.21', '10.0.0.22', '10.0.0.23']
        cls.proxies = list(set(cls.all_nodes) - set(cls.replicas))
        cls.proxy_address = ["http://" + hostname + ":" + cls.port[x] for x in cls.proxies]
        cls.causal_payload = {}
        cls.killed_nodes = []

    @classmethod
    def setUpClass(cls):
        TestHW3.__start_nodes()
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        global sudo
        print "Stopping all containers"
        os.system(sudo + " docker kill $(" + sudo + " docker ps -q)")

    def setUp(self):
        # within limit (199 chars)
        self.key1 = 'PgS5W3uzS7lKtY24ARgCEIcb4tEBYYtgct6WoTcwwL1JvYJV_5DKNkzblPEGCj3cavUZ8qi9NwtdpxkS_1YfoI0LETs2DEC7q8KiQlZI0RibE7dBJ9HLuppFjaEPdA4PY9uSlkjUbM0jopy9sin1vKA6A8ldxgEkU1kM4a7jdCo7mykYBrc_owJokxtTqw7DFyJlN_q'
        # at border (200 chars)
        self.key2 = 'n4P2bm7A0zNTHpr9VJs5yYL6zqCJgTBWEOAxDbOLhyVZnp2eTH55B0mZ0FhcyCuwZkulYdCHf_shthzk2RuHVG5QMrXJU1RSXHmyIjalfFEqWrW5fbOELdSuha4AMKF6HbMpq_aImVEzB7dDDDkliRoQhvCe6ICEM81vepjSRsZzACjGQbIubrMHN0pKXaqjS6TPhmQ0'
        # outside limit (201 chars)
        self.key3 = 'aKCG1zAFvXUWiIX75kU8Eq0ugVgz6dV7CItwQaA6oCczaJ_ScwhTSX87RchI9P9TgjDax56mcGWBWAtmUybG3OEh8kWfSTyAxsYyq0NQRQ4et1E4JCgwXq208zh3zpfG5lDyPBA0m5hMnpkSBB0PT0M8muLhmVwBVvYas8QO2CE7AGucjMeNEF4N1GbnRm03kNIRpOW41'
        self.val1 = 'aaaasjdhvksadjfbakdjs'
        self.val2 = 'asjdhvksadjfbakdjs'
        self.val3 = 'bknsdfnbKSKJDBVKpnkjgbsnk'

    """ REPLICA PUT TESTS """

    def test_a_put_nonexistent_via_replica(self):
        # put fresh key via replica
        res = requests.put(self.replica_address[0] + '/kv-store/dog', data={'val': 'bark', 'causal_payload': ''})
        self.assertTrue(res.status_code, [201, '201'])
        d = res.json()
        self.causal_payload['dog'] = d['causal_payload']
        self.assertEqual(d['result'], 'success')
        if not TEST_STATUS_CODES_ONLY:
            print "Test A: Putting non-existent key via replica:"
            print res
            print res.text

    def test_b_put_existing_key_via_replica(self):
        # put existing key via replica with new value
        res = requests.put(self.replica_address[1] + '/kv-store/dog', data={'val': 'woof', 'causal_payload': self.causal_payload['dog']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        if not TEST_STATUS_CODES_ONLY:
            print "Test B: Modifying (put) existing key via replica:"
            print res
            print res.text

    """ REPLICA GET TESTS """

    def test_c_get_nonexistent_key_via_replica(self):
        res = requests.get(self.replica_address[2] + '/kv-store/cat', data={'causal_payload': ''})
        self.assertTrue(res.status_code in [404, '404'])
        d = res.json()
        self.assertEqual(d['result'], 'error')
        if not TEST_STATUS_CODES_ONLY:
            print "Test C: Get non-existent key via replica:"
            print res
            print res.text

    def test_d_get_existing_key_via_replica(self):
        sleep(10)
        res = requests.get(self.replica_address[0] + '/kv-store/dog', data={'causal_payload': self.causal_payload['dog']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'woof')
        if not TEST_STATUS_CODES_ONLY:
            print "Test D: Get existing key via replica:"
            print res
            print res.text

    """ FORWARDING INSTANCE PUT TESTS """

    def test_e_put_nonexistent_key_via_forwarding_instance(self):
        res = requests.put(self.proxy_address[0] + '/kv-store/' + self.key1,data={'val': self.val1, 'causal_payload': ''})
        self.assertTrue(res.status_code, [201, '201'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload[self.key1] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test E: Put non-existing key via forwarding instance:"
            print res
            print res.text

    def test_f_put_existing_key_via_forwarding_instance(self):
        res = requests.put(self.proxy_address[0] + '/kv-store/' + self.key1, data={'val': self.val2, 'causal_payload': self.causal_payload[self.key1]})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload[self.key1] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test F: Put existing key via forwarding instance:"
            print res
            print res.text

    """ FORWARDING INSTANCE GET TESTS """

    def test_g_get_nonexistent_key_via_forwarding_instance(self):
        res = requests.get(self.proxy_address[0] + '/kv-store/' + self.key2, data={'causal_payload': ''})
        self.assertTrue(res.status_code, [404, '404'])
        d = res.json()
        self.assertEqual(d['result'], 'error')
        if not TEST_STATUS_CODES_ONLY:
            print "Test G: Get non-existing key via forwarding instance:"
            print res
            print res.text

    def test_h_get_existing_key_via_forwarding_instance(self):
        res = requests.get(self.proxy_address[0] + '/kv-store/' + self.key1,data={'causal_payload': self.causal_payload[self.key1]})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], self.val2)
        if not TEST_STATUS_CODES_ONLY:
            print "Test H: Get existing key via forwarding instance:"
            print res
            print res.text

    """ BEHAVIOR FOR LAYERED WRITES, RECONCILIATION """

    def test_i_bounded_staleness(self):
        res = requests.put(self.replica_address[0] + '/kv-store/k1', data={'val': 'a', 'causal_payload': ''})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['k_old'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test I: Bounded staleness (putting in the original key on a replica):"
            print res
            print res.text
        res = requests.put(self.replica_address[0] + '/kv-store/k1', data={'val': 'b', 'causal_payload': self.causal_payload['k_old']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['k_new'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test I: Bounded staleness (putting in a new value for that key on that same replica):"
            print res
            print res.text
        sleep(10)
        res = requests.get(self.replica_address[1] + '/kv-store/k1', data={'causal_payload': self.causal_payload['k_old']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertTrue(d['value'], ['a', 'b'])
        if not TEST_STATUS_CODES_ONLY:
            print "Test I: Bounded staleness (retrieving that key from a different replica, expecting the newer value):"
            print res
            print res.text

    def test_j_bounded_staleness(self):
        res = requests.put(self.replica_address[0] + '/kv-store/k2', data={'val': 'a', 'causal_payload': ''})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['k2'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test J: Bounded staleness (putting in the original key):"
            print res
            print res.text
        sleep(10)
        res = requests.get(self.replica_address[1] + '/kv-store/k2', data={'causal_payload': self.causal_payload['k2']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'a')
        if not TEST_STATUS_CODES_ONLY:
            print "Test J: Bounded staleness (retrieving that key from a different replica, expecting the newer value):"
            print res
            print res.text

    def test_k_monotonic_reads(self):
        res = requests.put(self.replica_address[0] + '/kv-store/m', data={'val': 'try', 'causal_payload': ''})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['m'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test K: Monotonic reads (putting in the original key on a replica):"
            print res
            print res.text
        res = requests.put(self.replica_address[0] + '/kv-store/m', data={'val': 'trial', 'causal_payload': self.causal_payload['m']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['m'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test K: Monotonic reads (put a new value for that same key on the same replica as before):"
            print res
            print res.text
        res = requests.put(self.replica_address[1] + '/kv-store/m', data={'val': 'tryout', 'causal_payload': self.causal_payload['m']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['m'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test K: Monotonic reads (put a new value for that same key, on a different replica this time):"
            print res
            print res.text
        res = requests.get(self.replica_address[1] + '/kv-store/m', data={'causal_payload': self.causal_payload['m']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertTrue(d['value'], ['trial', 'tryout'])
        if not TEST_STATUS_CODES_ONLY:
            print "Test K: Monotonic reads (get the value from the second replica, accepting the outcome of second put from *either* replica):"
            print res
            print res.text

    def test_l_transitive_causality(self):
        res = requests.put(self.replica_address[0] + '/kv-store/t', data={'val': 'transitive', 'causal_payload': ''})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['t'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test L: Transitive causality (put key,value on replica0):"
            print res
            print res.text

        res = requests.get(self.replica_address[1] + '/kv-store/t', data={'causal_payload': self.causal_payload['t']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'transitive')
        self.causal_payload['t'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test L: Transitive causality (issue a get for that key to replica1):"
            print res
            print res.text

        res = requests.put(self.replica_address[1] + '/kv-store/u', data={'val': 'later', 'causal_payload': self.causal_payload['t']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.causal_payload['u'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test L: Transitive causality (put a key1,value1 on a replica1):"
            print res
            print res.text

        res = requests.get(self.replica_address[2] + '/kv-store/u', data={'causal_payload': self.causal_payload['u']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'later')
        self.causal_payload['u'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test L: Transitive causality (get key1,value1 from a replica2):"
            print res
            print res.text

        res = requests.get(self.replica_address[2] + '/kv-store/t', data={'causal_payload': self.causal_payload['t']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'transitive')
        self.causal_payload['t'] = d['causal_payload']
        if not TEST_STATUS_CODES_ONLY:
            print "Test L: Transitive causality (get key,value from replica2):"
            print res
            print res.text

    """ VIEW CHANGE TESTS """

    def test_m_add_node(self):
        self.__kill_node(self.ip_nodeid[self.proxies[0]])
        self.__kill_node(self.ip_nodeid[self.replicas[2]])
        self.killed_nodes.append(self.proxies[0])
        self.killed_nodes.append(self.replicas[2])
        exec_string_for1 = sudo + " docker run -p 8087:8080 --net=mynet --ip=10.0.0.24 -e IPPORT=10.0.0.24:8080 -d %s" % container
        self.node_ids.append(subprocess.check_output(exec_string_for1, shell=True).rstrip('\n'))
        self.port['10.0.0.24'] = '8087'
        self.ip_nodeid['10.0.0.24'] = 4
        self.all_nodes.append('10.0.0.24')
        sleep(10)

        res = requests.put(self.replica_address[0] + "/kv-store/update_view?type=add", data={'ip_port': '10.0.0.24:8080'})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        print d
        self.assertEqual(d['msg'], 'success')
        self.assertEqual(d['number_of_nodes'], 3)
        if not TEST_STATUS_CODES_ONLY:
            print "Test M: Add a node (issue put request to add new node):"
            print res
            print res.text

        res = requests.get(self.replica_address[0] + '/kv-store/get_all_replicas')
        d = res.json()
        replica_ports = d['replicas']
        self.replicas = []
        for ip_port in replica_ports:
            m = re.match("([0-9\.]*):8080", ip_port)
            self.replicas.append(m.group(1))

        assert (len(self.replicas) == 3)
        if not TEST_STATUS_CODES_ONLY:
            print "Test M: Add a node (issue get request to query how many replicas in view):"
            print res
            print res.text

        self.replica_address = ["http://" + hostname + ":" + self.port[x] for x in self.replicas]
        self.proxies = list(set(self.all_nodes) - set(self.replicas) - set(self.killed_nodes))
        self.proxy_address = ["http://" + hostname + ":" + self.port[x] for x in self.proxies]

        res = requests.get("http://" + hostname + ":8087/kv-store/dog", data={'causal_payload': self.causal_payload['dog']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'woof')
        if not TEST_STATUS_CODES_ONLY:
            print "Test M: Add a node (issue get request to the new node):"
            print res
            print res.text

        res = requests.get("http://" + hostname + ":8087/kv-store/m", data={'causal_payload': self.causal_payload['m']})
        self.assertTrue(res.status_code, [200, '200'])
        d = res.json()
        self.assertEqual(d['result'], 'success')
        self.assertEqual(d['value'], 'tryout')
        if not TEST_STATUS_CODES_ONLY:
            print "Test M: Add a node (issue another get request to the new node):"
            print res
            print res.text

    # def test_n_remove_node(self):
    #     # kill the node, ruthlessly.
    #     self.__kill_node(self.ip_nodeid[self.replicas[2]])
    #     self.killed_nodes.append(self.replicas[2])
    #
    #     res = requests.put(self.replica_address[0] + "/kv-store/update_view?type=remove", data={'ip_port': str(self.replicas[2]) + ':8080'})
    #     self.assertTrue(res.status_code, [200, '200'])
    #     d = res.json()
    #     self.assertEqual(d['result'], 'success')
    #     self.assertEqual(d['number_of_nodes'], 2)
    #
    #     res = requests.get(self.replica_address[0] + '/kv-store/get_all_replicas')
    #     d = res.json()
    #     replica_ports = d['replicas']
    #     self.replicas = []
    #     for ip_port in replica_ports:
    #         m = re.match("([0-9\.]*):8080", ip_port)
    #         self.replicas.append(m.group(1))
    #
    #     assert (len(self.replicas) == 2)
    #     if not TEST_STATUS_CODES_ONLY:
    #         print "Test N: Remove a node (issue get request to see that node death has been reflected in view):"
    #         print res
    #         print res.text
    #
    #     self.replica_address = ["http://" + hostname + ":" + self.port[x] for x in self.replicas]
    #     self.proxies = list(set(self.all_nodes) - set(self.replicas) - set(self.killed_nodes))
    #     self.proxy_address = ["http://" + hostname + ":" + self.port[x] for x in self.proxies]
    #
    #     res = requests.put(self.replica_address[0] + "/kv-store/0", data={'val': 'degraded', 'causal_payload': ''})
    #     self.assertTrue(res.status_code, [200, '200'])
    #     d = res.json()
    #     self.causal_payload['o'] = d['causal_payload']
    #     self.assertEqual(d['result'], 'success')
    #     if not TEST_STATUS_CODES_ONLY:
    #         print "Test N: Remove a node (issue put key,value to see that even in degraded mode, the write is accepted):"
    #         print res
    #         print res.text
    #
    #     sleep(10)
    #
    #     res = requests.get(self.replica_address[1] + "/kv-store/0", data={'causal_payload': self.causal_payload['o']})
    #     self.assertTrue(res.status_code, [200, '200'])
    #     d = res.json()
    #     self.assertEqual(d['result'], 'success')
    #     self.assertEqual(d['value'], 'degraded')
    #     if not TEST_STATUS_CODES_ONLY:
    #         print "Test N: Remove a node (issue get request for key after 10s to other remaining replica, should get the correct value):"
    #         print res
    #         print res.text


# '''

if __name__ == '__main__':
    unittest.main()
    # os.system(sudo + " docker kill $(" + sudo + " docker ps -q)")
