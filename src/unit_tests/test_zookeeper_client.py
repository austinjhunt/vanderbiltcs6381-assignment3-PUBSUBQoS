""" Module to perform unit tests against Subscriber class for methods that
execute and can be tested independently of the publish/subscribe network """
import unittest
import sys
from src.unit_tests import *
from src.lib.zookeeper_client import ZookeeperClient
connected = False
class TestZookeeperClient(unittest.TestCase):
    broker = None
    def __init__(self, *args, **kwargs):
        try:
            global connected
            zk = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'],use_logger=True)
            connected = True
        except Exception as e:
            print(e)
            print("You need to start the ZooKeeper service (port 2181) before running these tests")
            sys.exit(1)
        super(TestZookeeperClient, self).__init__(*args, **kwargs)
        self.zookeeper_client = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'], verbose=True, use_logger=True)
        self.zookeeper_client.connect_zk()
        self.zookeeper_client.start_session()

    def tearDown(self):
        try:
            self.zookeeper_client.delete_znode(znode_name='/test_znode')
        except:
            pass

    def test_create_znode(self):
        global connected
        if not connected:
            self.skipTest(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
        assert(self.zookeeper_client.create_znode(znode_name='/test_znode'))

    def test_delete_znode(self):
        global connected
        if not connected:
            self.skipTest(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
        assert(self.zookeeper_client.create_znode(znode_name='/test_znode'))
        assert(self.zookeeper_client.delete_znode(znode_name='/test_znode'))

    def test_get_znode_value(self):
        global connected
        if not connected:
            self.skipTest(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
        self.zookeeper_client.create_znode(znode_name='/test_znode', znode_value="Test Value")
        assert(self.zookeeper_client.get_znode_value(znode_name='/test_znode') == "Test Value")

    def test_modify_znode_value(self):
        global connected
        if not connected:
            self.skipTest(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
        # Create a znode then set its value
        self.zookeeper_client.create_znode(znode_name='/test_znode')
        assert(self.zookeeper_client.modify_znode_value(
            znode_name='/test_znode',
            znode_value="this is a new value") == "this is a new value")
