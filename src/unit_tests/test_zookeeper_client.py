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
        global connected
        try:
            zk = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'])
            connected = True
        except Exception as e:
            print(e)
            print("You need to start the ZooKeeper service (port 2181) before running these tests")
            sys.exit(1)
        super(TestZookeeperClient, self).__init__(*args, **kwargs)

    def setUp(self):
        self.zookeeper_client = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'])
        self.zookeeper_client.connect_zk()

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_start_session(self):
        assert(self.zookeeper_client.start_session())

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_stop_session(self):
        assert(self.zookeeper_client.start_session())
        assert(self.zookeeper_client.stop_session())

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_close_connection(self):
        assert(self.zookeeper_client.close_connection())

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_create_znode(self):
        assert(self.zookeeper_client.create_znode())

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_get_znode_value(self):
        self.zookeeper_client.create_znode(znode_value="Test Value")
        assert(self.zookeeper_client.get_znode_value() == "Test Value")

    @unittest.skipIf(not connected, "Not connected to ZooKeeper. You need to start ZK service.")
    def test_modify_znode_value(self):
        # Create a znode then set its value
        self.zookeeper_client.create_znode()
        assert(self.zookeeper_client.modify_znode_value(
            "this is a new value") == "this is a new value")
