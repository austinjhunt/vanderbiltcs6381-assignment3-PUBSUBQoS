""" Module to perform unit tests against Publisher class for methods that
execute and can be tested independently of the publish/subscribe network """
from src.lib.zookeeper_client import ZookeeperClient
import unittest
import time
import pickle
from src.unit_tests import *
from src.lib.publisher import Publisher
import sys
connected = False
class TestPublisher(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.topics = ['A','B','C']
        self.publisher = Publisher(
            topics=self.topics,
            sleep_period=0.3,
        )
        try:
            global connected
            zk = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'],use_logger=True)
            zk.connect_zk()
            zk.start_session()
            zk.stop_session()
            zk.close_connection()
            connected = True
        except Exception as e:
            print(e)
            print("You need to start the ZooKeeper service (port 2181) before running these tests")
            sys.exit(1)
        super(TestPublisher, self).__init__(*args, **kwargs)

    def setUp(self):
        self.zookeeper_client = ZookeeperClient(zookeeper_hosts=['127.0.0.1:2181'],use_logger=True)
        self.zookeeper_client.connect_zk()
        self.zookeeper_client.start_session()

    def tearDown(self):
        self.zookeeper_client.stop_session()
        self.zookeeper_client.close_connection()

    def test_topics(self):
        # Publisher should store all topics passed on construction.
        for t in self.topics:
            assert t in self.publisher.topics

    def test_indefinite(self):
        # By default, publisher should publish a max of 15 events.
        assert not self.publisher.indefinite
        assert self.publisher.max_event_count == 15

    def test_bind_port(self):
        # Default port is 5556
        assert self.publisher.bind_port == 5556






