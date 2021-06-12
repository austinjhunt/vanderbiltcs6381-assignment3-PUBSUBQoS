""" Module to perform unit tests against Subscriber class for methods that
execute and can be tested independently of the publish/subscribe network """
import unittest
import os
from src.unit_tests import *
from src.lib.subscriber import Subscriber

class TestSubscriber(unittest.TestCase):
    broker = None
    def setUp(self):
        # Create a Subscriber object.
        # centralized and decentralized have no affect on
        # topology-independent units to be tested.
        self.topics = ['A','B','C']
        # Test file write in same folder
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.filename = os.path.join(__location__, "subscriber_write_test.csv")

        # Subscriber requires a broker to configure. Don't configure for unit testing.
        self.subscriber = Subscriber(
            topics=self.topics,
            filename=self.filename
            )

    def test_centralized(self):
        # Subscriber should use decentralized dissemination by default
        assert not self.subscriber.centralized

    def test_topics(self):
        # Subscriber should store all topics passed on construction.
        for t in self.topics:
            assert t in self.subscriber.topics

    def test_indefinite(self):
        # By default, subscriber should listen for a max of 15 events.
        assert not self.subscriber.indefinite
        assert self.subscriber.max_event_count == 15

    def test_get_host_address(self):
        """ Test the get_host_address() method. Since the unittests will not
        be run in mininet, this address will always be 127.0.0.1. Mininet
        host addresses return a very specific data structure that fails outside of
        mininet. """
        addr = self.subscriber.get_host_address()
        assert addr == '127.0.0.1'

    def test_write_stored_messages(self):
        # First, add some sample messages to stored messages of subscriber
        for i in range(5):
            self.subscriber.received_message_list.append(
                {
                    'publisher': f'publisher-{i}',
                    'topic': ['A','B','C','D','E'][i],
                    'total_time_seconds': i
                }
            )
        self.subscriber.write_stored_messages()
        try:
            with open(self.filename,'r') as f:
                header = f.readline()
                assert header.strip() == 'publisher,topic,total_time_seconds'
                for i,line in enumerate(f.readlines()):
                    assert line.strip() == f"publisher-{i},{['A','B','C','D','E'][i]},{i}"
            os.remove(self.filename)
        except:
            assert False
