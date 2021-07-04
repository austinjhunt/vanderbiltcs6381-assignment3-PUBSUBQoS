""" Module to perform unit tests against Publisher class for methods that
execute and can be tested independently of the publish/subscribe network """
import unittest
import time
import pickle
from src.unit_tests import *
from src.lib.publisher import Publisher

class TestPublisher(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.topics = ['A','B','C']
        self.publisher = Publisher(
            topics=self.topics,
            sleep_period=0.3,
        )
        super(TestPublisher, self).__init__(*args, **kwargs)

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

    def test_generate_publish_event(self):

        # Only 3 topics, iteration 4 will use topic index 1 (B)
        iteration = 4
        event = self.publisher.generate_publish_event(iteration=iteration)
        compare_topic = self.topics[iteration % len(self.topics)]
        compare_topic_encoded = compare_topic.encode('utf8')
        pickled_event_dict = event[1]
        unpickled_event_dict = pickle.loads(pickled_event_dict)
        assert unpickled_event_dict['publisher'] == '127.0.0.1:5556'
        assert unpickled_event_dict['topic'] == compare_topic
        assert event[0] == b'%b' % compare_topic_encoded
        assert (time.time() - unpickled_event_dict['publish_time']) < 2


