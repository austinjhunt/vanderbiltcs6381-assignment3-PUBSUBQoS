""" Module to perform unit tests against Broker class for methods that
execute and can be tested independently of the publish/subscribe network """
import unittest
from src.lib.broker import Broker
from src.unit_tests import *

class TestBroker(unittest.TestCase):
    broker = None
    def __init__(self, *args, **kwargs):
        super(TestBroker, self).__init__(*args, **kwargs)
        # Create a broker object.
        # centralized and decentralized have no affect on
        # topology-independent units to be tested.
        self.broker = Broker()
        self.broker.configure()

    def test_get_clear_port(self):
        p = self.broker.get_clear_port()
        assert p >= 10000 and p <= 20000

