""" Module to perform unit tests against Broker class for methods that
execute and can be tested independently of the publish/subscribe network """
import unittest
from src.lib.broker import Broker
from src.unit_tests import *

class TestBroker(unittest.TestCase):
    broker = None
    def setUp(self):
        # Create a broker object.
        # centralized and decentralized have no affect on
        # topology-independent units to be tested.
        self.broker = Broker()
        self.broker.configure()

    def test_get_clear_port(self):
        p = self.broker.get_clear_port()
        assert p >= 10000 and p <= 20000

    def test_get_host_address(self):
        """ Test the get_host_address() method. Since the unittests will not
        be run in mininet, this address will always be 127.0.0.1. Mininet
        host addresses return a very specific data structure that fails outside of
        mininet. """
        addr = self.broker.get_host_address()
        assert addr == '127.0.0.1'

