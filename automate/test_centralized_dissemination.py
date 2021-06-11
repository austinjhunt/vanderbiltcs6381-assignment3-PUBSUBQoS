import unittest
import time
from mininet.net import Mininet
from mininet.topolib import TreeTopo
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
import logging
from mininet.node import OVSController
from topologies.single_switch_topology import SingleSwitchTopo

class CentralizedTest(unittest.TestCase):
    # All centralized tests
    def test_single_switch_topology(self, subscribers=1, publishers=1):
        "Create and test a simple network"
        num_hosts = subscribers + publishers + 1 # (broker)
        if num_hosts < 3:
            # Raise exception. You need at least one broker, one subscriber and one publisher.
            raise Exception("Topology must include at least 3 hosts")
        topo = SingleSwitchTopo(n=num_hosts)
        net = Mininet(topo, controller = OVSController)
        net.start()
        logging.info( "Dumping host connections" )
        dumpNodeConnections(net.hosts)
        logging.info( "Testing network connectivity" )
        # net.pingAll()
        # net.stop()

        # Run broker in background on h1
        h1 = net.get('h1')
        logging.info( "Before broker creation" )
        h1.cmd('python3 driver.py --broker 1 -v --indefinite &')
        logging.info("After broker creation")
        broker_ip = h1.IP()
        logging.info("Now running Pub/Sub With Centralized Broker Dissemination")
        # Create subscribers first
        for i in range(2, subscribers + 2):
            logging.info("Creating sub")
            h = net.get(f'h{i}')
            h.cmd(
                f'python3 driver.py --subscriber 1 --topic A --topic B --topic C '
                f'--max_event_count 100  --broker_address {broker_ip} --verbose '
                f'--filename results/subscriber_h{i}.csv &'
                )

        # Create publishers
        for i in range(subscribers + 2, num_hosts + 1):
            h = net.get(f'h{i}')
            h.cmd(
                f'python3 driver.py --publisher 1 --topic A --topic B --topic C'
                f' --indefinite --broker_address {broker_ip} --verbose &'
                )
        time.sleep(120)
        net.stop()

if __name__ == "__main__":
    unittest.main()