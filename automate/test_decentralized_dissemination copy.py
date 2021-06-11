import unittest
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.node import OVSController

class CentralizedTest(unittest.TestCase):
    # All centralized tests
    publisher_command

    # FIXME: randomize topics
    PUBLISHER_COMMAND = 'python driver.py -pub 1 -t A -m 30 -b'
    SUBSCRIBER_COMMAND = 'python driver.py -sub 1 -t A -t B -t C -m 30 -b 127.0.0.1 -v'

    def test_single_switch_topology(self, subscribers=1, publishers=1):
        "Create and test a simple network"
        num_hosts = subscribers + publishers + 1 # (broker)
        if num_hosts < 3:
            # Raise exception. You need at least one broker, one subscriber and one publisher.
            raise Exception("Topology must include at least 3 hosts")
        topo = SingleSwitchTopo(n=num_hosts)
        net = Mininet(topo, controller = OVSController)
        net.start()
        print( "Dumping host connections" )
        dumpNodeConnections(net.hosts)
        # print( "Testing network connectivity" )
        # net.pingAll()
        # net.stop()
        print("Now running Pub/Sub With Centralized Broker Dissemination")
        for i in range(subscribers):
            h = net.get(f'h{i + 1}')
            h.cmd()

        for i in range(publishers):
            h = net.get(f'h{subscribers + i + 1}')
            h.cmd('python3')


if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    test(num_hosts=2)