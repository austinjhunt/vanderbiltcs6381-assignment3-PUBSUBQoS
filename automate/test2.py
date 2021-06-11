import unittest
import time
from mininet.net import Mininet
from mininet.topolib import TreeTopo
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
import logging
from mininet.node import OVSController
from topologies.single_switch_topology import SingleSwitchTopo
logging.basicConfig(level=logging.DEBUG, format='%(prefix)s - %(message)s')
NUM_MAX_EVENTS = 10

# All centralized tests
def main(subscribers=1, publishers=1):
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
    print( "Testing network connectivity" )
    net.pingAll()

    # Run broker in background on h1
    h1 = net.get('h1')
    print( "Before broker creation" )
    h1.cmd(
        f'python3 ../driver.py --broker 1 -v --indefinite --centralized &'
    )
    # print(output)
    print("After broker creation")
    broker_ip = h1.IP()
    print(f"Broker IP is {broker_ip}")
    print("Now running Pub/Sub With Centralized Broker Dissemination")
    # Create subscribers first
    for i in range(2, subscribers + 2):
        print("Creating sub")
        h = net.get(f'h{i}')
        print(f"Sub host: {h}")
        h.cmd(
            f'python3 ../driver.py --subscriber 1 --topics A --topics B --topics C '
            f'--max_event_count {NUM_MAX_EVENTS} --broker_address {broker_ip} --verbose '
            f'--filename results/subscriber_h{i}.csv --centralized &'
            )
        print("After sub cmd()")

    # Create publishers
    for i in range(subscribers + 2, num_hosts + 1):
        print("Creating pub")
        h = net.get(f'h{i}')
        print(f"Sub host: {h}")
        h.cmd(
            f'python3 ../driver.py --publisher 1 --sleep 0.3 --topics A --topics B --topics C'
            f' --indefinite --broker_address {broker_ip} --verbose --centralized &'
            )
        print("After pub cmd()")
    time.sleep(30)
    net.stop()

if __name__ == "__main__":
    main()