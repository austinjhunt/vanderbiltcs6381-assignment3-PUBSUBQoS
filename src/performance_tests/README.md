# [Performance Tests](main.py)
Within this directory, we have defined **3 classes**:
1. [PerformanceTest](ptest.py): This class defines the base properties and methods necessary for running performance tests against various publish/subscribe systems each in its own unique network topology. The following is an outline of the under-the-hood mechanics of this class.
```
### CONSTRUCTOR ###
def __init__(self, num_events=50, event_interval=0.3, wait_factor=10)

def cleanup(self):
    """ Method to run the shell command mn -c to clean up existing mininet networks/resources before creating a new one """

def setWaitFactor(self, factor):
    """ Method to update the wait factor (to wait <factor> times as long
    as num events * event interval for pub sub to generate data """

def create_network(self, topo=None):
    """ Method to create a Mininet network with a provided topology;
    handles pre-cleanup if necessary """

def run_network(self, network=None, num_hosts=None, network_name=""):
    """ Interface method; add implementation in subclasses for
    centralized/decentralized performance testing
    args:
    network (Mininet object) - network to start and create hosts on
    num_hosts (int) - number of hosts in network
    network_name - alias of network, used to create folders for data/logs
    """

def test_tree_topology(self, depth=2, fanout=2):
    """ Create and test Pub/Sub on a Tree topology with fanout^depth hosts,
    with one broker and an equal number of subscribers and publishers """

def test_single_switch_topology(self, num_hosts=3):
    """ Create and test Pub/Sub on a Single Switch Topology mininet network
    with a variable number of subscribers and publishers """
```
When creating a performance test, you need to specify **1)** how many events to use as a sample size (which equates to how many records will be written into the data file by the subscriber, where each record indicates the latency for specific message from a specific publisher), **2)** how long to sleep/wait between each publish event (the lower this is, the quicker the test module runs as a whole), and **3)** the wait factor, which is discussed in the main project **README** [here](https://github.com/austinjhunt/vanderbiltcs6381-assignment1-ZMQPUBSUB/tree/master#wait-factor-calculation). The class dynamically handles the cleanup of existing Mininet resources by executing the shell command `mn -c` before running each automated network creation to avoid the `RTNETLINK` error discussed [here](https://github.com/mininet/mininet/issues/737). The `run_network` method of the PerformanceTest class is the only one that does not include an implementation, but requires the subclasses (defined below) to define their own specific implementations for creating either centralized or decentralized Pub/Sub systems within the prepared topologies.

1. [CentralizedPerformanceTest](centralized.py)
```
def run_network(self, network=None, num_hosts=None, network_name=""):
    """ Implementation for centralized performance testing of pub/sub system in provided network
    """
```

2. [DecentralizedPerformanceTest](decentralized.py)
```
def run_network(self, network=None, num_hosts=None, network_name=""):
    """ Implementation for decentralized performance testing of pub/sub system in provided network
    """
```

The biggest differences between the two subclasses are:

1. Of course, the Centralized subclass passes `--centralized` to the driver.py script when creating the Broker and the Subscribers, where the Decentralized subclass does not
2. The Centralized subclass writes **data** to data/centralized/* and the Decentralized subclass writes data to data/decentralized/*
3. The Centralized subclass writes **logs** to logs/centralized/* and the Decentralized subclass writes data to logs/decentralized/*

The [main](main.py) module runs both centralized and decentralized performance tests using those subclasses. To run the main performance tests module:
1. Make sure you've installed the requirements for the project. We recommend using a virtual environment with Python 3.8.
2. `cd` into the `src` directory
3. Run the following command:
   1. `python3 -m performance_tests.main`
   2. Wait for a while for the tests to run and write their data to their respective folders.

