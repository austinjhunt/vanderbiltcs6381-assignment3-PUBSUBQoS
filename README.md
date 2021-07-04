# A Python Framework for Multi-Broker Publish/Subscribe Distributed Systems Built With [ZeroMQ, an asynchronous messaging library](https://zeromq.org/) and [Apache Zookeeper](https://zookeeper.apache.org), a distributed coordination service

This project is an extension of [this ZMQ Pub/Sub Python Framework Project](https://github.com/austinjhunt/vanderbiltcs6381-assignment1-ZMQPUBSUB). It offers a framework for spinning up a publish/subscribe system either on a single host or on a virtualized network with a tool like [Mininet](http://mininet.org/). It offers two main models of message dissemination, namely centralized dissemination (message broker forwards all messages from publisher to subscriber and decouples/anonymizes their communication) and decentralized dissemination (publisher and subscriber speak directly with each other after broker matches them with each other).

The project offers integrated performance / latency analysis by allowing you to configure subscribers to write out latency data (between publishers and subscribers) to a provided filename, which provide insight about how long it takes for messages with specific topics from specific publishers to reach the subscriber (this is done by including the publish time in the message that gets sent).

## How does this project extend the first?
This project extends the first by adding in [Apache ZooKeeper](https://zookeeper.apache.org) for distributed coordination. Specifically, it uses [kazoo, a Python library for ZooKeeper](https://kazoo.readthedocs.io/en/latest/), to handle **multi-broker** pub/sub with **warm passive replication** between brokers. The ZooKeeper usage is completely transparent, meaning if you use one broker, the project functions exactly the same as the first (which did not use ZooKeeper). If you use multiple brokers, ZooKeeper enables all publishers and subscribers to continue functioning as though nothing happened by simply electing the next available broker as the leader.
### How is ZooKeeper used?
When a broker is added to the system for redundancy, it tries to create an **ephemeral** ZooKeeper **znode** called **/broker**. Since the node is ephemeral, it gets removed when the broker that created it leaves or dies. If it creates the znode successfully, that broker is now the leader (the first broker in the system will always be the leader). If another broker already has created the /broker znode, then the current broker acknowledges this and waits for the **ephemeral znode** to disappear (when the current leader dies or leaves). When that happens, it tries creating the znode itself to become the leader. The order in which the brokers enter the system and "request" to create that /broker znode determines the order in which they each become the leader.
While that is happening, the publishers and subscribers in the pub/sub system watch the znode (**/broker**) for changes. When the znode is replaced by a new one, the publishers and subscribers know that the original broker they registered with has died, so they re-connect to the new broker. This is allowed by storing information about the current broker leader on the /broker znode. When the publishers and subscribers see the change happen, they read this leader information from the znode so that they can connect to the new leader and continue functioning as normal.

## Development Environment
To work with this system, you should do the following:
1. Install [VirtualBox](https://www.virtualbox.org/)
2. Set up an Ubuntu Desktop 20.04 virtual machine within VirtualBox. You can download the Ubuntu .iso file [here](https://ubuntu.com/download/server). You can follow [these instructions](https://www.youtube.com/watch?v=x5MhydijWmc) to set up your Ubuntu VM. Proceed when finished setting up your VM and it's started.
3.  Open a Terminal Window in your VM. Become root with: `sudo -i` and enter your sudo password.
4. Run the following commands to install Python3.8 and pip on your VM.
```
apt update
apt install software-properties-common
add-apt-repository ppa:deadsnakes/ppa
apt install python3.8
apt install python3-pip
```
5. Install Mininet using [Option 2: Native Installation from Source](http://mininet.org/download/) on the Mininet homepage, with the Python3 note at the bottom of the page.
```
cd /opt/
git clone git://github.com/mininet/mininet
cd mininet
git checkout -b mininet-2.3.0 2.3.0
cd ..
PYTHON=python3 mininet/util/install.sh -a
```
6. Clone this project into /opt/ on the VM.
```
cd /opt/
git clone https://github.com/austinjhunt/vanderbiltcs6381-assignment2-ZOOKEEPER.git
```
7. Navigate to the project.
```
cd vanderbiltcs6381-assignment2-ZOOKEEPER
```
8.  Now as root, install the Python requirements in the VM.
```
pip install -r requirements.txt
```
9. Now, [install ZooKeeper](https://phoenixnap.com/kb/install-apache-zookeeper) on your Ubuntu VM and start the ZooKeeper service. (Step 6 in the linked instructions)

You have now cloned the project onto an Ubuntu VM, started ZooKeeper, installed Mininet, and installed all Python requirements as root on your VM. Your development environment is ready to go.

## Architecture

The main underlying architecture of the publish/subscribe system is described in the original Pub/Sub Framework project's [README](https://github.com/austinjhunt/vanderbiltcs6381-assignment1-ZMQPUBSUB#architecture).


## Unit Testing [(src/unit_tests)](src/unit_tests/README.md)

For instructions on executing the unit tests, see the [Unit Tests README](src/unit_tests/README.md).

## Performance Testing

To test the performance of this framework (specifically latency for message dissemination across both centralized and decentralized dissemination), we rely on the same underlying performance testing module of the original project, [documented here.](https://github.com/austinjhunt/vanderbiltcs6381-assignment1-ZMQPUBSUB/tree/master/src/performance_tests#readme)

For instructions on executing the Performance Tests, see the [Performance Tests README](src/performance_tests/README.md).
### Performance Patterns Found
There are several interesting observations can be made from the tests that have been run within the Performance Testing Framework:
1. The latency with direct dissemination is in general smaller than that with centralized dissemination.
2. The distribution of latency for direct dissemination is more uniformly distributed without outliers, while there are extra large latencies with centralized dissemination
3. With an increased number of publishers and subscribers, the latency increases for both direct and centralized dissemination.
4. The impact of increased number of publishers and subscribers on centralized dissemination is larger than that on direct dissemination.
5. The relationship between the increase of latency and the increase of publishers/subscribers are non-linear. It seems to be a quadratic relationship. More testing is required to confirm this.


## Quick Test Steps (for Vanderbilt Peer Review)

The following is a list of steps you can take to perform two quick tests on the framework without dealing with all of the automation. The two tests are respectively for the centralized message dissemination (where the broker forwards all messages), and for decentralized message dissemination (where pubs and subs are in direct contact).

### These commands have been tested on an Ubuntu 20.04 VM. Each command can be executed in its own terminal window alongside other terminal windows.

### Testing with localhost

#### FIRST, start ZooKeeper Service (if not already started)

** Please Note: Common default 2181 is used as the port for zookeeper. If different port is used, when providing the `zookeeper_host` argument, it should be changed accordingly. **

Zookeeper Server - Terminal Window #1.
1. `cd /opt/`
2. `zookeeper/bin/zkServer.sh start`

#### Steps for Decentralized Testing
1. Cd into src directory of project
`cd src/`
2. Create TWO decentralized brokers. Since it is on the same localhost for the two brokers, each broker has its own set of ports opened for publisher registration and subscriber registration.
   1. Broker 1 - Terminal Window #2.
      1. `python3 driver.py --broker 1 --verbose --indefinite --zookeeper_host 127.0.0.1:2181 --pub_reg_port 10000 --sub_reg_port 10001`
   2. Broker 2 - Terminal Window #3.
      1. `python3 driver.py --broker 1 --verbose --indefinite --zookeeper_host 127.0.0.1:2181 --pub_reg_port 20000 --sub_reg_port 20001`
3. Create TWO publishers
  1. Publishers 1 - Terminal Window #4.
     1. `python3 driver.py --publisher 1 --verbose --max_event_count 120 --sleep 0.5 --zookeeper_host 127.0.0.1:2181 --topics A --topics B`
  2. Publishers 2 - Terminal Window #5.
     1. `python3 driver.py --publisher 1 --verbose --max_event_count 120 --sleep 0.5 --zookeeper_host 127.0.0.1:2181 --topics B --topics D`
4. Create TWO subscribers. Each subscriber will write the message that they have received to a txt file
 1. Subscriber 1 - Terminal Window #6.
    1. `python3 driver.py --subscriber 1 --verbose --max_event_count 60 --zookeeper_host 127.0.0.1:2181 --topics A --topics D --filename s1_local_direct.txt`
 2. Subscriber 2 - Terminal Window #7.
    1. `python3 driver.py --subscriber 1 --verbose --max_event_count 60 --zookeeper_host 127.0.0.1:2181 --topics B --filename s2_local_direct.txt`
5. Terminate active broker, broker 1 (Terminal Window #2) by pressing CTRL + C on the broker. You should find the standby broker in Terminal Window #3 become active. You should also see the publisher and subscriber switch to the new broker in their logs.

#### Steps for Centralized Testing
** Please Note: To make everything centralized, the `--centralized` parameter needs to be provided when create the broker, publisher and subscriber. All other remain the same **

1. Cd into src directory of project
`cd src/`
2. Create TWO decentralized brokers. Since it is on the same localhost for the two brokers, each broker has its own set of ports opened for publisher registration and subscriber registration.
   1. Broker 1 - Terminal Window #2.
      1. `python3 driver.py --broker 1 --verbose --indefinite --zookeeper_host 127.0.0.1:2181 --pub_reg_port 10000 --sub_reg_port 10001 --centralized`
   2. Broker 2 - Terminal Window #3.
      1. `python3 driver.py --broker 1 --verbose --indefinite --zookeeper_host 127.0.0.1:2181 --pub_reg_port 20000 --sub_reg_port 20001 --centralized`
3. Create TWO publishers
  1. Publishers 1 - Terminal Window #4.
     1. `python3 driver.py --publisher 1 --verbose --max_event_count 120 --sleep 0.5 --zookeeper_host 127.0.0.1:2181 --topics A --topics B --centralized`
  2. Publishers 2 - Terminal Window #5.
     1. `python3 driver.py --publisher 1 --verbose --max_event_count 120 --sleep 0.5 --zookeeper_host 127.0.0.1:2181 --topics B --topics D --centralized`
4. Create TWO subscribers. Each subscriber will write the message that they have received to a txt file
 1. Subscriber 1 - Terminal Window #6.
    1. `python3 driver.py --subscriber 1 --verbose --max_event_count 60 --zookeeper_host 127.0.0.1:2181 --topics A --topics D --filename s1_central_direct.txt --centralized`
 2. Subscriber 2 - Terminal Window #7.
    1. `python3 driver.py --subscriber 1 --verbose --max_event_count 60 --zookeeper_host 127.0.0.1:2181 --topics B --filename s2_central_direct.txt --centralized`
5. Terminate active broker, broker 1 (Terminal Window #2) by pressing CTRL + C on the broker. You should find the standby broker in Terminal Window #3 become active. You should also see the publisher and subscriber switch to the new broker in their logs.
