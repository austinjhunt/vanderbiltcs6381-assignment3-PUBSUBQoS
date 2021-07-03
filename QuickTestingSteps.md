# Quick Test Steps (for simpler Peer Review)
The following is a list of steps you can take to perform two quick tests on the framework without dealing with all of the automation. The two tests are respectively for the centralized message dissemination (where the broker forwards all messages), and for decentralized message dissemination (where pubs and subs are in direct contact).

## These commands have been tested in MacOS and on an Ubuntu 20.04 VM. Each command can be executed in its own terminal window alongside other terminal windows.

### FIRST, start ZooKeeper Server (if not already started)
1. `cd /opt/`
2. `zookeeper/bin/zkServer.sh start`
### Steps for Centralized Testing
1. Navigate to the src directory of the project.
`cd src/`
2. Create TWO centralized brokers to test the Zookeeper-enabled redundancy.
   1. Terminal Window #1
      1. `python3 driver.py --broker 1 --centralized --verbose --indefinite`
   2. Terminal Window #2
      1. `python3 driver.py --broker 1 --centralized --verbose --indefinite`
3. Terminal Window #3. Create a publisher of topic A (publisher doesn't care about centralized or not; everyone is a subscriber from its perspective)
   1. `python3 driver.py --publisher 1 --topics A --verbose --broker_address 127.0.0.1 --indefinite`
4. Terminal Window #4. Create a subscriber of topic A (cares about centralized or not)
`python3 driver.py --subscriber 1 --topics A --verbose --broker_address 127.0.0.1 --centralized --indefinite`
5. Terminate active broker, broker 1 (Terminal Window #1) by pressing CTRL + C on the broker. You should see the publisher and subscriber switch to the new broker in their logs.

### Steps for Decentralized Testing
1. Cd into src directory of project
`cd src/`
2. Create TWO decentralized brokers
   1. Broker 1 - Terminal Window #1.
      1. `python3 driver.py --broker 1 --verbose --indefinite`
   2. Broker 2 - Terminal Window #2.
      1. `python3 driver.py --broker 1 --verbose --indefinite`
3. Terminal Window #3. Create a publisher of topic A (doesn't care about centralized or not)
   1. `python3 driver.py --publisher 1 --topics A --verbose --broker_address 127.0.0.1 --indefinite`
4. Terminal Window #4. Create a subscriber of topic A (cares about centralized or not)
   1. `python3 driver.py --subscriber 1 --topics A --verbose --broker_address 127.0.0.1 --indefinite`
5. Terminate active broker, broker 1 (Terminal Window #1) by pressing CTRL + C on the broker. You should see the publisher and subscriber switch to the new broker in their logs.
