


## Classes

### Subscriber
Class representing a subscriber. A subscriber requires a list of publisher addresses to which it can connect, and a list of topics which it should subscribe to / "listen for" on those connections. The subscriber class is **indifferent** to the information dissemination method, in that the list of publishers it "knows" could be a **single intermediate message broker** that anonymizes the publishers, **or** it could be an explicit list of **publisher IP addresses** that were created **before** the subscriber. The subscriber class provides methods for **ad-hoc additions** of new publisher connections if new publishers are added to the topology after it is initialized, and it also allows easy disconnection from either **all publishers** or **specific publishers**. The subscriber can listen for published updates indefinitely (with --indefinite argument to driver.py) or for a specific number of publish events (with --max_event_count COUNT argument to driver.py). If you pass both --indefinite and --max_event_count COUNT to the driver, indefinite takes priority. 

### Publisher 
Class representing a publisher. One or more publishers can be created at a time using the driver. If you create more than one publisher at a time (e.g. ./driver.py --publisher N>1 ), the first publisher will be bound to the bind port you specify (or 5556 if none specified), then the following publisher will be bound to the next port, etcetera. The publisher publishes information independently of the subscriber classes, so if it publishes an update for a topic no subscriber is subscribed to, the publish event just gets dropped. The publisher passes the time of publish as part of the published message, so if a subscriber receives that message, it is able to calculate the difference between publish time and receive time for performance testing with different network topologies. The publisher can publish updates indefinitely (with --indefinite argument to driver.py) or can publish only a specific number of events (with --max_event_count COUNT argument to driver.py). If you pass both --indefinite and --max_event_count COUNT to the driver, indefinite takes priority. 


## Setup 
The setup directory contains bash scripts that provide an abstraction layer above the driver.py script for use with automation. If you want to spin up a set of publishers on a host, use ./setup_publishers.sh with arguments outlined in the script, and alternatively if you want to spin up a set of subscribers (or just one) on a host, use ./setup_subscribers.sh with arguments outlined in the script. 

## Unit Testing

## Performance Testing 