# A Python Framework for Creating Publish/Subscribe Distributed Systems built on top of [ZeroMQ, an asynchronous messaging library](https://zeromq.org/)

This project offers a framework for spinning up a publish/subscribe system either on a single host or on a virtualized network with a tool like [Mininet](http://mininet.org/). It offers two main models of message dissemination, namely:
1. centralized dissemination, where a central broker "subscribes" to publishers and forwards published messages appropriately to subscribers whose subscriptions match the published messages. This model offers anonymization (de-coupling) between publishers and subscribers, where each only needs to know about the broker and not about each other. This method also means the broker increasingly becomes a bottleneck as the number of publishers/subscribers grows.
2. Decentralized dissemination, where a central broker simply registers publishers and subscribers, and notifies subscribers about the IP addresses of the publishers that publish topics they are interested in so the subscribers can connect to those publishers directly. With this model, there is coupling between the publishers and subscribers in that the subscribers have to know the publisher IP addresses and listen to them directly, but there is also no need to funnel all published messages through the broker.

The project also offers integrated performance / latency analysis by allowing you to configure subscribers to write out latency data (between publishers and subscribers) to a provided filename, which provide insight about how long it takes for messages with specific topics from specific publishers to reach the subscriber (this is done by including the publish time in the message that gets sent).


## Architecture

The following sections provide an overview of the basic architecture of this project, outlining the core entities that interact to form a fully-functional Publish-Subscribe distributed system with optional broker-based anonyomization between publishers and subscribers.


### The Subscriber [(src/lib/subscriber.py)](src/lib/subscriber.py)

The following is a high level outline of the Subscriber's under-the-hood mechanics.

#### Constructor
```
subscriber = Subscriber(
    filename=<file to write received messages>,
    broker_address=<IP address of broker (created before subscriber)>,
    topics=<list of topics of interest for subscriber>,
    indefinite=<whether to listen indefinitely, default false>,
    max_event_count=<max number of publish events to receive if not indefinite>,
    centralized=<whether publish subscribe system uses centralized broker dissemination>
)
```
#### Underlying methods
```
def configure(self):
    """ Method to perform initial configuration of Subscriber entity """

def register_sub(self):
    """ Register self with broker """

def parse_publish_event(self, topic=""):
    """ Method to parse a published event for a given topic
    Args: topic (string) - topic this publish event corresponds to
    """

def notify(self):
    """ Method to poll for published events (or notifications about new publishers from broker) either indefinitely (if indefinite=True in constructor) or until max_event_count (passed to constructor) is reached. """

def write_stored_messages(self):
    """ Method to write all stored messages to filename passed to constructor """

def get_host_address(self):
    """ Method to return IP address of current host.
    If using a mininet topology, use netifaces (socket.gethost... fails on mininet hosts)
    Otherwise, local testing without mininet, use localhost 127.0.0.1 """

def disconnect(self):
    """ Method to disconnect from the pub/sub network """

###################################################################################
#######################  CENTRALIZED DISSEMINATION METHODS  #######################
###################################################################################
def setup_broker_topic_port_connections(self, received_message):
    """ Method to set up one socket per topic to listen to the broker
    where each topic is published from a different port on the broker address
    Args: received_message (dict) - message received from broker containing mapping
    between topics published from the broker and ports on which they will be published
    """

###################################################################################
######################  DECENTRALIZED DISSEMINATION METHODS  ######################
###################################################################################
def setup_notification_polling(self):
    """ Method to set up a socket for polling for notifications about
    new publishers from the broker. The notify port is randomly allocated
    by the broker when the subscriber registers and is sent back to be passed
    to this method
    Args:
    - notify_port (int) """

def setup_publisher_direct_connections(self, notification=None):
    """ Method to set up direct connections with publishers
    provided by the broker based on the topic that a subscriber has
    just registered itself with
    Args:
    - notification (list of dicts) new publisher notification from broker in JSON form
    """

def parse_notification(self):
    """ Method to parse notification about new publishers from broker
    IF there are new publishers, setup direct connections. """

```
 The Subscriber class, representing a subscriber in a publish/subscribe distributed system, can be configured differently across multiple variables.
 #### How Long it Listens
 The subscriber can be configured to either listen **indefinitely** or listen until a **specified number of published events** have been received. If configured to listen indefinitely, the subscriber cannot be configured with a ```filename``` to write received message data out, as this write does not happen in real-time to avoid slowing the Subscriber down. It writes data If configured to listen with a ```max_event_count```, you have the option of passing ```filename``` to which the subscriber will write received messages (at the end of the notify() loop) in the following CSV format:
 ```
 <publisher IP Address>,<topic published>,<difference between publish time and receive time>
 ```
 This format, simple as it is, was chosen for the purpose of performance testing, where we are interested in latencies between specific Pub/Sub pairs across various virtualized network topologies, where these performance tests are run using [Mininet](http://mininet.org/walkthrough/), seen (for example) in [src/performance_tests/centralized.py](src/performance_tests/centralized.py).

 Side note: when the subscriber disconnects (when subscriber.disconnect() is called), it disconnects from the pub/sub system **cleanly** by first notifying the broker that it is leaving so that the broker can remove the relevant data associated with the exiting subscriber. Otherwise, it's easy to reach a situation in which the broker tries sending a message to a subscriber that no longer exists, and the broker hangs, which can hang the full system.

#### The Broker Address
The subscriber **must** be provided the IP address of the broker, which also means that the **broker must be created before the subscriber**. This tells the subscriber either 1) where to listen for notifications about new publisher IP addresses (in the case of decentralized dissemination), or 2) where to listen for publish events (in the case of centralized broker dissemination)

#### The Topics
Of course, you can't have a real subscriber if they aren't subscribing to anything. The topics provided to the constructor tell the subscriber what to subscribe to, and this list of topics (which could just be one topic) are sent to the broker during registration (via ```register_sub```) so that the broker can either 1) tell the subscriber about the addresses of new publishers of that topic when they join (for *decentralized dissemination*) or 2) forward published events that match those topic subscriptions from publishers to the subscriber when they are published (for *centralized dissemination*)

#### Centralized or Not
This is perhaps the most important configuration parameter, as it governs the path of a lot of the internal logic of the subscriber. The same is true for the publisher and the broker, as well. In short, if centralized is set to **True** in the constructor, the subscriber listens **only to the broker** for published events; basically, in this case, the broker is the one publisher in the distributed system. With **centralized dissemination**, of course, there is the issue of the broker representing a bottleneck in the system, which means it is more likely that the latency between the original publisher and the subscriber will be greater, so you have the option of setting centralized to **False**. If you do this, the subscriber will **1)** listen for notification events from the broker about IP addresses of newly joined publishers that are publishing a topic of interest, AND **2)** listen directly to the publishers (about which the subscriber was notified) for publish events. This **decentralized dissemination method** lessens the load on the Broker and decreases latency between the original publisher(s) and the subscriber, since the connection is direct.


### The Publisher [(src/lib/publisher.py)](src/lib/publisher.py)
The following is a high level outline of the Publisher's under-the-hood mechanics.
#### Constructor
```
publisher = Publisher(
    broker_address=<IP address of broker (created before publisher)>,
    topics=<list of topics to publish>,
    indefinite=<whether to publish events indefinitely, default false>,
    max_event_count=<max number of events to publish if not indefinite>,
    sleep_period=<interval in seconds between each publish event, default 1>,
    bind_port=<port on which to bind the event publishing socket>
)
```

#### Underlying Methods
```
def configure(self):
    """ Method to perform initial configuration of Publisher """

def setup_port_binding(self):
    """
    Method to bind socket to network address to begin publishing/accepting client connections
    using bind_port specified. If bind_port already in use, increment and keep trying until success. Final bind port for event publishing will be >= initial bind_port constructor argument.
    """

def register_pub(self):
    """ Method to register this publisher with the broker """

def get_host_address(self):
    """ Method to return IP address of current host.
    If using a mininet topology, use netifaces (socket.gethost... fails on mininet hosts)
    Otherwise, local testing without mininet, use localhost 127.0.0.1 """

def generate_publish_event(self, iteration=0):
    """ Method to generate a publish event
    Args:
    - iteration (int) - current publish event iteration for this publisher,
    used to determine topic to publish using iteration % len(topics) """

def publish(self):
    """ Method to publish events either indefinitely or until a max event count
    is reached """

def disconnect(self):
    """ Method to disconnect from the pub/sub network """
```
The publisher is the most **indifferent** entity in the Pub/Sub system. Notice that you do not need to tell the publisher if the Pub/Sub system is centralized or not, because the internal logic of the publisher functions the same either way. Whether the system is using a **centralized dissemination model** (broker receives/forwards all events to subscribers) or a **decentralized dissemination model** (broker notifies subscribers about publisher addresses for direct connections), the publisher simply 1) registers with the broker, and 2) publishes events. From there, it's up to the **subscribers** and the **broker** to determine who to connect to for what content depending on a **centralized=True/False** parameter. The publisher passes the time of publish as part of the published message so that if a subscriber receives that message, it is able to calculate the difference between publish time and receive time for performance testing with different network topologies.

Like the Subscriber, the Publisher is configurable along a number of variables, outlined below.

#### How Long to Publish
Similar to the Subscriber, the Publisher accepts an ```indefinite``` argument indicating whether to publish events indefinitely, and if False, how many events to publish specifically. An interesting thing to note here is that if not publishing indefinitely, then a Publisher's maximum event count is the hard limiting factor for a pairing between that Publisher and a subscriber to one of that Publisher's topics. If a Publisher is publishing topic A events and a Subscriber is receiving those events, then even if Subscriber A is supposed to have a max event count of 50, it will not reach that event count if the Publisher's max event count is 10. Granted, that is assuming the Publisher in this case is the only publisher of topic A. You can't consume what isn't being produced. On the flip side, if you configure a Publisher to publish indefinitely, and a Subscriber to subscribe for a max event count, (and we assume this is the only pub/sub pair), then all published events after that max event count will simply be **dropped**, as no one will be left consuming those published events. **Generally speaking, a published event with no subscriber will always be dropped.** In this case, "Subscriber" could refer to a Subscriber object or a Broker object "subscribing" to that publisher with a max event count to forward a specific number of events to interested subscribers.

#### The Broker Address
The publisher must be provided the IP address of the broker, which also means that the broker must be created before the publisher. This tells the publisher who to register with, where a publisher must be registered in order for a subscriber to consume what they are publishing, regardless of the dissemination method.

#### The Topics
A publisher must have something to publish. The topics provided to the constructor tell the publisher what to publish, and this list of topics (which could just be one topic) are sent to the broker during registration (via ```register_pub```) so that the broker can either 1) notify all subscribers subscribed to this publisher's topic(s) about the IP address of this new publisher (for **decentralized dissemination**), or 2) forward events published by this publisher to subscribers that are subscribed to the respective topics (for **centralized dissemination**)

#### Sleep Interval
The sleep interval (or ```sleep_period```) defines how long to wait (in seconds) between each publish event. The default is 1 second. Note: this sleep period does not affect the time it takes for a published event to reach a subscriber.


### The Broker [(src/lib/broker.py)](src/lib/broker.py)
The following is a high level outline of the Broker's under-the-hood mechanics. It should be noticeable (when comparing the outline to those of the Subscriber and Publisher) that the Broker holds the most responsibility and internal logic in a Pub/Sub distributed system.
#### Constructor
```
publisher = Broker(
    centralized=<whether publish subscribe system uses centralized broker dissemination>
    indefinite=<whether to listen for events indefinitely, default false>,
    max_event_count=<max number of events to publish if not indefinite>,
)
```

#### Underlying Methods
```
def configure(self):
    """ Method to perform initial configuration of Broker entity """

def parse_events(self, index):
    """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
    Parse events returned by ZMQ poller and handle accordingly
    Args:
    - index (int) - event index, just used for logging current event loop index
        """
def event_loop(self):
    """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
    Poll for events either indefinitely or until a specific
    event count (self.max_event_count, passed in constructor) is reached """

def update_receive_socket(self):
    """ CENTRALIZED DISSEMINATION
    Once publisher registers with broker, broker will begin receiving messages from it
    for a given topic; broker must open a SUB socket for the topic if not already opened"""

def send(self, topic):
    """ CENTRALIZED DISSEMINATION
    Take a received message for a given topic and forward
    that message to the appropriate set of subscribers using
    send_socket_dict[topic] """

def get_clear_port(self):
    """ Method to get a clear port that has not been allocated """

def disconnect_sub(self, msg):
    """ Method to remove data related to a disconnecting subscriber """

def register_sub(self):
    """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
    Register a subscriber address as interested in a set of topics """

def notify_subscribers(self, topics, pub_address=None, sub_id=None):
    """ DECENTRALIZED DISSEMINATION
    Tell the subscribers of a given topic that a new publisher
    of this topic has been added; they should start listening to
    that/those publishers directly """

def disconnect_pub(self, msg):
    """ Method to remove data related to a disconnecting publisher """

def register_pub(self):
    """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
    Register (or disconnect) a publisher as a publisher of a given topic,
    e.g. 1.2.3.4 registering as publisher of topic "XYZ" """

def get_host_address(self):
    """ Method to return IP address of current host.
    If using a mininet topology, use netifaces (socket.gethost... fails on mininet hosts)
    Otherwise, local testing without mininet, use localhost 127.0.0.1 """

def update_send_socket(self):
    """ CENTRALIZED DISSEMINATION
    Once a subscriber registers with the broker, the broker must
    create a socket to publish the topic; the broker will let the
    subscriber know the port """

def disconnect(self):
    """ Method to disconnect from the publish/subscribe system by destroying the ZMQ context """
```

The Broker class holds a number of responsibilities, which vary depending on the dissemination model, namely:
* With Both Dissemination Models
  * Registering subscribers and mapping them to topics they are interested in
  * Registering publishers and mapping them to topics they publish
  * Handling clean and reliable ad-hoc connections and disconnections for both publishers and subscribers
* With the Decentralized Dissemination Model
  * Upon subscriber registration, notifying that subscriber about the IP addresses of all publishers that publish topics that subscriber is interested in
  * Upon publisher registration, notifying the subscribers that are subscribed to that publisher's topics about the IP address of that new publisher
* With the Centralized Dissemination Model
  * Listening for and receiving published events from newly registered publishers as a "subscriber"
  * Forwarding those received events to subscribers who are registered as interested in the event topics, if any. If none, then drop events.
  * Upon publisher registration (with new topics being published), setting up a local (on the Broker) publishing/forwarding mechanism for each of that publisher's topics and subsequently notifying that topic's subscribers (if any) about it so they can listen to the Broker for forwarded messages with that topic

To align with these responsibilities, the Broker class is also configurable along a couple of key variables:

#### Centralized or Not
First and foremost, the broker must know whether the Pub/Sub system uses a centralized (centralized=True) or decentralized (centralized=False) dissemination model. This argument significantly governs the path of internal logic when processing received messages from the publisher(s), as can be seen in the above outline.

#### How Long to Listen/Send
The Broker, similar to the Subscriber and Publisher, can be configured to process events (where "process" could mean different things depending on the dissemination model) either indefinitely or, if not indefinitely, until a specific event count is reached. Note that with the centralized dissemination model, if the Broker is configured to process N events, then any registered Subscriber will receive at most N events even if configured to listen indefinitely.

### The Driver [(src/driver.py)](src/driver.py)
The driver script was created to facilitate a modular implementation of performance testing by "driving" the creation, configuration, connection, execution, and disconnection of the different pub/sub entities based on arguments passed into it. Rather than creating one single script to perform a complete test, the reusable driver script allows you to spin up (and down) and configure Pub/Sub entities on a host (or different hosts) by simply passing some key arguments to the driver. Below is an outline of the arguments accepted by the driver.
* `-v | --verbose` : verbose logging; pass this to enable debugging
* `-pub | --publisher [COUNT]` : create COUNT publishers on this host (NOTE: current limit on COUNT is 1 as we have not implemented multiprocessing on the driver)
* `-sub | --subscriber [COUNT]` : create COUNT subscribers on this host (NOTE: current limit on COUNT is 1 as we have not implemented multiprocessing on the driver)
* `--broker [COUNT]` : create COUNT brokers on this host; NOTE: limit is 1, as the architecture does not support a multi-broker Pub/Sub system
* `-f | --filename <FILE NAME/FULL PATH>` : Only for use with --subscriber if **NOT** `--indefinite`
* `-c | --centralized` : Whether to use centralized dissemination model; required with `--subscriber` and `--broker ` but not with `--publisher` because publisher is purely indifferent to dissemination method. To a publisher, everyone is a subscriber.
* Required with `--publisher` and `--subscriber`
    * `-t | --topic <TOPIC>` : if creating a publisher or subscriber, provide a topic to either publish or subscribe to. If you want multiple for either, use `-t T1 -t T2 [-t T3...]`
    * `-b | --broker_address` : must provide the IP address of the broker (regardless of dissemination method)
* `-i | --indefinite` : If passed with `--publisher`, publish events indefinitely; If passed with `--subscriber`, listen for published events indefinitely; If passed with `--broker`, process events indefinitely
* `-m | --max_event_count` : Only applies if `--indefinite` **NOT** passed; maximum number of events to process (publish/listen for/forward).
* Required with `--publisher`:
    * `-bp | --bind_port` : port from which to publish events. When passing around the publisher address in messages for identifying publishers, the bind port is included in the address (IP:bind_port) in case multiple publishers are running on the same host.
    * `-s | --sleep` : Number of seconds to sleep between publish events

A couple of key things to note with the driver:
* You cannot pass a mix of `--subscriber`, `--publisher`, and `--broker`; you can only create one type of entity per driver.py call
* You cannot currently pass a COUNT greater than 1 to `--subscriber COUNT`, `--publisher COUNT`, or `--broker COUNT` because multiprocessing on the driver side has not been implemented. With the current structure of the driver, for example, if you want to create 3 Subscribers (`--subscriber 3`) with one call, the driver would enter a 3-iteration loop, where each iteration 1) creates a subscriber, 2) configures the subscriber, and 3) starts the either *indefinite* or *N max events* `notify()` loop for the subscriber. This `notify()` method of the subscriber is blocking, which prevents the 2nd and 3rd subscribers from being created until the first disconnects. Multiprocessing would allow the notify() call to be non-blocking and thus would allow the creation/execution of multiple subscribers per driver.py call. This same problem exists with the Publisher's `publish()` method. For the Broker, the limit is 1 because the general project architecture does not currently support a multiple-broker Publish/Subscribe system.
        * HOWEVER, you can still create multiple Publishers (or multiple Subscribers) per host, just using separated driver calls with `--publisher 1` or `--subscriber 1`

## Unit Testing [(src/unit_tests)](src/unit_tests/README.md)

The Unit Testing module (which uses [unittest](https://docs.python.org/3/library/unittest.html)) is designed to test those methods of the Publisher, Subscriber, and Broker classes whose functionality can be tested independently of a full Publish/Subscribe system. This module does not test things like registration, message sending, and message receiving, since those methods depend on the Publish/Subscribe system as a whole. This module tests some foundational basic units responsible for things like randomized port selection, host IP address determination, publish event generation, and file writing.

## Performance Testing

The Performance Testing module uses [The Python API for Mininet](https://github.com/mininet/mininet/wiki/Introduction-to-Mininet) in combination with [The Driver](src/driver.py) to automatically spin up a series of different virtualized network topologies through Mininet (all with Python) and embed a unique Publish/Subscribe system into each of those topologies for the purpose of collecting file-written performance data (via the `--filename` argument to the Subscriber) to understand how Publish/Subscribe latency is impacted by things like:
* Number of hosts (# pubs, # subs) in the system
* Dissemination model (centralized vs decentralized)
* Topology type (e.g. single switch vs. tree topology)
as well as how the above variables relate in regard to latency.

The Performance Testing module is split into two main classes:
* [(CentralizedPerformanceTest)](src/performance_tests/centralized.py) - for testing performance of centralized dissemination pub/sub systems along various network topologies and various counts of publishers and subcribers
* [(DecentralizedPerformanceTest)](src/performance_tests/decentralized.py) - for testing performance of decentralized dissemination pub/sub systems along various network topologies and various counts of publishers and subcribers

Both of the above classes can be configured along:
* The number of events to collect data for (sample size) within a given pub sub system
* The event interval (the argument to `--sleep` when creating a publisher indicating the number of seconds to sleep between publish events)
* The **wait factor**. When the virtualized network is spun up and the Pub/Sub system begins executing in the background, we need to wait some amount of time for that Pub/Sub system to generate and write the data we are interested in. Specifically, the subscriber is responsible for writing the performance data to a file, and it only writes this data once the event count (item 1 in this list) is reached. This wait time for a given test is calculated as:
##### wait factor calculation
`wait_time = self.wait_factor * (self.num_events * self.event_interval)`
where `self` refers to an instance of one of the above Classes.
    * Since the amount of time we need to wait for each subscriber in the system to receive and write all of their expected events certainly needs to scale with the number of hosts in the system, we added a **setWaitFactor(factor)** method to the testing classes allowing for the wait factor to be updated with respect to a changing number of hosts. For example:
    ```
    # Min = 4 hosts, Max = 256 hosts
    for depth in range(2,5):
        for fanout in range(2,5):
            # Adjust the wait factor as number of hosts grows.
            centralized_perf_test.setWaitFactor(factor=depth*fanout)

            # Test centralized pub sub with this tree topology
            centralized_perf_test.test_tree_topology(depth=depth, fanout=fanout)
    ```

For each virtualized network, every subscriber in the respective embedded Publish/Subscribe system writes out its performance data into a file like:

**src/performance_tests/data/[centralized,decentralized]/<network name, e.g. "**tree-d3f2-8hosts**" for a tree topology with depth=3, fanout=2>/subscriber-<index, i.e. which subscriber this is for the current system>**

From there, we are able to extract the written data and generate plots to visualize the patterns that exist within it.

## Patterns Found
There are several interesting observations can be made from the tests that have been run within the Performance Testing Framework:
1. The latency with direct dissemination is in general smaller than that with centralized dissemination.
2. The distribution of latency for direct dissemination is more uniformly distributed without outliers, while there are extra large latencies with centralized dissemination
3. With an increased number of publishers and subscribers, the latency increases for both direct and centralized dissemination.
4. The impact of increased number of publishers and subscribers on centralized dissemination is larger than that on direct dissemination.
5. The relationship between the increase of latency and the increase of publishers/subscribers are non-linear. It seems to be a quadratic relationship. More testing is required to confirm this.