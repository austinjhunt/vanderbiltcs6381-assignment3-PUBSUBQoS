"""
Message Broker to serve as anonymizing middleware between
publishers and subscribers
"""
from .zookeeper_client import ZookeeperClient
import zmq
import json
import random
import logging
import pickle
import netifaces
import sys
import time

class Broker(ZookeeperClient):
    def __init__(self, centralized=False, indefinite=False, max_event_count=15,
        zookeeper_hosts=['127.0.0.1:2181'], pub_reg_port=5555, sub_reg_port=5556, autokill=None,
        verbose=False, zone=1, primary=False ):
        self.zone = zone
        self.primary = primary # alternative is backup
        self.verbose = verbose
        self.centralized = centralized
        self.prefix = {'prefix': f'BROKER({id(self)}'}
        self.set_logger()
        self.autokill_time = None
        if autokill:
            self.autokill_time = time.time() + autokill
        # Either poll for events indefinitely or for specified max_event_count
        self.indefinite = indefinite
        self.max_event_count = max_event_count

        # publishers = {
        #   'A': [{
        #           'address': 127.0.0.1:5555,
        #           'offered': int (sliding window/history size),
        #           'id': str
        #       }]
        # }

        # subscribers = {
        #   'A': [{
        #           'address': 127.0.0.1:5555,
        #           'requested': int (sliding window/history size),
        #           'id': str
        #       }]
        # }

        self.subscribers = {}
        self.publishers = {}
        #  The zmq context
        self.context = None
        # we will use the poller to poll for incoming data
        self.poller = None
        # these are the sockets we open one for each registration
        self.pub_reg_socket = None
        self.sub_reg_socket = None

        # Socket to notify subscribers about publishers of topics
        # Used for decentralized dissemination
        # self.notify_sub_socket = None

        # To fix competitive poll()s among subscribers (one's subscriber's poll() steals another's)
        # use a separate socket per subscriber.
        self.notify_sub_sockets = {}

        # this is the centralized dissemination system
        # broker will have a list of sockets for receiving from publisher
        # broker will also have a list of sockets for sending to subscrbier
        self.receive_socket_dict = {}
        self.send_socket_dict = {}
        self.send_port_dict = {}
        self.used_ports = []
        self.zone = zone

        # Initialize configuration for ZooKeeper client
        super().__init__(zookeeper_hosts=zookeeper_hosts, verbose=verbose)

        # this is for write into the znode about the broker information
        self.pub_reg_port = pub_reg_port
        self.sub_reg_port = sub_reg_port

        self.info(f"Successfully initialized broker object (BROKER{id(self)})")

    def get_all_publisher_addresses(self):
        """ Extract a flattened list of publisher addresses from the
        {topic: [pub addr list] } structure of self.publishers"""
        pubs = []
        for pub_addr_list in self.publishers.values():
            for p in pub_addr_list:
                pubs.append(p['address'])
        return pubs

    def get_all_publisher_ids(self):
        """ Extract a flattened list of publisher ids from the
        {topic: [pub addr list] } structure of self.publishers"""
        pubs = []
        for pub_addr_list in self.publishers.values():
            for p in pub_addr_list:
                pubs.append(p['id'])
        return pubs

    def get_all_subscriber_addresses(self):
        """ Extract a flattened list of subscriber addresses from the
        {topic: [sub addr list] } structure of self.subscribers"""
        subs = []
        for sub_addr_list in self.subscribers.values():
            for s in sub_addr_list:
                subs.append(s['address'])
        return subs

    def get_all_subscriber_ids(self):
        """ Extract a flattened list of subscriber ids from the
        {topic: [sub addr list] } structure of self.subscribers"""
        subs = []
        for sub_addr_list in self.subscribers.values():
            for s in sub_addr_list:
                subs.append(s['id'])
        return subs

    def remove_subscriber(self, sub_id=None, topic=None):
        """ Remove subscriber from internal matchmaking data """
        if not topic:
            for sub_list in self.subscribers.values():
                for sub in sub_list:
                    if sub['id'] == sub_id:
                        self.debug(f'Removing subscriber {sub_id}')
                        sub_list.remove(sub)
        else:
            for sub in self.subscribers[topic]:
                if sub['id'] == sub_id:
                    self.debug(f'Removing subscriber {sub_id}')
                    self.subscribers[topic].remove(sub)

    def remove_publisher(self, pub_id=None, topic=None):
        """ Remove publisher from internal matchmaking data """
        if not topic:
            for pub_list in self.publishers.values():
                for pub in pub_list:
                    if pub['id'] == pub_id:
                        self.debug(f'Removing publisher {pub_id}')
                        pub_list.remove(pub)
        else:
            for pub in self.publishers[topic]:
                if pub['id'] == pub_id:
                    self.debug(f'Removing publisher {pub_id}')
                    self.publishers[topic].remove(pub)

    def setup_load_balancing_znode(self):
        self.debug("initializing /primaries/ znode if not exists")
        if not self.znode_exists(znode_name="/primaries"):
            self.create_znode(znode_name="/primaries", znode_value="container of all primary brokers")

    def setup_shared_state_znode(self):
        """ Set up a znode for sharing state among brokers """
        self.debug("Setting up shared state znode for multiple primary brokers")
        if not self.znode_exists(znode_name="/shared_state/"):
            self.create_znode(
                znode_name="/shared_state/",
                znode_value="container of shared state among primary brokers"
            )
        if not self.znode_exists(znode_name="/shared_state/publishers"):
            self.create_znode(
                znode_name="/shared_state/publishers/",
                znode_value="shared state container of all active publishers"
            )
        if not self.znode_exists(znode_name="/shared_state/subscribers"):
            self.create_znode(
                znode_name="/shared_state/subscribers/",
                znode_value="shared state container of all active subscribers"
            )

    def watch_shared_state_publishers(self):
        @self.zk.ChildrenWatch('/shared_state/publishers/')
        def changed_publishers(children):
            self.debug('Publishers shared state changed!')
            # Each znode name under publishers is an address of a publisher
            # znode value is JSON/publisher info
            current_internally_stored_pubs = self.get_all_publisher_ids()
            for pub_id in children:
                if pub_id not in current_internally_stored_pubs:
                    self.debug(f'New publisher {pub_id}')
                    publisher_info = self.get_znode_value(znode_name=f'/shared_state/publishers/{pub_id}')
                    self.debug(f'New pub info: {publisher_info}')
                    publisher_info = json.loads(publisher_info)
                    topics = publisher_info['topics']
                    offered = publisher_info['offered']
                    pub_addr = publisher_info['address']
                    for topic in topics:
                        pub_data = {
                            'address': pub_addr,
                            'offered': int(offered),
                            'id': pub_id
                        }
                        self.debug(f'Adding publisher to publishers[{topic}]')
                        if topic in self.publishers:
                            self.publishers[topic].append(pub_data)
                        else:
                            self.publishers[topic] = [pub_data]
                    # Finally, notify existing subscribers about new publisher!
                    self.notify_subscribers(topics=topics, pub_address=pub_addr)
            # Also handle if change was a subscriber leaving
            # (trim internal subscribers to match zookeeper)
            for pub_id in current_internally_stored_pubs:
                if pub_id not in children:
                    self.debug(f'Removing a publisher: {pub_id}')
                    self.remove_publisher(pub_id=pub_id)

    def watch_shared_state_subscribers(self):
        """ Watch for new / """
        @self.zk.ChildrenWatch('/shared_state/subscribers/')
        def changed_subscribers(children):
            self.info('Subscribers shared state changed!')
            # Each znode name under subscribers is an address of a subscriber
            # The znode value is json: {topics: <list>, requested: int } -> requested is the requested sliding window/history
            current_internally_stored_subs = self.get_all_subscriber_ids()
            for sub_id in children:
                if sub_id not in current_internally_stored_subs:
                    self.debug(f'New subscriber! {sub_id}')
                    subscriber_info = self.get_znode_value(znode_name=f'/shared_state/subscribers/{sub_id}')
                    self.debug(f'New sub info: {subscriber_info}')
                    subscriber_info = json.loads(subscriber_info)
                    topics = subscriber_info['topics']
                    requested = subscriber_info['requested']
                    sub_addr = subscriber_info['address']
                    for topic in topics:
                        sub_data = {
                            'address': sub_addr,
                            'requested': int(requested),
                            'id': sub_id
                        }
                        self.debug(f'Adding sub to subscribers[{topic}]')
                        if topic in self.subscribers:
                            self.subscribers[topic].append(sub_data)
                        else:
                            self.subscribers[topic] = [sub_data]
            # Also handle if change was a subscriber leaving (trim internal subscribers to match zookeeper)
            for sub_id in current_internally_stored_subs:
                if sub_id not in children:
                    self.remove_subscriber(sub_id=sub_id)

    def setup_fault_tolerance_znode(self):
        # Set election path with zone. Backup assigned to same zone will contend via election for
        # fault tolerance. Pub/sub requests will be load balanced across zones using zookeeper.
        self.election_path = f"/elections/zone_{self.zone}"
        # This will take the place of the original /broker znode from Assignment 2. Each zone will have primary broker whose info is stored in this znode.
        self.broker_leader_znode = f"/primaries/zone_{self.zone}"

    def set_logger(self, prefix=None):
        if not prefix:
            self.prefix = {'prefix': f'BROKER<zone_{self.zone}>'}
        else:
            self.prefix = {'prefix': prefix}
        self.logger = logging.getLogger(f'BROKER{id(self)}')
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        for h in self.logger.handlers:
            self.logger.removeHandler(h)
        self.logger.addHandler(handler)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def leader_function(self):
        self.set_logger(prefix=f'BROKER<zone_{self.zone},primary>')
        self.info(f"I ({self.zk_instance_id}) AM LEADER OF {self.election_path}")
        self.debug(f"Storing my information in {self.broker_leader_znode}")
        self.info("Configuring myself!")
        self.configure()
        if self.zk.exists(self.broker_leader_znode):
            self.debug(f"{self.broker_leader_znode} znode exists : only modify the value)")
            self.modify_znode_value(znode_name=self.broker_leader_znode, znode_value=self.broker_info.encode('utf-8'))
        else:
            self.debug(f"{self.broker_leader_znode} znode does not exist: creating!")
            self.create_znode(znode_name=self.broker_leader_znode,znode_value=self.broker_info)
        self.debug('Updating current system load znode')
        self.update_current_system_load_znode()
        try:
            self.event_loop()
            # Reached if not indefinite
            self.disconnect()
        except KeyboardInterrupt:
            # If you interrupt/cancel a broker, be sure to disconnect/clean all sockets
            self.disconnect()

    def zk_run_election(self):
        """ Run election for a given zone. Each zone has a primary broker replica. """
        self.set_logger(prefix=f'BROKER<zone_{self.zone},backup>')
        self.info(f"I am currently a contender/backup for zone_{self.zone}")
        self.election = self.zk.Election(f"{self.election_path}", self.zk_instance_id)
        self.debug(f"contenders for {self.election_path}: {self.election.contenders()}")
        # Blocks until election is won, then calls leader function
        self.election.run(self.leader_function)

    def configure(self):
        """ Method to perform initial configuration of Broker entity """
        self.debug("Configure Start")
        #  The zmq context
        self.context = zmq.Context()
        # we will use the poller to poll for incoming data
        self.poller = zmq.Poller()
        # these are the sockets we open one for each registration
        self.debug("Opening two REP sockets for publisher registration and subscriber registration")
        self.pub_reg_socket = self.context.socket(zmq.REP)
        self.setup_pub_port_reg_binding()
        self.sub_reg_socket = self.context.socket(zmq.REP)
        self.setup_sub_port_reg_binding()
        self.used_ports.append(self.pub_reg_port)
        self.used_ports.append(self.sub_reg_port)
        self.broker_info = f"{self.get_host_address()},{self.pub_reg_port},{self.sub_reg_port}"
        self.debug(f"Enabling publisher registration on port {self.pub_reg_port}")
        self.debug(f"Enabling subscriber registration on port {self.sub_reg_port}")
        # register these sockets for incoming data
        self.debug("Register sockets with a ZMQ poller")
        self.poller.register(self.pub_reg_socket, zmq.POLLIN)
        self.poller.register(self.sub_reg_socket, zmq.POLLIN)
        self.debug("Configure Stop")

    def setup_pub_port_reg_binding(self):
        """
        Method to bind socket to network address to begin publishing/accepting client connections
        using bind_port specified. If bind_port already in use, increment and keep trying until success.
        """
        success = False
        while not success:
            try:
                self.debug(f'Attempting bind to port {self.pub_reg_port}')
                self.pub_reg_socket.bind(f'tcp://*:{self.pub_reg_port}')
                success = True
                self.debug(f'Successful bind to port {self.pub_reg_port}')
            except:
                self.error(f"Exception when binding to port {self.pub_reg_port}")
                try:
                    self.error(f'Port {self.pub_reg_port} already in use, attempting next port')
                    success = False
                    self.pub_reg_port += 1
                except Exception as e:
                    self.debug(e)

    def setup_sub_port_reg_binding(self):
        """
        Method to bind socket to network address to begin publishing/accepting client connections
        using bind_port specified. If bind_port already in use, increment and keep trying until success.
        """
        success = False
        while not success:
            try:
                self.debug(f'Attempting bind to port {self.sub_reg_port}')
                self.sub_reg_socket.bind(f'tcp://*:{self.sub_reg_port}')
                success = True
                self.debug(f'Successful bind to port {self.sub_reg_port}')
            except:
                try:
                    self.error(f'Port {self.sub_reg_port} already in use, attempting next port')
                    success = False
                    self.sub_reg_port += 1
                except Exception as e:
                    self.debug(e)

    def parse_events(self, index):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Parse events returned by ZMQ poller and handle accordingly
        Find which socket was enabled and accordingly make a callback
        Note that we must check for all the sockets since multiple of them
        could have been enabled.
        Args:
        - index (int) - event index, just used for logging current event loop index
         """
        try:
            # Don't block indefinitely; wait max of .5 second
            events = dict(self.poller.poll(500))
        except zmq.error.ZMQError as e:
            if 'Socket operation on non-socket' in str(e):
                self.error(f'Exception with self.poller.poll(): {e}')
                self.disconnect()
        if self.pub_reg_socket in events:
            self.register_pub()
            if self.centralized:
                self.update_receive_socket()
        elif self.sub_reg_socket in events:
            self.debug(f"Event {index}: subscriber")
            self.register_sub()
        # For centralized dissemination, also handle sending
        if self.centralized:
            for topic in self.receive_socket_dict.keys():
                if self.receive_socket_dict[topic] in events:
                    self.send(topic)

    def event_loop(self):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Poll for events either indefinitely or until a specific
        event count (self.max_event_count, passed in constructor) is reached """
        self.debug("Start Event Loop")
        if self.indefinite:
            self.debug("Begin indefinite event poll loop")
            i = 0
            if self.autokill_time:
                while time.time() < self.autokill_time:
                    i += 1
                    self.parse_events(i)
                self.info("Autokilling broker")
            else:
                while True:
                    i += 1
                    self.parse_events(i)
        else:
            self.debug(f"Begin finite (max={self.max_event_count}) event poll loop")
            event_count = 0
            if self.autokill_time:
                while event_count < self.max_event_count and time.time() < self.autokill_time:
                    self.parse_events(event_count+1)
                    event_count += 1
                if time.time() >= self.autokill_time:
                    self.info("Autokilling broker")
            else:
                while event_count < self.max_event_count:
                    self.parse_events(event_count+1)
                    event_count += 1

    def update_receive_socket(self):
        """ CENTRALIZED DISSEMINATION
        Once publisher registers with broker, broker will begin receiving messages from it
        for a given topic; broker must open a SUB socket for the topic if not already opened"""
        self.debug("Updating receive socket to 'subscribe' to publisher")
        for topic in self.publishers.keys():
            if topic not in self.receive_socket_dict.keys():
                self.receive_socket_dict[topic] = self.context.socket(zmq.SUB)
                self.poller.register(self.receive_socket_dict[topic], zmq.POLLIN)
            for pub in self.publishers[topic]:
                address = pub['address']
                self.debug(f"'Subscribing' to publisher {address}")
                self.receive_socket_dict[topic].connect(f"tcp://{address}")
                self.receive_socket_dict[topic].setsockopt_string(zmq.SUBSCRIBE, topic)
        # self.debug("Broker Receive Socket: {0:s}".format(str(list(self.receive_socket_dict.keys()))))

    def send(self, topic):
        """ CENTRALIZED DISSEMINATION
        Take a received message for a given topic and forward
        that message to the appropriate set of subscribers using
        send_socket_dict[topic] """
        if topic in self.send_socket_dict.keys():
            # received_message = self.receive_socket_dict[topic].recv_string()
            [topic,received_message] = self.receive_socket_dict[topic].recv_multipart()
            unpickled_message = pickle.loads(received_message)
            self.debug(f"Forwarding Msg: <{unpickled_message}>")
            # Beyond this point it is the subscriber's responsibility to check the dominance relationship.
            # Broker must forward all of these without filtering since some subscribers may satisfy and others may not.
            # Publisher should include <offered> value in the message, so subscriber can filter before processing.
            self.send_socket_dict[topic.decode('utf8')].send_multipart([topic, received_message])

    def get_clear_port(self):
        """ Method to get a clear port that has not been allocated """
        while True:
            port = random.randint(10000, 20000)
            if port not in self.used_ports:
                break
        return port

    def disconnect_sub(self, msg):
        """ Method to remove data related to a disconnecting subscriber """
        self.debug(f"Disconnecting subscriber...")
        dc = msg['disconnect']
        topics = dc['topics']
        address = dc['address']
        sub_id = dc['id']
        sub_znode = f'/shared_state/subscribers/{sub_id}'
        try:
            self.delete_znode(znode_name=sub_znode)
            self.update_current_system_load_znode()
        except Exception as e:
            self.error(f'Exception when deleting pub znode {sub_znode}: {str(e)}')

        if not self.centralized:
            notify_port = dc['notify_port']
            self.used_ports.remove(notify_port)
            # Close the notification socket for this subscriber with id as key
            self.notify_sub_sockets[sub_id].close()
            self.notify_sub_sockets.pop(sub_id)
        for t in topics:
            if t in self.subscribers:
                # if only subscriber to topic, remove topic altogether
                if len(self.subscribers[t]) == 1:
                    self.subscribers.pop(t)
                    if self.centralized:
                        # Close socket then remove. No other subscribers active for t.
                        self.send_socket_dict[t].close()
                        self.send_socket_dict.pop(t)
                else:
                    # Remove just this subscriber
                    self.remove_subscriber(sub_id=sub_id,topic=t)
        response = {'disconnect': 'success'}
        return json.dumps(response)

    def register_sub(self):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Register a subscriber address as interested in a set of topics """
        try:
            # the format of the registration string is a json
            # '{ "address":"1234", "topics":['A', 'B']}'
            self.debug("Subscriber Registration Started")
            sub_reg_string = self.sub_reg_socket.recv_string()
            # Get topics and address of subscriber
            sub_reg_dict = json.loads(sub_reg_string)

            # Handle disconnect if requested
            if 'disconnect' in sub_reg_dict:
                response = self.disconnect_sub(msg=sub_reg_dict)
                # send response
                self.sub_reg_socket.send_string(response)
                return

            topics = sub_reg_dict['topics']
            sub_address = sub_reg_dict['address']
            sub_id = sub_reg_dict['id']
            requested = int(sub_reg_dict['requested'])
            sub_data = {
                'address': sub_address,
                'requested': requested,
                'id': sub_id,
                'topics': topics
                }
            self.debug(f'New subscriber info: {sub_data}')
            for topic in topics:
                if topic not in self.subscribers.keys():
                    self.subscribers[topic] = [sub_data]
                else:
                    self.subscribers[topic].append(sub_data)

            if not self.centralized:
                # Allocate a random unique port to notify this subscriber about new hosts.
                # Port must be different for each subscriber since they are each polling
                # and subscribers steal poll pipeline events from each other.
                notify_port = self.get_clear_port()
                msg = {'register_sub': {'notify_port': notify_port}}
                ## Notify new subscriber about all publishers of topic
                ## so they can listen directly
                # Set up a new notify socket on a clear port for this subscriber
                self.debug("Enabling subscriber notification (about publishers)")
                self.notify_sub_sockets[sub_id] = self.context.socket(zmq.REQ)
                self.used_ports.append(notify_port)
                self.notify_sub_sockets[sub_id].bind(f"tcp://*:{notify_port}")
                self.sub_reg_socket.send_string(json.dumps(msg))
                self.notify_subscribers(topics=topics, sub_id=sub_id)
            else:
                ## Make sure there is a socket for each new topic.
                self.update_send_socket()

                ## Publish topic messages to subscribers.
                reply_sub_dict = {}
                for topic in sub_reg_dict['topics']:
                    reply_sub_dict[topic] = self.send_port_dict[topic]
                self.debug(f"Sending topic/ports: {reply_sub_dict}")
                self.sub_reg_socket.send_string(json.dumps(reply_sub_dict, indent=4))

            self.debug("Subscriber registered successfully")
            # Write to zookeeper node in shared state.
            # This notifies the other brokers (all of which watch shared state) about new sub.
            self.create_znode(
                znode_name=f"/shared_state/subscribers/{sub_id}",
                znode_value=json.dumps(sub_data)
            )
            # Also update current system load znode!
            self.update_current_system_load_znode()

        except Exception as e:
            self.error(e)

    def get_pub_id_from_address(self, pub_addr=None):
        pub_id = None
        for pub_list in self.publishers.values():
            for pub in pub_list:
                if pub['address'] == pub_addr:
                    pub_id = pub['id']
                    break
        if not pub_id:
            raise Exception(f"Cannot get pub id from pub_address {pub_addr}; id not stored internally")
        return pub_id

    def dominance_relationship_satisfied(self, pub_id=None, sub_id=None):
        """ Determine if the offered vs. requested dominance relationship is satisfied between a publisher and a subscriber """
        sub_requested = None
        pub_offered = None
        self.debug(f"Checking offered vs. requested relation between sub {sub_id} and pub {pub_id}")
        for pub_list in self.publishers.values():
            for pub in pub_list:
                if pub['id'] == pub_id:
                    pub_offered = int(pub['offered'])
                    break
        self.debug(f"Subscribers: {self.subscribers.values()}")
        for sub_list in self.subscribers.values():
            for sub in sub_list:
                if sub['id'] == sub_id:
                    sub_requested = int(sub['requested'])
        return pub_offered >= sub_requested

    def notify_subscribers(self, topics, pub_address=None, sub_id=None):
        """ DECENTRALIZED DISSEMINATION
        Tell the subscribers of a given topic that a new publisher
        of this topic has been added; they should start listening to
        that/those publishers directly """
        self.debug("Notifying subscribers")
        message = []
        addresses = []
        if pub_address: # when registering single new publisher
            pub_id = self.get_pub_id_from_address(pub_addr=pub_address)
            message = [
                {
                    'register_pub': {
                        'addresses': [pub_address],
                        'topic': topic
                    }
                } for topic in topics
            ]
            message = json.dumps(message)
            # Send to notify socket for each subscriber
            for sub_id, notify_socket in self.notify_sub_sockets.items():
                self.debug('Checking dominance relation between offered/requested')
                if self.dominance_relationship_satisfied(pub_id=pub_id, sub_id=sub_id):
                    self.debug(f"Sending notification to sub: {sub_id}")
                    notify_socket.send_string(message)
                    self.debug(f"Waiting for response...")
                    confirmation = notify_socket.recv_string()
                    self.debug(f"Subscriber notified successfully (confirmation: <{confirmation}>")
                else:
                    self.debug(f"Dominance relationship not satisfied. Not notifying {sub_id} about pub. ")
        else: # registering new subscriber
            for t in topics:
                addresses = []
                if t in self.publishers:
                    for publisher in self.publishers[t]:
                        pub_id = publisher['id']
                        self.debug(f'notify_subscribers::sub_id={sub_id}')
                        if self.dominance_relationship_satisfied(pub_id=pub_id,sub_id=sub_id):
                            addresses.append(publisher['address'])
                        else:
                            self.debug(f'Dominance relationship not satisfied for publisher {pub_id} and subscriber {sub_id}')
                message.append(
                    {
                        'register_pub': {
                            'addresses': addresses,
                            'topic': t
                        }
                    }
                )
            message = json.dumps(message)
            # Send to notify socket for new subscriber address
            self.debug(f"Sending message to subscriber: {message}")
            self.notify_sub_sockets[sub_id].send_string(message)
            self.debug(f"Waiting for response...")
            confirmation = self.notify_sub_sockets[sub_id].recv_string()
            self.debug(f"Subscriber notified successfully (confirmation: <{confirmation}>")

    def disconnect_pub(self, msg):
        """ Method to remove data related to a disconnecting publisher """
        self.debug(f"Disconnecting publisher...")
        dc = msg['disconnect']
        topics = dc['topics']
        address = dc['address']
        pub_id = dc['id']
        pub_znode = f'/shared_state/publishers/{pub_id}'
        try:
            self.delete_znode(znode_name=pub_znode)
            self.update_current_system_load_znode()
        except Exception as e:
            self.error(f'Exception when deleting pub znode {pub_znode}: {str(e)}')
        for t in topics:
            # If this is the only publisher of a topic, remove the topic from
            # self.publishers and from self.receive_socket_dict
            if len(self.publishers[t]) == 1:
                self.publishers.pop(t,None)
                if self.centralized:
                    # Close socket then remove. No other publishers active for t.
                    self.receive_socket_dict[t].close()
                    self.receive_socket_dict.pop(t)
            else:
                # Only remove the single publisher connection from
                # publisher connections for this topic
                self.remove_publisher(pub_id=pub_id, topic=t)
                if self.centralized:
                    self.receive_socket_dict[t].disconnect(address)
        response = {'disconnect': 'success'}
        return json.dumps(response)

    def register_pub(self):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Register (or disconnect) a publisher as a publisher of a given topic,
        e.g. 1.2.3.4 registering as publisher of topic "XYZ" """
        try:
            pub_reg_string = self.pub_reg_socket.recv_string()
            self.debug(f"Publisher Registration Started: {pub_reg_string}")
            pub_reg_dict = json.loads(pub_reg_string)
            if 'disconnect' in pub_reg_dict:
                response = self.disconnect_pub(msg=pub_reg_dict)
                self.pub_reg_socket.send_string(response)
                return
            pub_address = pub_reg_dict['address']
            offered = int(pub_reg_dict['offered'])
            pub_id = pub_reg_dict['id']
            topics = pub_reg_dict['topics']
            pub_data = {
                'address': pub_address,
                'offered': offered,
                'id': pub_id,
                'topics': topics
                }
            for topic in topics:
                if topic not in self.publishers.keys():
                    self.publishers[topic] = [pub_data]
                else:
                    self.publishers[topic].append(pub_data)

            if not self.centralized:
                # For de-centralized dissemination:
                # Subscribers interested in this topic need to know to listen
                # directly to this new publisher.
                # This starts a while loop on the subscriber.
                self.notify_subscribers(pub_reg_dict['topics'], pub_address=pub_address)

            response = {'success': 'registration success'}
            # write to zookeeper node in shared state.
            # This notifies the other brokers (all of which watch shared state) about new pub.
            self.create_znode(
                znode_name=f"/shared_state/publishers/{pub_id}",
                znode_value=json.dumps(pub_data)
            )
            # Also update current system load znode!
            self.update_current_system_load_znode()

        except Exception as e:
            response = {'error': f'registration failed due to exception: {e}'}
        self.debug(f"Sending response: {response}")
        self.pub_reg_socket.send_string(json.dumps(response))
        self.debug("Publisher Registration Succeeded")

    def update_current_system_load_znode(self):
        """ Assumption; the /shared_state/[publishers,subscribers] and
        /primaries children znodes are always updated before this gets called,
        so we can just read the current values and update the /shared_state/current_load znode"""
        num_publishers = len(self.zk.get_children("/shared_state/publishers/"))
        num_subscribers = len(self.zk.get_children("/shared_state/subscribers/"))
        num_clients_total = num_publishers + num_subscribers
        num_zones = len(self.zk.get_children("/primaries/"))
        updated_system_load = num_clients_total / num_zones
        self.modify_znode_value(
            znode_name="/shared_state/current_load",
            znode_value=updated_system_load
        )



    def get_host_address(self):
        """ Method to return IP address of current host.
        If using a mininet topology, use netifaces (socket.gethost... fails on mininet hosts)
        Otherwise, local testing without mininet, use localhost 127.0.0.1 """
        try:
            # Will succeed on mininet. Two interfaces, get second one.
            # Then get AF_INET address family with key = 2
            # Then get first element in that address family (0)
            # Then get addr property of that element.
            address = netifaces.ifaddresses(netifaces.interfaces()[-1])[2][0]['addr']
            address = f'{address}'
        except:
            address = f"127.0.0.1"
        return address

    def update_send_socket(self):
        """ CENTRALIZED DISSEMINATION
        Once a subscriber registers with the broker, the broker must
        create a socket to publish the topic; the broker will let the
        subscriber know the port """
        # Use PUB sockets (one per topic) for sending publish events
        for topic in self.subscribers.keys():
            if topic not in self.send_socket_dict.keys():
                self.send_socket_dict[topic] = self.context.socket(zmq.PUB)
                while True:
                    port = random.randint(10000, 20000)
                    if port not in self.send_port_dict.values():
                        break
                self.send_port_dict[topic] = port
                self.debug(f"Topic {topic} is being sent at port {port}")
                self.send_socket_dict[topic].bind(f"tcp://{self.get_host_address()}:{port}")

    def disconnect(self):
        """ Method to disconnect from the publish/subscribe system by destroying the ZMQ context """
        self.debug("Disconnect")
        try:
            self.info("Disconnecting. Destroying ZMQ context..")
            self.context.destroy()
            # Don't destroy the zone!
            exit_code = 0
        except Exception as e:
            self.error(f"Failed to destroy context: {e}")
            exit_code = 1
        sys.exit(exit_code)
