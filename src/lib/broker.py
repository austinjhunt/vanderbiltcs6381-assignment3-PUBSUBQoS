"""
Message Broker to serve as anonymizing middleware between
publishers and subscribers
"""
from src.lib.zookeeper_client import ZookeeperClient
from kazoo.exceptions import KazooException
import zmq
import json
import traceback
import random
import logging
import pickle
import netifaces
import sys
import uuid
from kazoo.client import KazooClient, KazooState



class Broker(ZookeeperClient):
    #################################################################
    # constructor
    #################################################################
    def __init__(self, centralized=False, indefinite=False, max_event_count=15,
        zookeeper_hosts=['127.0.0.1:2181']):
        self.centralized = centralized
        self.prefix = {'prefix': 'BROKER - '}
        if self.centralized:
            log_format = logging.Formatter('%(message)s')
            log_format._fmt = "CENTRAL PUBLISHING BROKER - " + log_format._fmt
            self.debug("Initializing broker for centralized dissemination")
        else:
            log_format = logging.Formatter('%(message)s')
            log_format._fmt = "DECENTRAL PUBLISHING BROKER - " + log_format._fmt
            self.debug("Initializing broker for decentralized dissemination")
        # Either poll for events indefinitely or for specified max_event_count
        self.indefinite = indefinite
        self.max_event_count = max_event_count
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

        # Initialize configuration for ZooKeeper client
        super().__init__(zookeeper_hosts=zookeeper_hosts)
        self.debug(f"My Zookeeper instance ID is {self.zk_instance_id}")

        # this is for write into the znode about the broker information
        self.pub_reg_port = 5555
        self.sub_reg_port = 5556
        self.znode_value = f"{self.get_host_address()},{self.pub_reg_port},{self.sub_reg_port}"

    def info(self, msg):
        logging.info(msg, extra=self.prefix)

    def error(self, msg):
        logging.error(msg, extra=self.prefix)

    def debug(self, msg):
        logging.debug(msg, extra=self.prefix)


    def leader_function(self):
        self.debug(f"I am the leader {str(self.zk_instance_id)}")
        self.debug(f"Create a Znode with my information")
        if self.zk.exists(self.zk_name):
            ## FIXME: since znode /broker is ephemeral, then its existence implies liveness of another leader
            self.debug(f"{self.zk_name} znode exists : only modify the value)")
            self.modify_znode_value(self.znode_value.encode('utf-8'))
        else:
            self.debug(f"{self.zk_name} znode does not exists : create one)")
            self.create_znode()
        # the following does not necessarily change
        self.debug("Configure Myself")
        self.configure()
        try:
            self.event_loop()
        except KeyboardInterrupt:
            # If you interrupt/cancel a broker, be sure to disconnect/clean all sockets
            self.disconnect()

    def zk_run_election(self):
        self.election = self.zk.Election("/electionpath", self.zk_instance_id)
        self.debug("contenders", self.election.contenders())
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
        self.debug("Opening two REP sockets for publisher registration "
            "and subscriber registration")
        self.debug("Enabling publisher registration on port 5555")
        self.debug("Enabling subscriber registration on port 5556")
        self.pub_reg_socket = self.context.socket(zmq.REP)
        self.setup_pub_port_reg_binding()
        self.sub_reg_socket = self.context.socket(zmq.REP)
        self.setup_sub_port_reg_binding()
        self.used_ports.append(self.pub_reg_port)
        self.used_ports.append(self.sub_reg_port)
        logging.debug(f"Enabling publisher registration on port {self.pub_reg_port}", extra=self.prefix)
        logging.debug(f"Enabling subscriber registration on port {self.sub_reg_port}", extra=self.prefix)

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
                logging.info(f'Attempting bind to port {self.pub_reg_port}', extra=self.prefix)
                self.pub_reg_socket.bind(f'tcp://*:{self.pub_reg_port}')
                success = True
                logging.info(f'Successful bind to port {self.pub_reg_port}', extra=self.prefix)
            except:
                try:
                    logging.error(f'Port {self.pub_reg_port} already in use, attempting next port', extra=self.prefix)
                    success = False
                    self.pub_reg_port += 1
                except Exception as e:
                    self.debug(e)
        logging.debug("Finished loop", extra=self.prefix)

    def setup_sub_port_reg_binding(self):
        """
        Method to bind socket to network address to begin publishing/accepting client connections
        using bind_port specified. If bind_port already in use, increment and keep trying until success.
        """
        success = False
        while not success:
            try:
                logging.info(f'Attempting bind to port {self.sub_reg_port}', extra=self.prefix)
                self.sub_reg_socket.bind(f'tcp://*:{self.sub_reg_port}')
                success = True
                logging.info(f'Successful bind to port {self.sub_reg_port}', extra=self.prefix)
            except:
                try:
                    logging.error(f'Port {self.sub_reg_port} already in use, attempting next port', extra=self.prefix)
                    success = False
                    self.sub_reg_port += 1
                except Exception as e:
                    self.debug(e)
        logging.debug("Finished loop", extra=self.prefix)

    def parse_events(self, index):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Parse events returned by ZMQ poller and handle accordingly
        Args:
        - index (int) - event index, just used for logging current event loop index
         """
        # find which socket was enabled and accordingly make a callback
        # Note that we must check for all the sockets since multiple of them
        # could have been enabled.
        # The return value of poll() is a socket to event mask mapping
        # self.debug("Polling for events...")
        try:
            events = dict(self.poller.poll())
        except zmq.error.ZMQError as e:
            if 'Socket operation on non-socket' in str(e):
                self.error(f'Exception with self.poller.poll(): {e}')
                self.disconnect()
                sys.exit(1)

        if self.pub_reg_socket in events:
            self.register_pub()
            if self.centralized:
                # Open a SUB socket to receive from publisher
                self.update_receive_socket()
        elif self.sub_reg_socket in events:
            self.debug(f"Event {index}: subscriber")
            self.register_sub()
        # For centralized dissemination, also handle sending
        if self.centralized:
            for topic in self.receive_socket_dict.keys():
                if self.receive_socket_dict[topic] in events:
                    self.send(topic)

    #################################################################
    # Run the event loop
    #################################################################
    def event_loop(self):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Poll for events either indefinitely or until a specific
        event count (self.max_event_count, passed in constructor) is reached """
        self.debug("Start Event Loop")
        if self.indefinite:
            self.debug("Begin indefinite event poll loop")
            i = 0 # index used just for logging event index
            while True:
                i += 1
                self.parse_events(i)
        else:
            self.debug(f"Begin finite (max={self.max_event_count}) event poll loop")
            event_count = 0
            while event_count < self.max_event_count:
                self.parse_events(event_count+1)
                event_count += 1

    # once a publisher register with broker, the broker will start to receive message from it
    # for a particular topic, the broker will open a SUB socket for a topic
    def update_receive_socket(self):
        """ CENTRALIZED DISSEMINATION
        Once publisher registers with broker, broker will begin receiving messages from it
        for a given topic; broker must open a SUB socket for the topic if not already opened"""
        self.debug("Updating receive socket to 'subscribe' to publisher")
        for topic in self.publishers.keys():
            if topic not in self.receive_socket_dict.keys():
                self.receive_socket_dict[topic] = self.context.socket(zmq.SUB)
                self.poller.register(self.receive_socket_dict[topic], zmq.POLLIN)
            for address in self.publishers[topic]:
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
            # self.send_socket_dict[topic].send_string(received_message)
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
        if not self.centralized:
            notify_port = dc['notify_port']
            self.used_ports.remove(notify_port)
            # Close the notification socket for this subscriber with id as key
            self.notify_sub_sockets[sub_id].close()
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
                    self.subscribers[t].remove(address)
                    # No need to update self.send_socket_dict. No outward connections with connect()
                    # to disconnect() as with receive_socket_dict.

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
            for topic in topics:
                if topic not in self.subscribers.keys():
                    self.subscribers[topic] = [sub_address]
                else:
                    self.subscribers[topic].append(sub_address)

            if not self.centralized:
                # Allocate a random unique port to notify this subscriber about new hosts.
                # Port must be different for each subscriber since they are each polling
                # and subscribers steal poll pipeline events from each other.
                notify_port = self.get_clear_port()
                msg = {'register_sub': {'notify_port': notify_port}}
                ## Notify new subscriber about all publishers of topic
                ## so they can listen directly
                # Set up a new notify socket on a clear port for this subscriber
                self.debug("Enabling subscriber notification (about publishers)",
                    extra=self.prefix)
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
        except Exception as e:
            self.error(e)

    def notify_subscribers(self, topics, pub_address=None, sub_id=None):
        """ DECENTRALIZED DISSEMINATION
        Tell the subscribers of a given topic that a new publisher
        of this topic has been added; they should start listening to
        that/those publishers directly """
        self.debug("Notifying subscribers")
        message = []
        addresses = []
        if pub_address: # when registering single new publisher
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
                self.debug(f"Sending notification to sub: {sub_id}")
                notify_socket.send_string(message)
                self.debug(f"Waiting for response...")
                confirmation = notify_socket.recv_string()
                self.debug(f"Subscriber notified successfully (confirmation: <{confirmation}>")

        else: # when registering a new subscriber
            for t in topics:
                if t in self.publishers:
                    addresses = self.publishers[t]
                else:
                    addresses = []
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
                self.publishers[t].remove(address)
                if self.centralized:
                    self.receive_socket_dict[t].disconnect(address)
        response = {'disconnect': 'success'}
        return json.dumps(response)

    def register_pub(self):
        """ BOTH CENTRAL AND DECENTRALIZED DISSEMINATION
        Register (or disconnect) a publisher as a publisher of a given topic,
        e.g. 1.2.3.4 registering as publisher of topic "XYZ" """
        try:
            # the format of the registration string is a json
            # '{ "address":"1234", "topics":['A', 'B']}'
            pub_reg_string = self.pub_reg_socket.recv_string()
            self.debug(f"Publisher Registration Started: {pub_reg_string}")
            pub_reg_dict = json.loads(pub_reg_string)
            # Handle disconnect if requested
            if 'disconnect' in pub_reg_dict:
                response = self.disconnect_pub(msg=pub_reg_dict)
                # send response
                self.pub_reg_socket.send_string(response)
                return
            pub_address = pub_reg_dict['address']
            for topic in pub_reg_dict['topics']:
                if topic not in self.publishers.keys():
                    self.publishers[topic] = [pub_address]
                else:
                    self.publishers[topic].append(pub_address)

            if not self.centralized:
                # For de-centralized dissemination:
                # Subscribers interested in this topic need to know to listen
                # directly to this new publisher.
                # This starts a while loop on the subscriber.
                self.notify_subscribers(pub_reg_dict['topics'], pub_address=pub_address)
            response = {'success': 'registration success'}
        except Exception as e:
            response = {'error': f'registration failed due to exception: {e}'}
        self.debug(f"Sending response: {response}")
        self.pub_reg_socket.send_string(json.dumps(response))
        self.debug("Publisher Registration Succeeded")

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
        except Exception as e:
            self.error(f"Failed to destroy context: {e}")
