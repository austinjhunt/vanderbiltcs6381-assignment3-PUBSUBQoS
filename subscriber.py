import socket as sock
import zmq
import logging
import datetime
import json
import time
import pickle
import netifaces

class Subscriber:
    """ Class to represent a single subscriber in a Publish/Subscribe distributed system.
    Subscriber is indifferent to who is disseminating the information, as long as it knows their
    address(es). Subscriber can subscribed to specific topics and will listen for relevant
    information/updates across all publisher connections. If many publishers with relevant updates,
    updates will be interleaved and no single publisher connection will drown out the others. """

    def __init__(self, filename=None, broker_address="127.0.0.1",
        # own_address="127.0.0.1",
        topics=[], indefinite=False,
        max_event_count=15, centralized=False):
        """ Constructor
        args:
        - broker_address - IP address of broker
        - address - address of host running this subscriber. FIXME: try using python to obtain dynamically.
        - topics (list) - list of topics this subscriber should subscribe to / 'is interested in'
        - indefinite (boolean) - whether to listen for published updates indefinitely
        - max_event_count (int) - if not (indefinite), max number of relevant published updates to receive
         """
        self.id = id(self)
        self.filename = filename
        self.broker_address = broker_address
        # self.own_address = own_address
        self.centralized = centralized
        self.prefix = {'prefix' : f'SUB{id(self)}<{",".join(topics)}> -'}

        if self.centralized:
            self.debug("Initializing subscriber to centralized broker")
        else:
            self.debug("Initializing subscriber to direct publishers")

        self.topics = topics # topic subscriber is interested in
        self.indefinite = indefinite
        self.max_event_count = max_event_count
        self.publisher_connections = {}
        # Create a shared context object for all publisher connections
        self.context = None
        # Poller for incoming publish data
        self.poller = None

        # sockets dictionary -> one per subscription
        # key = topic, value = socket for that topic
        self.sub_socket_dict = {}

        # Socket for registering with broker
        self.broker_reg_socket = None

        # Socket for listening to notifications about new publishers
        # socket type = REP (broker initiates notification as "client")
        self.notify_sub_socket = None

        # a list to store all the messages received
        self.received_message_list = []

        # port on broker to listen for notifications about new hosts
        # without competition/stealing from other subscriber poll()s
        self.notify_port = None


    def configure(self):
        """ Method to perform initial configuration of Subscriber entity """
        self.debug("Initializing")
        # Create a shared context object for all publisher connections
        self.debug("Setting the context object")
        self.context = zmq.Context()
        # Poller for incoming data
        self.debug("Setting the poller objects")
        self.poller = zmq.Poller()
        # now create socket to register with broker
        self.debug("Connecting to register with broker")

        self.broker_reg_socket = self.context.socket(zmq.REQ)
        self.broker_reg_socket.connect(f"tcp://{self.broker_address}:5556")

        # Register self with broker on init
        self.register_sub()

    def setup_notification_polling(self):
        """ Method to set up a socket for polling for notifications about
        new publishers from the broker. The notify port is randomly allocated
        by the broker when the subscriber registers.
        Args:
        - notify_port (int) """
        self.notify_sub_socket = self.context.socket(zmq.REP)
        self.notify_sub_socket.connect(f"tcp://{self.broker_address}:{self.notify_port}")
        self.poller.register(self.notify_sub_socket, zmq.POLLIN)

    def register_sub(self):
        """ Register self with broker """
        self.debug("Registering with broker")
        message_dict = {'address': self.get_host_address(), 'id': self.id, 'topics': self.topics}
        message = json.dumps(message_dict, indent=4)
        self.broker_reg_socket.send_string(message)
        self.debug(f"Sent registration message: {json.dumps(message)}")
        reg_started = self.broker_reg_socket.recv_string()
        reg_started = json.loads(reg_started)
        # Begin listening to this port on broker for notifications.
        self.debug(f"Registration start message from broker: {reg_started}")
        # Structure: {'register_sub': {'notify_port': notify_port}}
        if not self.centralized:
            # Get the port that was allocated for notifications to this subscriber
            # about new publishers
            self.notify_port = reg_started['register_sub']['notify_port']
            # Set up notification polling with that port
            self.setup_notification_polling()
        else:
            # Request the topic/port mapping from broker
            topic_port_request = {'request': 'topic_port_mapping'}
            self.broker_reg_socket.send_string(json.dumps(topic_port_request))
            # Listen for broker to publish about topics
            received_message = self.broker_reg_socket.recv_string()
            self.setup_broker_topic_port_connections(received_message)
            self.debug(f"Successfully set up broker topic/port connections")
        self.info("Registration successful")

    def setup_publisher_direct_connections(self, notification=None):
        """ Method to set up direct connections with publishers
        provided by the broker based on the topic that a subscriber has
        just registered itself with
        Args:
        - notification (list of dicts) new publisher notification from broker in JSON form
        """
        # Broker may send one notification per topic upon register_sub
        # about all publisher addresses publishing that topic. Listen for those first.
        # They are over once 'register_pub' is no longer in message.
        # value is a list of publisher addresses to listen to or a single address
        self.debug("Setting up direct publisher connections")
        for item in notification:
            # each item = { 'register_pub': { 'addresses': addresses,  'topic': t } }
            publisher_addresses = item['register_pub']['addresses']
            # The topic these publishers publish
            topic = item['register_pub']['topic']
            if topic in self.topics:
                # Set up one SUB socket for topic if not already created
                if topic not in self.sub_socket_dict:
                    self.sub_socket_dict[topic] = self.context.socket(zmq.SUB)
                    self.poller.register(self.sub_socket_dict[topic], zmq.POLLIN)
                # Connect to publisher addresses if topic is of interest
                for p in publisher_addresses:
                    self.debug(f'Adding publisher {p} to known publishers')
                    # p includes port!
                    self.sub_socket_dict[topic].connect(f"tcp://{p}")
                    # Set filter <topic> on the socket
                    self.sub_socket_dict[topic].setsockopt_string(zmq.SUBSCRIBE, topic)
        self.debug("Finished setting up direct publisher connections")

    def setup_broker_topic_port_connections(self, received_message):
        """ Method to set up one socket per topic to listen to the broker
        where each topic is published from a different port on the broker address """
        broker_port_dict = json.loads(received_message)
        self.debug("Broker port dict: {broker_port_dict}")
        # Broker will provide the published events so
        # create socket to receive message from broker
        for topic in self.topics:
            # Get the port on which the broker publishes about this topic
            broker_port = broker_port_dict[topic]
            # One SUB socket per topic
            self.sub_socket_dict[topic] = self.context.socket(zmq.SUB)
            self.poller.register(self.sub_socket_dict[topic], zmq.POLLIN)
            self.debug(
                f"Connecting to broker for topic <{topic}> at "
                f"tcp://{self.broker_address}:{broker_port}")
            self.sub_socket_dict[topic].connect(f"tcp://{self.broker_address}:{broker_port}")
            # Set filter <topic> on the socket
            self.sub_socket_dict[topic].setsockopt_string(zmq.SUBSCRIBE, topic)
            self.debug(
                f"Getting Topic {topic} from broker at "
                f"{self.broker_address}:{broker_port}"
                )

    def parse_notification(self):
        """ DECENTRALIZED DISSEMINATION
        Method to parse notification about new publishers from broker
        IF there are new publishers, setup direct connections. """
        # First determine if there are new publisher connections to setup
        # New publisher(s) to add for direct connection
        self.debug("Parsing notification...")
        notification = self.notify_sub_socket.recv_string()
        self.debug(f"Notification: {notification}")
        if 'register_pub' in notification:
            self.debug(f"New register_pub notification...")
            notification = json.loads(notification) # [{'register_pub':{'addresses': [<pub address list>], 'topic': topic published by these pubs}},...]
            self.setup_publisher_direct_connections(notification=notification)
            self.notify_sub_socket.send_string("Notification Acknowledged. New publishers added.")

    def parse_publish_event(self, topic=""):
        self.debug(f"Waiting for publish event for topic {topic}")
        # received_message = self.sub_socket_dict[topic].recv_string()
        [topic, received_message] = self.sub_socket_dict[topic].recv_multipart()
        received_message = pickle.loads(received_message)
        self.received_message_list.append(
            {
                'publisher': received_message['publisher'],
                'topic': received_message['topic'],
                'total_time_seconds': time.time() - float(received_message['publish_time'])
            }
        )
        self.debug(f'Received: <{json.dumps(received_message)}>')


    def notify(self):
        """ Notify method functions independently of dissemination method,
        underlying poller and sockets are either connected only to the broker
        (centralized) or are connected directly to the source publishers """
        self.debug("Start to receive message")
        if self.indefinite:
            while True:
                self.debug("Polling for new events...")
                events = dict(self.poller.poll())
                self.debug("Got events!")
                if self.notify_sub_socket in events:
                    # This is a notification about new publishers
                    self.parse_notification()
                else:
                    # This is a normal publish event from a publisher
                    for topic in self.sub_socket_dict.keys():
                        if self.sub_socket_dict[topic] in events:
                            self.parse_publish_event(topic=topic)
        else:
            for i in range(self.max_event_count):
                events = dict(self.poller.poll())
                if self.notify_sub_socket in events:
                    # This is a notification about new publishers
                    self.parse_notification()
                else:
                    for topic in self.sub_socket_dict.keys():
                        if self.sub_socket_dict[topic] in events:
                            #full_message = self.sub_socket_dict[topic].recv_string() + ' Received at ' + f'{receive_time}'
                            self.parse_publish_event(topic=topic)

    def write_stored_messages(self):
        self.info(f"Writing all stored messages to {self.filename}")
        if self.filename:
            with open(self.filename, 'w') as f:
                header = "publisher,topic,total_time_seconds"
                f.write(f'{header}\n')
                for message in self.received_message_list:
                    publisher = message['publisher']
                    topic = message['topic']
                    total_time_seconds = message['total_time_seconds']
                    f.write(f'{publisher},{topic},{total_time_seconds}\n')
        else:
            self.info("No filename was provided at construction of Subscriber")

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
        except:
            address = "127.0.0.1"
        return address

    def disconnect(self):
        """ Method to disconnect from the pub/sub network """
        # Close all sockets associated with this context
        # Tell broker publisher is disconnecting. Remove from storage.
        msg = {'disconnect': {'id': self.id, 'address': self.get_host_address(),
            'topics': self.topics, 'notify_port': self.notify_port}}
        logging.debug(f"Disconnecting, telling broker: {msg}", extra=self.prefix)
        self.broker_reg_socket.send_string(json.dumps(msg))
        # Wait for response
        response = self.broker_reg_socket.recv_string()
        logging.debug(f"Broker response: {response} ", extra=self.prefix)
        try:
            logging.debug(f'Destroying ZMQ context, closing all sockets', extra=self.prefix)
            self.context.destroy()
        except Exception as e:
            logging.error(f'Could not destroy ZMQ context successfully - {str(e)}', extra=self.prefix)


    def subscribe_to_new_topics(self, topics=[]):
        """ Method to add additional subscriptions/topics of interest for this subscriber
        Args:
        - topics (list) : list of new topics to add to subscriptions if not already added """
        for t in topics:
            if t not in self.topics:
                self.topics.append(t)

    def get_publish_time_from_event(self, event):
        """ Method to pull a datetime object from an event received from a publisher
        where the datetime object represents the time the event was published.
        Args:
        event (str) - event received from publisher
        Return:
        datetime object (time of publish)
        """
        items = [el.strip() for el in event.split('-')]
        # second item is time of publish
        publish_time_string = items[1]
        # Parse using same format used by publisher to generate the string
        return float(publish_time_string)

    def get_time_difference_to_now(self, compare_time):
        """ Method to calculate the difference between some compare_time and now
        Args:
        - compare_time (stringified float)
        Return
        datetime.timedelta
        """
        return time.time() - float(compare_time)


    def info(self, msg):
        logging.info(msg, extra=self.prefix)

    def debug(self, msg):
        logging.debug(msg, extra=self.prefix)

    def error(self, msg):
        logging.error(msg, extra=self.prefix)




