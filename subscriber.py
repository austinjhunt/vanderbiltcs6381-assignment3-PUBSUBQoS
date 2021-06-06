import socket as sock
import zmq
import logging
import datetime
import json
import time

class Subscriber:
    """ Class to represent a single subscriber in a Publish/Subscribe distributed system.
    Subscriber is indifferent to who is disseminating the information, as long as it knows their
    address(es). Subscriber can subscribed to specific topics and will listen for relevant
    information/updates across all publisher connections. If many publishers with relevant updates,
    updates will be interleaved and no single publisher connection will drown out the others. """

    def __init__(self, broker_address="127.0.0.1", own_address="127.0.0.1", topics=[], indefinite=False,
        max_event_count=15, centralized=False):
        """ Constructor
        args:
        - broker_address - IP address of broker
        - address - address of host running this subscriber. FIXME: try using python to obtain dynamically.
        - topics (list) - list of topics this subscriber should subscribe to / 'is interested in'
        - indefinite (boolean) - whether to listen for published updates indefinitely
        - max_event_count (int) - if not (indefinite), max number of relevant published updates to receive
         """
        self.broker_address = broker_address
        self.own_address = own_address
        self.centralized = centralized
        self.prefix = {'prefix' : f'SUB{id(self)}<{",".join(topics)}> -'}

        if self.centralized:
            logging.debug("Initializing subscriber to centralized broker", extra=self.prefix)
        else:
            logging.debug("Initializing subscriber to direct publishers", extra=self.prefix)

        self.topics = topics # topic subscriber is interested in
        self.indefinite = indefinite
        self.max_event_count = max_event_count
        self.publisher_connections = {}
        # Create a shared context object for all publisher connections
        self.context = None
        # Poller for incoming data
        self.poller = None
        # sockets dictionary -> one per subscription
        self.sub_socket_dict = {}
        self.broker_reg_socket = None


    def configure(self):
        """ Method to perform initial configuration of Subscriber entity """
        logging.debug("Initializing", extra=self.prefix)
        # Create a shared context object for all publisher connections
        logging.debug ("Setting the context object", extra=self.prefix)
        self.context = zmq.Context()
        # Poller for incoming data
        logging.debug("Setting the poller object", extra=self.prefix)
        self.poller = zmq.Poller()
                # now create socket to register with broker
        logging.debug("Connecting to register with broker", extra=self.prefix)
        self.broker_reg_socket = self.context.socket(zmq.REQ)
        self.broker_reg_socket.connect(f"tcp://{self.broker_address}:5556")
        # Register self with broker on init
        self.register_sub()

    def register_sub(self):
        """ Register self with broker """
        logging.debug("Registering with broker", extra=self.prefix)
        message_dict = {'address': self.own_address, 'topics': self.topics}
        message = json.dumps(message_dict, indent=4)
        self.broker_reg_socket.send_string(message)
        if not self.centralized:
            # Listen for publishers directly to publish about topics
            self.setup_publisher_direct_connections()
        else:
            # Listen for broker to publish about topics
            self.setup_broker_topic_port_connections()

        logging.debug("Registration complete", extra=self.prefix)

    def setup_publisher_direct_connections(self):
        """ Method to set up direct connections with publishers
        provided by the broker based on the topic that a subscriber has
        just registered itself with """
        # Broker may send one notification per topic upon register_sub
        # about all publisher addresses publishing that topic. Listen for those first.
        # They are over once 'register_pub' is no longer in message.
        logging.debug("Setting up direct connections to publishers provided by broker", extra=self.prefix)
        register_pub = True
        while register_pub:
            register_pub = False
            logging.debug("Waiting for message...", extra=self.prefix)
            received_message = json.loads(self.broker_reg_socket.recv_string())
            logging.debug(f'Received: {received_message}', extra=self.prefix)
            if 'register_pub' in received_message:
                register_pub = True
                # value is a list of publisher addresses to listen to
                publisher_addresses = received_message['register_pub']['addresses']
                # The topic these publishers publish
                topic = received_message['register_pub']['topic']
                self.add_publisher_direct_connections_helper(
                    publisher_addresses=publisher_addresses,
                    topic=topic
                )

    def add_publisher_direct_connections_helper(self, publisher_addresses=[], topic=""):
        """ DECENTRALIZED DISSEMINATION
        Helper method to add direct connections to publisher addresses
        that have been provided by the broker (for a specific topic)
        Args:
        - publisher_addresses (list) - list of publisher IP addresses to which to connect directly
        - topic (str) - the topic published by these publishers to use as the key for the new socket
        """
        # One SUB socket per topic
        self.sub_socket_dict[topic] = self.context.socket(zmq.SUB)
        self.poller.register(self.sub_socket_dict[topic], zmq.POLLIN)
        for p in publisher_addresses:
            logging.debug(f'Adding publisher {p} to known publishers', extra=self.prefix)
            self.publishers.append(p)
            self.sub_socket_dict[topic].connect(f"tcp://{p}:5556")
            # Set filter <topic> on the socket
            self.sub_socket_dict[topic].setsockopt_string(zmq.SUBSCRIBE, topic)

    def setup_broker_topic_port_connections(self):
        """ Method to set up one socket per topic to listen to the broker
        where each topic is published from a different port on the broker address """
        received_message = self.broker_reg_socket.recv_string()
        broker_port_dict = json.loads(received_message)
        # Broker will provide the published events so
        # create socket to receive message from broker
        for topic in self.topics:
            # Get the port on which the broker publishes about this topic
            broker_port = broker_port_dict[topic]
            # One SUB socket per topic
            self.sub_socket_dict[topic] = self.context.socket(zmq.SUB)
            self.poller.register(self.sub_socket_dict[topic], zmq.POLLIN)
            logging.debug(
                f"Connecting to broker for topic <{topic}> at "
                f"tcp://{self.broker_address}:{broker_port}", extra=self.prefix)
            self.sub_socket_dict[topic].connect(f"tcp://{self.broker_address}:{broker_port}")
            # Set filter <topic> on the socket
            self.sub_socket_dict[topic].setsockopt_string(zmq.SUBSCRIBE, topic)
            logging.debug(
                f"Getting Topic {topic} from broker at "
                f"{self.broker_address}:{broker_port}", extra=self.prefix
                )

    def notify(self):
        """ Notify method functions independently of dissemination method,
        underlying poller and sockets are either connected only to the broker
        (centralized) or are connected directly to the source publishers, but
        information is pulled in the same way for both methods """
        logging.debug("Start to receive message", extra=self.prefix)
        if self.indefinite:
            while True:
                events = dict(self.poller.poll())
                for topic in self.sub_socket_dict.keys():
                    if self.sub_socket_dict[topic] in events:
                        receive_time = time.time()
                        full_message = self.sub_socket_dict[topic].recv_string() + ' Received at ' + f'{receive_time}'
                        self.received_message_list.append(full_message)
                        logging.debug(f'Received: <{full_message}>', extra=self.prefix)
        else:
            for i in range(self.max_event_count):
                events = dict(self.poller.poll())
                for topic in self.sub_socket_dict.keys():
                    if self.sub_socket_dict[topic] in events:
                        receive_time = time.time()
                        full_message = self.sub_socket_dict[topic].recv_string() + ' Received at ' + f'{receive_time}'
                        self.received_message_list.append(full_message)
                        logging.debug(f'Received: <{full_message}>', extra=self.prefix)


    def disconnect(self):
        """ Method to disconnect from the pub/sub network """
        # Close all sockets associated with this context
        logging.debug(f'Destroying ZMQ context, closing all sockets', extra=self.prefix)
        try:
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
        - compare_time (datetime object)
        Return
        datetime.timedelta
        """
        return time.time() - compare_time






