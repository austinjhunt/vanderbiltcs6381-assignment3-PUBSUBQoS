import socket as sock
import zmq
import logging
import datetime

class Subscriber:
    """ Class to represent a single subscriber in a Publish/Subscribe distributed system.
    Subscriber is indifferent to who is disseminating the information, as long as it knows their
    address(es). Subscriber can subscribed to specific topics and will listen for relevant
    information/updates across all publisher connections. If many publishers with relevant updates,
    updates will be interleaved and no single publisher connection will drown out the others. """

    def __init__(self, publishers=[], topics=[], indefinite=False,
        max_event_count=15):
        """ Constructor
        args:
        - publishers (list) - list of IP addresses of publishers created beforehand
        - topics (list) - list of topics this subscriber should subscribe to / 'is interested in'
        - indefinite (boolean) - whether to listen for published updates indefinitely
        - max_event_count (int) - if not (indefinite), max number of relevant published updates to receive
         """

        # the publisher(s) will either be a set of publisher addresses
        # (direct dissemination, subscribers / publishers NOT anonymous to each other)
        # or a single broker address.
        # (broker dissemination, subscribers / publishers anonymous to each other)
        self.publishers = publishers
        self.topics = topics # topic subscriber is interested in
        self.logging_prefix = f'SUB{id(self)}<{",".join(self.topics)}> -'
        self.indefinite = indefinite
        self.max_event_count = max_event_count
        self.publisher_connections = {}
        # Create a shared context object for all publisher connections
        self.zmq_context = zmq.Context()
        # Use a shared zmq SUB socket with the shared context to connect()
        # to one or many publishers
        self.socket = self.zmq_context.socket(zmq.SUB)
        # Apply topics of interest filter to subscriber
        self.apply_topic_filters()
        # connect to all publishers stored in self.publishers
        logging.debug(f"Publishers: {self.publishers}")
        self.connect_to_publishers()

    def add_publisher(self, address=""):
        """ Method to add a publisher to subscriber's known publishers list
        if publisher created after initial topology setup
        Args:
        - address (str) - IP address as string of publisher to connect to
        """
        logging.debug(f'{self.logging_prefix} Adding publisher {address} to known publishers')
        self.publishers.append(address)
        # will skip existing connections, only adds new
        self.connect_to_publishers()

    def connect_to_publishers(self):
        """ Method to connect to all publishers known by this subscriber.
        Could just be a single broker. """
        # ZMQ.SUB can connect to multiple ZMQ.PUB
        for pub in self.publishers:
            if pub not in self.publisher_connections:
                logging.debug(f'{self.logging_prefix} Connecting to publisher {pub} at tcp://{pub}')
                # Only connect if not already connected.
                self.publisher_connections[pub] = self.socket.connect(f'tcp://{pub}')

    def disconnect_from_publishers(self, clean=False, publishers=[]):
        """ Method to disconnect either from all publishers (clean=True)
        or from specific publishers (defined in publishers list of addresses
        Args:
        - clean (bool) : if true, disconnect from all publishers; ignore publishers list
        - publishers (list) : list of specific publisher addresses from which to disconnect
        """
        if clean:
            # Close all sockets associated with this context
            logging.debug(f'{self.logging_prefix} Destroying ZMQ context, closing all sockets')
            try:
                self.zmq_context.destroy()
            except Exception as e:
                logging.error(f'{self.logging_prefix} Could not destroy ZMQ context successfully - {str(e)}')
        else:
            for pub in publishers:
                logging.debug(f'{self.logging_prefix} Disconnecting from {pub}')
                try:
                    self.publisher_connections[pub].close()
                except Exception as e:
                    logging.error(f'{self.logging_prefix} Could not close connection to {pub} successfully - {str(e)}')

    def subscribe_to_new_topics(self, topics=[]):
        """ Method to add additional subscriptions/topics of interest for this subscriber
        Args:
        - topics (list) : list of new topics to add to subscriptions if not already added """
        for t in topics:
            if t not in self.topics:
                self.topics.append(t)

    def apply_topic_filters(self):
        """ Method to apply filter to the subscriber's socket based on topics of interest """
        for t in self.topics:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, t)

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
        return datetime.datetime.strptime(publish_time_string, "%m/%d/%Y, %H:%M:%S")

    def get_time_difference_to_now(self, compare_time): 
        """ Method to calculate the difference between some compare_time and now 
        Args: 
        - compare_time (datetime object)
        Return 
        datetime.timedelta
        """
        return datetime.datetime.now() - compare_time

    
    def receive_response(self, i): 
        """ Method to receive and parse/process a published event 
        Args: 
        - i (int) - number representing the current iteration of event reception
        """
        event = self.socket.recv_string()
        publish_time = self.get_publish_time_from_event(event)
        time_diff = self.get_time_difference_to_now(publish_time)
        logging.debug(f'{self.logging_prefix} Receiving published update {i + 1}: {event} ;total_time={time_diff}')


    def listen(self):
        """ Method to listen for published events matching topic filter applied in
        apply_topic_filters(). Will receive updates in a JSON format.
        """
        if self.indefinite:
            i = 0
            while True:
                i += 1
                # Get response
                self.receive_response(i)
        else:
            for i in range(self.max_event_count):
                # Get response
                self.receive_response(i+1)




