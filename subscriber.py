import socket as sock
import zmq
import logging

class Subscriber:
    # the publisher(s) will either be a set of publisher addresses
    # (direct dissemination, subscribers / publishers NOT anonymous to each other)
    # or a single broker address.
    # (broker dissemination, subscribers / publishers anonymous to each other)

    def __init__(self, publishers=[], topics=[]):
        """ Constructor
        args:
        - publishers (list) - list of IP addresses of publishers created beforehand
        - topics (list) - list of topics this subscriber should subscribe to / 'is interested in' """
        self.publishers = publishers
        self.topics = topics # topic subscriber is interested in
        self.publisher_connections = {}
        # Create a shared context object for all publisher connections
        self.zmq_context = zmq.Context()
        # Use a shared zmq SUB socket with the shared context to connect()
        # to one or many publishers
        self.socket = self.zmq_context.socket(zmq.SUB)
        # connect to all publishers stored in self.publishers
        self.connect_to_publishers()

    def add_publisher(self, address=""):
        """ Method to add a publisher to subscriber's known publishers list
        if publisher created after initial topology setup
        Args:
        - address (str) - IP address as string of publisher to connect to
        """
        logging.debug(f'Adding publisher {address} to known publishers')
        self.publishers.append(address)
        # will skip existing connections, only adds new
        self.connect_to_publishers()

    def connect_to_publishers(self):
        """ Method to connect to all publishers known by this subscriber.
        Could just be a single broker. """
        # ZMQ.SUB can connect to multiple ZMQ.PUB
        for pub in self.publishers:
            if pub not in self.publisher_connections:
                logging.debug(f'Connecting to publisher {pub} at tcp://{pub}:5556')
                # Only connect if not already connected.
                self.publisher_connections[pub] = self.socket.connect(f'tcp://{pub}:5556')

    def disconnect_from_publishers(self, clean=False, publishers=[]):
        """ Method to disconnect either from all publishers (clean=True)
        or from specific publishers (defined in publishers list of addresses
        Args:
        - clean (bool) : if true, disconnect from all publishers; ignore publishers list
        - publishers (list) : list of specific publisher addresses from which to disconnect
        """
        if clean:
            # Close all sockets associated with this context
            logging.debug('Destroying ZMQ context, closing all sockets')
            try:
                self.zmq_context.destroy()
            except Exception as e:
                logging.error(f'Could not destroy ZMQ context successfully - {str(e)}')
        else:
            for pub in publishers:
                logging.debug(f'Disconnecting from {pub}')
                try:
                    self.publisher_connections[pub].close()
                except Exception as e:
                    logging.error(f'Could not close connection to {pub} successfully - {str(e)}')





