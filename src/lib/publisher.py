import socket as sock
import zmq
import logging
import time
import datetime
import json
import pickle
import netifaces

class Publisher:
    """ Class to represent a single publisher in a Publish/Subscribe distributed
    system. Publisher does not need to know who is consuming the information, it
    simply publishes information independently of the consumer. If publisher has
    no connected subscribers, it will drop all messsages it produces. """

    def __init__(self,
        broker_address='127.0.0.1',
        # own_address='127.0.0.1',
        topics=[], sleep_period=1, bind_port=5556,
        indefinite=False, max_event_count=15):
        """ Constructor
        args:
        - broker_address (str) - IP address of broker (port 5556)
        - own_address (str) - IP of host running this publisher
        - topics (list) - list of topics to publish
        - sleep_period (int) - number of seconds to sleep between each publish event
        - bind_port - port on which to publish information
        - indefinite (boolean) - whether to publish events/updates indefinitely
        - max_event_count (int) - if not (indefinite), max number of events/updates to publish
        """
        self.id = id(self)
        self.broker_address = broker_address
        # self.own_address = own_address
        self.topics = topics
        self.sleep_period = sleep_period
        self.bind_port = bind_port
        self.indefinite = indefinite
        self.max_event_count = max_event_count
        self.prefix = {'prefix': f'PUB{id(self)}<{",".join(self.topics)}> -'}
        self.context = None
        self.broker_reg_socket = None
        self.pub_socket = None
        self.pub_port = None

    def configure(self):
        """ Method to perform initial configuration of Publisher """
        logging.debug("Initializing", extra=self.prefix)
        # first get the context
        logging.debug ("Setting the context object", extra=self.prefix )
        self.context = zmq.Context()

        # now create socket to register with broker
        logging.debug("Connecting to register with broker", extra=self.prefix)
        self.broker_reg_socket = self.context.socket(zmq.REQ)
        self.broker_reg_socket.connect("tcp://{0:s}:5555".format(self.broker_address))
        # now create socket to publish
        self.pub_socket = self.context.socket(zmq.PUB)
        self.setup_port_binding()
        logging.debug(f"Binding at {self.get_host_address()} to publish", extra=self.prefix)
        self.register_pub()

    def setup_port_binding(self):
        """
        Method to bind socket to network address to begin publishing/accepting client connections
        using bind_port specified. If bind_port already in use, increment and keep trying until success.
        """
        success = False
        while not success:
            try:
                logging.info(f'Attempting bind to port {self.bind_port}', extra=self.prefix)
                self.pub_socket.bind(f'tcp://*:{self.bind_port}')
                success = True
                logging.info(f'Successful bind to port {self.bind_port}', extra=self.prefix)
            except:
                try:
                    logging.error(f'Port {self.bind_port} already in use, attempting next port', extra=self.prefix)
                    success = False
                    self.bind_port += 1
                except Exception as e:
                    print(e)
        logging.debug("Finished loop", extra=self.prefix)

    def register_pub(self):
        """ Method to register this publisher with the broker """
        logging.debug(f"Registering with broker at {self.broker_address}:5555", extra=self.prefix)
        message_dict = {'address': self.get_host_address(), 'topics': self.topics,
            'id': self.id}
        message = json.dumps(message_dict, indent=4)
        logging.debug(f"Sending registration message: {message}", extra=self.prefix)
        self.broker_reg_socket.send_string(message)
        logging.debug(f"Sent!", extra=self.prefix)
        received = self.broker_reg_socket.recv_string()
        received = json.loads(received)
        if 'success' in received:
            logging.debug(f"Registration successful: {received}", extra=self.prefix)
        else:
            logging.debug(f"Registration failed: {received}", extra=self.prefix)

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
            address = f'{address}:{self.bind_port}'
        except:
            address = f"127.0.0.1:{self.bind_port}"
        return address

    def generate_publish_event(self, iteration=0):
        """ Method to generate a publish event
        Args:
        - iteration (int) - current publish event iteration for this publisher """
        event = {
            # Send this to subscriber even if broker is anonymizing so performance can be analyzed.
            'publisher': self.get_host_address(),
            # If only N topics, then N+1 publish event will publish first topic over again
            'topic': self.topics[iteration % len(self.topics)],
            'publish_time': time.time()
        }
        topic = self.topics[iteration % len(self.topics)].encode('utf8')
        event = [b'%b' % topic, pickle.dumps(event)]
        return event


    def publish(self):
        """ Method to publish events either indefinitely or until a max event count
        is reached """
        if self.indefinite:
            i = 0
            while True:
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=i)
                logging.debug(f'Sending event: [{event}]', extra=self.prefix)
                # self.pub_socket.send_string(event)
                self.pub_socket.send_multipart(event)
                time.sleep(self.sleep_period)
                i += 1
        else:
            for i in range(self.max_event_count):
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=i)
                logging.debug(f'Sending event: [{event}]', extra=self.prefix)
                self.pub_socket.send_multipart(event)
                time.sleep(self.sleep_period)

    def disconnect(self):
        """ Method to disconnect from the pub/sub network """
        # Close all sockets associated with this context
        # Tell broker publisher is disconnecting. Remove from storage.
        msg = {'disconnect': {'id': self.id, 'address': self.get_host_address(),
            'topics': self.topics}}
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

