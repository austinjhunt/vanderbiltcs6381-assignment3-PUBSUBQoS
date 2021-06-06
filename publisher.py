import socket as sock
import zmq
import logging
import time
import datetime
import json
class Publisher:
    """ Class to represent a single publisher in a Publish/Subscribe distributed
    system. Publisher does not need to know who is consuming the information, it
    simply publishes information independently of the consumer. If publisher has
    no connected subscribers, it will drop all messsages it produces. """

    def __init__(self,
        broker_address='127.0.0.1',
        own_address='127.0.0.1',
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
        self.broker_address = broker_address
        self.own_address = own_address
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
        logging.debug(f"Binding at {self.own_address}:{self.bind_port} to publish", extra=self.prefix)
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
        logging.debug(f"Registering with broker at {self.broker_address}", extra=self.prefix)
        message_dict = {'address': f'{self.own_address}:{self.bind_port}', 'topics': self.topics}
        message = json.dumps(message_dict, indent=4)
        self.broker_reg_socket.send_string(message)
        logging.debug(f'Received message: {self.broker_reg_socket.recv_string()}', extra=self.prefix)

    def get_address(self):
        """ Method to return the IP address and port (IP:PORT) of the current host as a string"""
        return self.own_address
        # return f'{sock.gethostbyname(sock.gethostname())}:{self.bind_port}'

    def generate_publish_event(self, iteration=0):
        """ Method to generate a publish event
        Args:
        - iteration (int) - current publish event iteration for this publisher """
        # If only N topics, then N+1 publish event will publish first topic over again
        current_topic = self.topics[iteration % len(self.topics)]
        current_time = time.time()
        ip_address = self.get_address()
        return  f'{current_topic} - {current_time} - {ip_address}'

    # Assumption:
    # Publisher is only going to publish a limited number of topics.
    def publish(self):
        if self.indefinite:
            i = 0
            while True:
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=i)
                logging.debug(f'Sending event: [{event}]', extra=self.prefix)
                self.pub_socket.send_string(event)
                time.sleep(self.sleep_period)
                i += 1
        else:
            for i in range(self.max_event_count):
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=i)
                logging.debug(f'Sending event: [{event}]', extra=self.prefix)
                self.pub_socket.send_string(event)
                time.sleep(self.sleep_period)
