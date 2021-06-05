import socket as sock
import zmq
import logging
import time
import datetime
class Publisher:
    """ Class to represent a single publisher in a Publish/Subscribe distributed
    system. Publisher does not need to know who is consuming the information, it
    simply publishes information independently of the consumer. If publisher has
    no connected subscribers, it will drop all messsages it produces. """

    def __init__(self, topics=[], sleep_period=1, bind_port=5556,
        indefinite=False, max_event_count=15):
        """ Constructor
        args:
        - topics (list) - list of topics to publish
        - sleep_period (int) - number of seconds to sleep between each publish event
        - bind_port - port on which to publish information
        - indefinite (boolean) - whether to publish events/updates indefinitely
        - max_event_count (int) - if not (indefinite), max number of events/updates to publish
        """
        self.topics = topics
        self.sleep_period = sleep_period
        self.bind_port = bind_port
        self.indefinite = indefinite
        self.max_event_count = max_event_count

        # Create ZMQ context
        self.zmq_context = zmq.Context()
        # Create and store single zmq publisher socket type
        self.socket = self.zmq_context.socket(zmq.PUB)
        # Bind socket to network address to begin accepting client connections
        # using port specified
        self.socket.bind(f'tcp://*:{self.bind_port}')

        self.logging_prefix = f'PUB{id(self)}<{",".join(self.topics)}> -'

    def current_time_to_string(self):
        """ Method to return the current time as a string;
        allows time to be sent as part of publish event so subscriber
        can calculate send - receive time difference for performance testing """
        return datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

    def get_address(self):
        """ Method to return the IP address and port (IP:PORT) of the current host as a string"""
        return f'{sock.gethostbyname(sock.gethostname())}:{self.bind_port}'

    def generate_publish_event(self, iteration=0):
        """ Method to generate a publish event
        Args:
        - iteration (int) - current publish event iteration for this publisher """
        # If only N topics, then N+1 publish event will publish first topic over again
        current_topic = self.topics[iteration % len(self.topics)]
        current_time = self.current_time_to_string()
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
                logging.debug(f'{self.logging_prefix} sending event: [{event}]')
                self.socket.send_string(event)
                time.sleep(self.sleep_period)
                i += 1
        else:
            for i in range(self.max_event_count):
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=i)
                logging.debug(f'{self.logging_prefix} sending event: [{event}]')
                self.socket.send_string(event)
                time.sleep(self.sleep_period)
