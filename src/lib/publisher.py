import socket as sock
from .zookeeper_client import ZookeeperClient
import zmq
import logging
import time
import datetime
import json
import pickle
import netifaces
import uuid
import sys

class Publisher(ZookeeperClient):
    """ Class to represent a single publisher in a Publish/Subscribe distributed
    system. Publisher does not need to know who is consuming the information, it
    simply publishes information independently of the consumer. If publisher has
    no connected subscribers, it will drop all messsages it produces. """

    def __init__(self,
        broker_address,
        # own_address='127.0.0.1',
        topics=[], sleep_period=1, bind_port=5556,
        indefinite=False, max_event_count=15,zookeeper_hosts=["127.0.0.1:2181"]):
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
        self.context = None
        self.broker_reg_socket = None
        self.pub_socket = None
        self.pub_port = None
        self.pub_reg_port = 5555
        self.set_logger()

        # Set up initial config for ZooKeeper client.
        super().__init__(zookeeper_hosts)

        self.WATCH_FLAG = False

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

    def set_logger(self):
        self.prefix = {'prefix': f'PUB{id(self)}<{",".join(self.topics)}> -'}
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.setLevel(logging.DEBUG)

    def get_znode_value (self):
        """ ******************* retrieve a znode value  ************************ """
        try:
            self.debug ("Checking if {} exists (it better be)".format(self.zk_name))
            if self.zk.exists (self.zk_name):
                self.debug ("{} znode indeed exists; get value".format(self.zk_name))
                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zk_name, watch=self.watch)
                value,stat = self.zk.get (self.zk_name)
                self.znode_value = value.decode("utf-8")
                self.debug(("Details of znode {}: value = {}, stat = {}".format (self.zk_name, value, stat)))
                self.debug(f"Values stored in field znode_value is {self.znode_value}")
            else:
                self.debug ("{} znode does not exist, why?".format(self.zk_name))
        except:
            self.debug("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    def update_broker_info(self):
        if self.znode_value != None:
            self.debug("Getting broker information from znode_value")
            self.broker_address = self.znode_value.split(",")[0]
            self.pub_reg_port = self.znode_value.split(",")[1]
            self.debug(f"Broker address: {self.broker_address}")
            self.debug(f"Broker Pub Reg Port: {self.pub_reg_port}")

    # -----------------------------------------------------------------------
    def watch_znode_data_change(self):
        #*****************************************************************
        # This is the watch callback function that is supposed to be invoked
        # when changes get made to the znode of interest. Note that a watch is
        # effective only once. So the client has to set the watch every time.
        # To overcome the need for this, Kazoo has come up with a decorator.
        # Decorators can be of two kinds: watching for data on a znode changing,
        # and children on a znode changing
        @self.zk.DataWatch(self.zk_name)
        def dump_data_change (data, stat, event):
            if event == None:
                self.WATCH_FLAG = True
                self.debug("No ZNODE Event - First Watch Call! Initializing publisher...")
                self.configure()
                self.WATCH_FLAG = False
            elif event.type == 'CHANGED':
                self.WATCH_FLAG = True
                self.debug("ZNODE CHANGED")
                self.debug("Close all sockets and terminate the context")
                self.context.destroy()
                self.debug("Update Broker Information")
                self.debug(f"Data changed for znode: data={data},stat={stat}")
                self.get_znode_value()
                self.update_broker_info()
                self.debug("Reconfiguring...")
                self.configure()
                self.WATCH_FLAG = False
            elif event.type == 'DELETED':
                self.debug("ZNODE DELETED")

    def configure(self):
        """ Method to perform initial configuration of Publisher """
        self.debug("Configure Start")
        self.debug("Initializing")
        # first get the context
        self.debug ("Setting the context object" )
        self.context = zmq.Context()

        # now create socket to register with broker
        self.debug("Connecting to register with broker")
        self.broker_reg_socket = self.context.socket(zmq.REQ)
        self.broker_reg_socket.connect(f"tcp://{self.broker_address}:{self.pub_reg_port}")
        # now create socket to publish
        self.pub_socket = self.context.socket(zmq.PUB)
        self.setup_port_binding()
        self.debug(f"Binding at {self.get_host_address()} to publish")
        self.register_pub()
        self.debug("Configure Stop")

    def setup_port_binding(self):
        """
        Method to bind socket to network address to begin publishing/accepting client connections
        using bind_port specified. If bind_port already in use, increment and keep trying until success.
        """
        success = False
        while not success:
            try:
                self.info(f'Attempting bind to port {self.bind_port}')
                self.pub_socket.bind(f'tcp://*:{self.bind_port}')
                success = True
                self.info(f'Successful bind to port {self.bind_port}')
            except:
                try:
                    self.error(f'Port {self.bind_port} already in use, attempting next port')
                    success = False
                    self.bind_port += 1
                except Exception as e:
                    self.debug(e)
        self.debug("Finished loop")

    def register_pub(self):
        """ Method to register this publisher with the broker """
        self.debug(f"Registering with broker at {self.broker_address}:5555")
        message_dict = {'address': self.get_host_address(), 'topics': self.topics,
            'id': self.id}
        message = json.dumps(message_dict, indent=4)
        self.debug(f"Sending registration message: {message}")
        self.broker_reg_socket.send_string(message)
        self.debug(f"Sent!")
        received = self.broker_reg_socket.recv_string()
        received = json.loads(received)
        if 'success' in received:
            self.debug(f"Registration successful: {received}")
        else:
            self.debug(f"Registration failed: {received}")

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
        self.debug("Publish Start")
        if self.indefinite:
            i = 0
            while True:
                if not self.WATCH_FLAG:
                    # Continuous loop over topics
                    event = self.generate_publish_event(iteration=i)
                    self.debug(f'Sending event: [{event}]')
                    # self.pub_socket.send_string(event)
                    self.pub_socket.send_multipart(event)
                    time.sleep(self.sleep_period)
                    i += 1
                else:
                    self.debug("SWITCHING BROKER")
        else:
            event_count = 0
            while event_count < self.max_event_count:
                if not self.WATCH_FLAG:
                    # Continuous loop over topics
                    event = self.generate_publish_event(iteration=event_count)
                    self.debug(f'Sending event: [{event}]')
                    self.pub_socket.send_multipart(event)
                    time.sleep(self.sleep_period)
                    event_count += 1
                else:
                    self.debug("SWITCHING BROKER")

    def disconnect(self):
        """ Method to disconnect from the pub/sub network """
        # Close all sockets associated with this context
        # Tell broker publisher is disconnecting. Remove from storage.
        self.debug("Disconnect")
        msg = {'disconnect': {'id': self.id, 'address': self.get_host_address(),
            'topics': self.topics}}
        self.debug(f"Disconnecting, telling broker: {msg}")
        self.broker_reg_socket.send_string(json.dumps(msg))
        # Wait for response
        response = self.broker_reg_socket.recv_string()
        self.debug(f"Broker response: {response} ")
        try:
            self.debug(f'Destroying ZMQ context, closing all sockets')
            self.context.destroy()
        except Exception as e:
            self.error(f'Could not destroy ZMQ context successfully - {str(e)}')
