import socket as sock
import zmq
import logging
import time
import datetime
import json
import pickle
import netifaces
import uuid

from kazoo.client import KazooClient   # client API
from kazoo.client import KazooState    # for the state machine
# to avoid any warning about no handlers for logging purposes, we
# do the following
import logging
logging.basicConfig ()
def listener4state (state):
    if state == KazooState.LOST:
        print ("Current state is now = LOST")
    elif state == KazooState.SUSPENDED:
        print ("Current state is now = SUSPENDED")
    elif state == KazooState.CONNECTED:
        print ("Current state is now = CONNECTED")
    else:
        print ("Current state now = UNKNOWN !! Cannot happen")

class Publisher:
    """ Class to represent a single publisher in a Publish/Subscribe distributed
    system. Publisher does not need to know who is consuming the information, it
    simply publishes information independently of the consumer. If publisher has
    no connected subscribers, it will drop all messsages it produces. """

    def __init__(self,
        broker_address,
        # own_address='127.0.0.1',
        topics=[], sleep_period=1, bind_port=5556,
        indefinite=False, max_event_count=15,
        zk_address="127.0.0.1", zk_port="2181"):
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

        # this is to connect with zookeeper Server
        self.zk_address = zk_address
        self.zk_port = zk_port
        self.zk_server = f"{zk_address}:{zk_port}"

        # this is an identifier for ZooKeeper
        self.instanceId = str(uuid.uuid4())
        print(f"My InstanceId is {self.instanceId}")

        # this is the zk node name
        self.zkName = '/broker'

        # this is in the infor stored in znode
        # this info is broker_address,pub_port,sub_port
        self.znode_value = None
        self.pub_reg_port = "5555"

    def connect_zk(self):
        try:
            print("Try to connect with ZooKeeper server: hosts = {}".format(self.zk_server))
            self.zk = KazooClient(self.zk_server)
            self.zk.add_listener (listener4state)
            print("ZooKeeper Current Status = {}".format (self.zk.state))
        except:
            print("Issues with ZooKeeper, cannot connect with Server")

    def start_session(self):
        """ Starting a Session """
        try:
            # now connect to the server
            self.zk.start()
        except:
            print("Exception thrown in start (): ", sys.exc_info()[0])

    def stop_session (self):
        """ Stopping a Session """
        try:
            # now disconnect from the server
            self.zk.stop ()
        except:
            print("Exception thrown in stop (): ", sys.exc_info()[0])
            return

    def close_connection(self):
        try:
            # now disconnect from the server
            self.zk.close()
        except:
            print("Exception thrown in close (): ", sys.exc_info()[0])
            return

    def get_znode_value (self):
        """ ******************* retrieve a znode value  ************************ """
        try:
            print ("Checking if {} exists (it better be)".format(self.zkName))
            if self.zk.exists (self.zkName):
                print ("{} znode indeed exists; get value".format(self.zkName))
                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get (self.zkName)
                self.znode_value = value.decode("utf-8")
                print(("Details of znode {}: value = {}, stat = {}".format (self.zkName, value, stat)))
                print(f"Values stored in field znode_value is {self.znode_value}")
            else:
                print ("{} znode does not exist, why?".format(self.zkName))
        except:
            print("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    def update_broker_info(self):
        if self.znode_value != None:
            print("Getting broker information from znode_value")
            self.broker_address = self.znode_value.split(",")[0]
            self.pub_reg_port = self.znode_value.split(",")[1]
            print(f"Broker address: {self.broker_address}")
            print(f"Broker Pub Reg Port: {self.pub_reg_port}")

    # -----------------------------------------------------------------------
    def watch_znode_data_change(self):
        #*****************************************************************
        # This is the watch callback function that is supposed to be invoked
        # when changes get made to the znode of interest. Note that a watch is
        # effective only once. So the client has to set the watch every time.
        # To overcome the need for this, Kazoo has come up with a decorator.
        # Decorators can be of two kinds: watching for data on a znode changing,
        # and children on a znode changing
        @self.zk.DataWatch(self.zkName)
        def dump_data_change (data, stat, event):
            if event == None:
                print("No Event")
            elif event.type == 'CHANGED':
                print("Event is {0:s}".format(event.type))
                print("Broker Changed, First close all sockets and terminate the context")
                self.context.destroy()
                print("Broker Changed, Second Update Broker Information")
                print(("Data changed for znode: data = {}".format (data)))
                print(("Data changed for znode: stat = {}".format (stat)))
                self.get_znode_value()
                self.update_broker_info()
                print("Broker Changed, Third Reconnect and Publish")
                self.run_publisher()
            elif event.type == 'DELETED':
                print("Event is {0:s}".format(event.type))

    def run_publisher(self):
        try:
            self.configure()
            self.publish()
        except KeyboardInterrupt:
            self.disconnect()


    def configure(self):
        """ Method to perform initial configuration of Publisher """
        print("Configure Start")
        logging.debug("Initializing", extra=self.prefix)
        # first get the context
        logging.debug ("Setting the context object", extra=self.prefix )
        self.context = zmq.Context()

        # now create socket to register with broker
        logging.debug("Connecting to register with broker", extra=self.prefix)
        self.broker_reg_socket = self.context.socket(zmq.REQ)
        self.broker_reg_socket.connect(f"tcp://{self.broker_address}:{self.pub_reg_port}")
        # now create socket to publish
        self.pub_socket = self.context.socket(zmq.PUB)
        self.setup_port_binding()
        logging.debug(f"Binding at {self.get_host_address()} to publish", extra=self.prefix)
        self.register_pub()
        print("Configure Stop")

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
        print("Publish Start")
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
            event_count = 0
            while event_count < self.max_event_count:
                # Continuous loop over topics
                event = self.generate_publish_event(iteration=event_count)
                logging.debug(f'Sending event: [{event}]', extra=self.prefix)
                self.pub_socket.send_multipart(event)
                time.sleep(self.sleep_period)
                event_count += 1

    def disconnect(self):
        """ Method to disconnect from the pub/sub network """
        # Close all sockets associated with this context
        # Tell broker publisher is disconnecting. Remove from storage.
        print("Disconnect")
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
