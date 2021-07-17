from hashlib import new
from .zookeeper_client import ZookeeperClient
import logging
import random
from time import sleep

class LoadBalancer(ZookeeperClient):
    def __init__(self, clients_per_primary_threshold=3, verbose=False,
        backup_brokers=[], primary_brokers=[], zookeeper_hosts=["127.0.0.1:2181"]):
        """
        Arguments:
        - client_broker_ratio_threshold (int) - ratio of clients (pubs or subs in system) to primary
        broker replicas to serve as a threshold for increasing number of primary replicas
        (i.e. load balancing). Ex: if 3, create one new primary replica for every 3 new clients"""
        self.verbose = verbose
        # initialize counts of brokers to 0 and pubs/subs to 0
        self.primary_broker_replica_count = 0
        self.broker_client_count = 0
        self.load_balance_threshold = clients_per_primary_threshold
        self.set_logger()
        self.primary_brokers = primary_brokers
        self.backup_brokers = backup_brokers # Promotable to primary
        self.max_zone = 0
        self.zones = {}
            #   zones = {
            #   1 : {
            #       "primary_broker": broker object,
            #       "publishers" : [ pub in zone, pub in zone, etc. ],
            #       "subscribers": [ sub in zone, sub in zone, etc. ],
            #       "clients_per_primary" : <some ratio> - each zone has its own ratio
            #   }, .... }
        super().__init__(zookeeper_hosts=zookeeper_hosts)
        # Start a clear Zookeeper session
        self.connect_zk()
        self.start_session()
        self.clear_zookeeper_nodes()

        # Create a znode to keep track of new brokers. Each new broker that gets created will
        # write itself as a child of this znode with a znode name of its id.
        # self.create_znode(znode_name=self.all_primary_brokers_znode, znode_value="all active primary brokers")

    def clear_zookeeper_nodes(self):
        for znode in ["/leaders", "/counts", "/primary_brokers"]:
            self.delete_znode(znode_name=znode, recursive=True)

    def balance_act(self):
        self.debug("Starting balance act :)")
        while True:
            self.debug("sleeping...")
            sleep(1)

    def add_broker_to_backup_pool(self, broker=None):
        self.backup_brokers.append(broker)

    def add_broker_to_primary_pool(self, broker=None):
        """ Broker should be a leader already for a given zone """
        if broker not in self.primary_brokers:
            self.primary_brokers.append(broker)
            # Increment count
            self.primary_broker_replica_count += 1
            self.create_new_primary_broker_zone(primary_broker=broker)

    def get_broker_client_count(self):
        # Get the broker client count (number of pubs and subs in whole system combined)
        broker_client_count = self.get_znode_value(
            znode_name=self.broker_client_count_znode
        )
        try:
            broker_client_count = int(broker_client_count)
        except Exception as e:
            self.error(str(e))
            broker_client_count = 0
        return broker_client_count

    def get_primary_broker_replica_count(self):
        # Get the broker client count (number of pubs and subs in whole system combined)
        primary_broker_replica_count = self.get_znode_value(
            znode_name=self.primary_replica_count_znode
        )
        try:
            primary_broker_replica_count = int(primary_broker_replica_count)
        except Exception as e:
            self.error(str(e))
            primary_broker_replica_count = 0
        return primary_broker_replica_count

    def load_increasing(self, new_broker_client_count=0):
        """ Is the load on the primary broker replica set increasing?"""
        return new_broker_client_count > self.broker_client_count

    def load_decreasing(self, new_broker_client_count=0):
        """ Is the load on the primary broker replica set decreasing?"""
        return new_broker_client_count < self.broker_client_count

    def load_nochange(self, new_broker_client_count=0):
        """ Is the load on the primary broker replica set staying the same?"""
        return new_broker_client_count == self.broker_client_count

    def need_to_promote_backup(self):
        """ Does a backup broker replica need to be promoted? Use
        broker_client/primary_replicas ratio to find out. """
        primary_broker_count = self.get_primary_broker_replica_count()
        broker_client_count = self.get_broker_client_count()
        return (broker_client_count / primary_broker_count) >= self.load_balance_threshold

    def able_to_demote_primary(self):
        """ Can a primary broker be deleted while staying below threshold?
        Called when load is decreasing (pub or sub has left). Use
        broker_client/primary_replicas ratio to find out. """
        primary_broker_count = self.get_primary_broker_replica_count()
        broker_client_count = self.get_broker_client_count()
        return (broker_client_count / (primary_broker_count - 1)) >= self.load_balance_threshold

    def create_new_primary_broker_zone(self, primary_broker=None, publishers=[], subscribers=[]):
        """ Create a zone "owned" by this new primary broker. """
        new_zone = self.max_zone + 1
        self.zones[new_zone] =  {
                "primary_broker" : primary_broker,
                "publishers": publishers,
                "subscribers": subscribers,
                "clients_per_primary" : 0 # initially no clients
            }
        self.max_zone = new_zone

    def promote_backup_broker_to_primary(self):
        """ Select a random broker from the backup pool of brokers and call promote() on it. """
        new_primary_broker = random.choice(self.backup_brokers)
        new_primary_broker.promote()
        self.primary_broker_replica_count = self.get_primary_broker_replica_count()
        self.create_new_primary_broker_zone(primary_broker=new_primary_broker)
        self.backup_brokers.remove(new_primary_broker)

    def dissolve_primary_broker_zone(self, zone=None):
        self.zones.pop(zone)

    def assign_publisher_to_zone(self, publisher=None, zone=None):
        """ Assign a publisher to a zone """
        if zone in self.zones:
            if publisher not in self.zones[zone]['publishers']:
                self.zones[zone]['publishers'].append(publisher)

    def assign_subscriber_to_zone(self, subscriber=None, zone=None):
        """ Assign a subscriber to a zone """
        if zone in self.zones:
            if subscriber not in self.zones[zone]['subscribers']:
                self.zones[zone]['publishers'].append(subscriber)

    def watch_primary_brokers_leave_join(self):
        @self.zk.ChildrenWatch(self.all_primary_brokers_znode)
        def my_func(children):
            self.debug(f"Children (primary brokers): type=({type(children)}), val = {children}")

    def watch_primary_broker_count_change(self):
        """ Watch for new / leaving primary broker replicas """
        @self.zk.DataWatch(self.primary_replica_count_znode)
        def dump_data_change (data, stat, event):
            if event == None:
                pass
            elif event.type == 'CHANGED':
                primary_broker_replica_count = self.get_primary_broker_replica_count()
                if primary_broker_replica_count > self.primary_broker_replica_count:
                    self.debug("Increased capacity! New broker.")
                elif primary_broker_replica_count < self.primary_broker_replica_count:
                    # A primary broker has left. Do we need to promote a backup to fill its spot?
                    if self.need_to_promote_backup():
                        self.promote_backup_broker_to_primary()
            elif event.type == 'DELETED':
                pass

    def watch_client_count_change(self):
        """ Watch for new / leaving pubs/subs"""
        @self.zk.DataWatch(self.broker_client_count_znode)
        def dump_data_change (data, stat, event):
            if event == None:
                pass
            elif event.type == 'CHANGED':
                broker_client_count = self.get_broker_client_count()
                if self.load_increasing(new_broker_client_count=broker_client_count):
                    if self.need_to_promote_backup():
                        self.promote_backup_broker_to_primary()
                if self.load_decreasing(new_broker_client_count=broker_client_count):
                    if self.able_to_demote_primary():
                        self.demote_primary_to_backup()

            elif event.type == 'DELETED':
                pass

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

    def set_logger(self):
        self.prefix = {'prefix': 'LB'}
        self.logger = logging.getLogger(f'LB')
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)