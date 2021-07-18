"""
Create this first. Backup pool of dormant brokers that automatically promote themselves to
leaders of new zones when load threshold (--load_threshold) is exceeded by current
system load read from a Zookeeper Znode (shared by all brokers for current load status updates
via watch mechanism). Backup pool brokers are not initially assigned a zone.
"""
from .broker import Broker
from .zookeeper_client import ZookeeperClient
import zmq
import json
import random
import logging
import pickle
import netifaces
import sys
import time
class BackupPool(ZookeeperClient):
    def __init__(self, zookeeper_hosts=['127.0.0.1:2181'], centralized=False, indefinite=False,
        max_event_count=15, verbose=False, threshold=3):
        super().__init__(zookeeper_hosts=zookeeper_hosts, verbose=verbose)
        self.verbose = verbose
        self.zookeeper_hosts_arg = zookeeper_hosts
        self.threshold = threshold
        # Pass these to brokers on creation
        self.max_event_count = max_event_count
        self.centralized = centralized
        self.indefinite = indefinite
        self.set_logger()
        self.connect_zk()
        self.start_session()
        self.setup_current_load_znode()

    def setup_current_load_znode(self):
        ## Assume this service is created FIRST.
        self.zk.ensure_path('/shared_state/')
        self.current_load_znode = "/shared_state/current_load/"
        if not self.znode_exists(self.current_load_znode):
            self.create_znode(
                znode_name=self.current_load_znode,
                znode_value=0
            )

    def wait_for_trigger(self):
        while True:
            time.sleep(0.5)

    def need_more_capacity(self):
        current_load = self.get_znode_value(znode_name=self.current_load_znode)
        return float(current_load) > self.threshold

    def get_new_zone_number(self):
        zones = self.zk.get_children("/primaries")
        max_current_zone_num = 0
        for z in zones:
            zone_num = int(z.split("_")[1])
            if zone_num > max_current_zone_num:
                max_current_zone_num = zone_num
        return max_current_zone_num + 1

    def spin_up_new_broker(self):
        self.debug('Spinning up a new broker!')
        new_zone = self.get_new_zone_number()
        broker = Broker(
            centralized=self.centralized,
            indefinite=self.indefinite,
            max_event_count=self.max_event_count,
            zookeeper_hosts=self.zookeeper_hosts_arg,
            verbose=self.verbose,
            zone=new_zone
        )
        # Create a new zone managed by this new primary broker
        broker.connect_zk()
        broker.start_session()
        broker.setup_fault_tolerance_znode()
        broker.setup_shared_state_znode()
        broker.setup_load_balancing_znode()
        broker.watch_shared_state_publishers()
        broker.watch_shared_state_subscribers()
        # FIXME: this is going to block! Will this spin up only work for one broker?
        self.debug(f'Running election for new broker {id(broker)}')
        broker.zk_run_election()

    def watch_system_load(self):
        @self.zk.DataWatch(self.current_load_znode)
        def dump_data_change (data, stat, event):
            if not event:
                pass
            elif event.type == 'CHANGED':
                # Load has been updated by a broker
                self.debug('System load has changed!')
                if self.need_more_capacity():
                    self.debug('Need more capacity!')
                    self.spin_up_new_broker()

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

    def set_logger(self, prefix=None):
        if not prefix:
            self.prefix = {'prefix': f'BackupPool'}
        else:
            self.prefix = {'prefix': prefix}
        self.logger = logging.getLogger(f'BackupPool')
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        for h in self.logger.handlers:
            self.logger.removeHandler(h)
        self.logger.addHandler(handler)