""" All entities in pub/sub are clients of ZooKeeper, so they will each inherit from this class
for basic zookeeper client functionality
"""
import uuid
import sys
from kazoo.client import KazooClient, KazooState
import logging
class ZookeeperClient:
    def __init__(self, zookeeper_hosts=["127.0.0.1:2181"],verbose=False, use_logger=False):
        try:
            self.zk_hosts = ','.join(zookeeper_hosts)
        except TypeError:
            self.error(zookeeper_hosts)
            sys.exit(1)
        # ZooKeeper client -> self.zk
        self.zk = None
        self.zk_instance_id = str(uuid.uuid4())
        self.verbose = verbose
        if use_logger:
            self.set_logger()

    def clear_zookeeper(self):
        for znode in ['/shared_state','/topics','/primaries']:
            self.delete_znode(znode_name=znode, recursive=True)

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)

    def set_logger(self):
        self.prefix = {'prefix': f'ZKCLI'}
        self.logger = logging.getLogger(f'ZKCLI')
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def listener4state (self, state):
        if state == KazooState.LOST:
            self.debug ("Current state is now = LOST")
        elif state == KazooState.SUSPENDED:
            self.debug ("Current state is now = SUSPENDED")
        elif state == KazooState.CONNECTED:
            self.debug ("Current state is now = CONNECTED")
        else:
            self.debug ("Current state now = UNKNOWN !! Cannot happen")

    def connect_zk(self):
        success = False
        try:
            self.debug(f"Try to connect with ZooKeeper server: hosts = {self.zk_hosts}")
            self.zk = KazooClient(self.zk_hosts)
            self.zk.add_listener (self.listener4state)
            self.debug(f"ZooKeeper Current Status = {self.zk.state}")
            success = True
        except:
            self.debug("Issues with ZooKeeper, cannot connect with Server")
        return success

    def start_session(self):
        """ Start a Zookeeper Session """
        success = False
        try:
            self.zk.start()
            success = True
        except:
            self.debug(f"Exception thrown in start (): {sys.exc_info()[0]}")
        return success

    def stop_session(self):
        """ Stop a ZooKeeper Session """
        success = False
        try:
            self.zk.stop()
            success = True
        except:
            self.error(f"Exception thrown in stop (): {sys.exc_info()[0]}")
        return success

    def close_connection(self):
        success = False
        try:
            # now disconnect from the server
            self.zk.close()
            success = True
        except:
            self.error(f"Exception thrown in close (): {sys.exc_info()[0]}")
        return success

    def get_znode_value (self, znode_name=""):
        try:
            self.debug (f"Checking if Znode {znode_name} exists")
            if self.zk.exists (znode_name):
                value, stat = self.zk.get (znode_name)
                # ip, pub_reg_port, sub_reg_port
                znode_value = value.decode("utf-8")
                self.debug(
                    f"Details of znode {znode_name}: value = {value}, "
                    f"stat = {stat}"
                    )
                response = znode_value
            else:
                self.debug (f"{znode_name} znode does not exist")
                response = None
        except Exception as e:
            self.error(f"Exception thrown checking for exists/get: {sys.exc_info()[0]}")
            response = f"Error: {str(e)}"
        return response

    def create_znode(self, znode_name=None, znode_value=None, ephemeral=False):
        """ Create a znode with name = znode_name and value = either znode_value or
        znode_value. Used by the broker specifically.  """
        success = False
        try:
            if self.zk.exists(znode_name):
                self.debug(f'znode {znode_name} already exists')
            else:
                self.debug(f'Creating znode {znode_name} with value {znode_value}')
                value = str(znode_value).encode('utf-8')
                self.zk.create(znode_name, value=value, ephemeral=ephemeral)
            success = True
        except Exception as e:
            self.error(str(e))
            self.error(f"Exception thrown in create (): {sys.exc_info()[0]}")
        return success

    def znode_exists(self, znode_name=None):
        return self.zk.exists(znode_name)

    def delete_znode(self, znode_name=None, recursive=False):
        success = False
        try:
            self.zk.delete(znode_name, recursive=recursive)
            success = True
        except Exception as e:
            self.error(str(e))
        return success

    def modify_znode_value(self, znode_name=None, znode_value=None):
        """ Modify a znode value
        Args:
        new_val (str): new value to set on the /broker znode """
        value = None
        try:
            # Now let us change the data value on the znode and see if
            # our watch gets invoked
            self.debug(f"Setting a new value = {znode_value} on znode {znode_name}")
            if self.zk.exists (znode_name):
                self.debug(f"{znode_name} znode exists; setting a new value on it")
                znode_value = str(znode_value).encode('utf-8')
                self.zk.set(znode_name, znode_value)
                # Now see if the value was changed
                value, stat = self.zk.get(znode_name)
                self.debug(f"New value at znode {znode_name}: value = {value}, stat = {stat}")
                value = value.decode("utf-8")
            else:
                self.debug(f"{znode_name} znode does not exist")
        except Exception as e:
            self.error(f"Exception thrown checking for exists/set: {sys.exc_info()[0]}")
            value = str(e)
        return value

    def debug(self, msg):
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        self.logger.error(msg, extra=self.prefix)
