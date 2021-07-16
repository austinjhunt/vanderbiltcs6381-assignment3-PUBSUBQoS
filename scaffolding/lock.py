import os
import sys
import time
import uuid

# argument parser
import argparse

# Now import the kazoo package that supports Python binding
# to ZooKeeper
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

class ZK_Driver ():
    """ The ZooKeeper Driver Class """

    #################################################################
    # constructor
    #################################################################
    def __init__ (self, args):
        self.zk = None  # session handle to the zookeeper server
        self.zkIPAddr = args.zkIPAddr  # ZK server IP address
        self.zkPort = args.zkPort # ZK server port num
        self.zkName = args.zkName # refers to the znode path being manipulated
        self.zkVal = args.zkVal # refers to the znode value
        self.instanceId = str(uuid.uuid4())
        self.zNode = self.zkName + '/' + self.instanceId
        self.election = None

    #-----------------------------------------------------------------------
    # Debugging: Dump the contents

    def dump (self):
        """dump contents"""
        print ("=================================")
        print ("Server IP: {}, Port: {}; Path = {} and Val = {}".format (self.zkIPAddr, self.zkPort, self.zkName, self.zkVal))
        print ("=================================")

    # -----------------------------------------------------------------------
    # Initialize the driver
    # -----------------------------------------------------------------------
    def init_driver (self):
        """Initialize the client driver program"""

        try:
            # debug output
            self.dump ()

            # instantiate a zookeeper client object
            # right now only one host; it could be the ensemble
            hosts = self.zkIPAddr + str (":") + str (self.zkPort)
            print ("Driver::init_driver -- instantiate zk obj: hosts = {}".format(hosts))

            # instantiate the kazoo client object
            self.zk = KazooClient (hosts)

            # register it with the state listener.
            # recall that the "listener4state" is a callback method
            # we defined above and so we are just passing the pointer
            # to this callback to the listener method on kazoo client.
            self.zk.add_listener (listener4state)
            print ("Driver::init_driver -- state after connect = {}".format (self.zk.state))

        except:
            print ("Unexpected error in init_driver:", sys.exc_info()[0])
            raise


    # -----------------------------------------------------------------------
    # start a session with the zookeeper server
    #
    def start_session (self):
        """ Starting a Session """
        try:
            # now connect to the server
            self.zk.start ()

        except:
            print("Exception thrown in start (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # stop a session with the zookeeper server
    #
    def stop_session (self):
        """ Stopping a Session """
        try:
            #
            # now disconnect from the server
            self.zk.stop ()

        except:
            print("Exception thrown in stop (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # create a znode
    #
    def create_znode (self):
        """ ******************* znode creation ************************ """
        try:
            # here we create a node just like we did via the CLI. But here we are
            # also showcasing the ephemeral attribute which means that the znode
            # will be deleted automatically by the server when the session is
            # terminated by this client. The "makepath=True" parameter ensures that
            # the znode will first be created and then a value attached to it.
            #
            # Note that we do not check here if the node already exists. If it does,
            # then we will get an exception
            print ("Creating an ephemeral znode {} with value {}".format(self.zkName, self.instanceId))
            # self.zk.ensure_path(self.zkName)
            self.zk.create (self.zkName, value=self.instanceId.encode('utf-8'), ephemeral=True)

        except:
            print("Exception thrown in create (): ", sys.exc_info()[0])
            return


    # -----------------------------------------------------------------------
    # Retrieve the value stored at a znode
    def get_znode_value (self):

        """ ******************* retrieve a znode value  ************************ """
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            print ("Checking if {} exists (it better be)".format(self.zNode))
            if self.zk.exists (self.zNode):
                print ("{} znode indeed exists; get value".format(self.zNode))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get (self.zNode)
                print(("Details of znode {}: value = {}, stat = {}".format (self.zNode, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(self.zkName))

        except:
            print("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # Modify the value stored at a znode
    def modify_znode_value (self, new_val):

        """ ******************* modify a znode value  ************************ """
        try:
            # Now let us change the data value on the znode and see if
            # our watch gets invoked
            print ("Setting a new value = {} on znode {}".format (new_val, self.zkName))

            # make sure that the znode exists before we actually try setting a new value
            if self.zk.exists (self.zkName):
                print ("{} znode still exists :-)".format(self.zkName))

                print ("Setting a new value on znode")
                self.zk.set (self.zkName, new_val)

                # Now see if the value was changed
                value,stat = self.zk.get (self.zkName)
                print(("New value at znode {}: value = {}, stat = {}".format (self.zkName, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(self.zkName))

        except:
            print("Exception thrown checking for exists/set: ", sys.exc_info()[0])
            return

    def leader_function(self):
        print("I am the leader {}".format(str(self.instanceId)))
        print("Create a znode as the leader")
        # as a leader, I will create a znode if it does not exist
        # or I will modify the znode if it exists
        if self.zk.exists(self.zkName):
            print("{} znode exists : only modify the value)".format(self.zkName))
            self.modify_znode_value(self.instanceId.encode('utf-8'))
        else:
            print("{} znode does not exists : create one)".format(self.zkName))
            self.create_znode()

        # hold the leader for sometime
        for i in range(10):
            print("{} is working! ".format(self.instanceId))
            time.sleep(3)
        print("I am done and leaving")


    def run_election(self):
        self.election = self.zk.Election("/electionpath", self.instanceId)
        print(self.election.contenders())
        self.election.run(self.leader_function)
        # try:
        #     self.election = self.zk.Election("/electionpath", self.instanceId)
        #     self.election.run(leader_function)
        # except:
        #     print("Exception thrown: ", sys.exc_info()[0])

    def test_lock(self):
        """
def _getImageLock(self, image, blocking=True, timeout=None):
    # If we don't already have a znode for this image, create it.
    image_lock = self._imageLockPath(image)
    try:
        self.client.ensure_path(self._imagePath(image))
        self._current_lock = Lock(self.client, image_lock)
        have_lock = self._current_lock.acquire(blocking, timeout)
    except kze.LockTimeout:
        raise npe.TimeoutException(
            "Timeout trying to acquire lock %s" % image_lock)

    # If we aren't blocking, it's possible we didn't get the lock
    # because someone else has it.
    if not have_lock:
        raise npe.ZKLockException("Did not get lock on %s" % image_lock)

        """
        self.zk.ensure_path("/topic")
        for i in range(5):
            self.lock = self.zk.Lock("/topic", self.instanceId)
            self.have_lock = self.lock.acquire(blocking=False)
            self.contenders = self.lock.contenders()
            print(self.have_lock)
            print(self.contenders)
            if self.have_lock:
                break
            else:
                input ("Test Lock -- Press any key to continue to have another try")


    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # Run the driver
    #
    # We do a whole bunch of things to demonstrate the use of ZooKeeper
    # Note that as you are trying this out, use the ZooKeeper CLI to verify
    # that indeed these things are happening on the server (just as a validation)
    # -----------------------------------------------------------------------
    def run_driver (self):
        """The actual logic of the driver program """
        # print ("\n")
        # input ("Starting Session with the ZooKeeper Server -- Press any key to continue")
        # self.start_session ()
        # print ("\n")
        # input ("Election -- Press any key to continue")
        # self.run_election()

        try:
            # first step is to start a session
            print ("\n")
            input ("Starting Session with the ZooKeeper Server -- Press any key to continue")
            self.start_session ()

            # # run the election
            # print ("\n")
            # input ("Election -- Press any key to continue")
            # self.run_election ()

            # test lock
            print ("\n")
            input ("Test Lock -- Press any key to continue")
            self.test_lock()

            # print ("\n")
            # input ("Creating a znode -- Press any key to continue:")
            # self.create_znode ()

            # now let us disconnect. Doing so should delete our znode because
            # it is ephemeral
            print ("\n")
            input ("Disconnect from the server -- Press any key to continue")
            self.stop_session ()

            # start another session to see if the node magically comes back up
            print ("\n")
            input ("Starting new Session to the ZooKeeper Server -- Press any key to continue")
            self.start_session ()

            # # now check if the znode still exists
            # print ("\n")
            # input ("check if the node still exists -- Press any key to continue")
            # if self.zk.exists (self.zkName):
            #     print ("{} znode still exists -- not possible".format (self.zkName))
            # else:
            #     print ("{} znode no longer exists as expected".format (self.zkName))
            #
            # # disconnect once again
            # print ("\n")
            # input ("Disconnecting for the final time -- Press any key to continue")
            # self.stop_session ()

            # cleanup
            print ("\n")
            input ("Cleaning up the handle -- Press any key to continue")
            self.zk.close ()

        except:
            print("Exception thrown: ", sys.exc_info()[0])


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-a", "--zkIPAddr", default="127.0.0.1", help="ZooKeeper server ip address, default 127.0.0.1")
    parser.add_argument ("-p", "--zkPort", type=int, default=2181, help="ZooKeeper server port, default 2181")
    parser.add_argument ("-n", "--zkName", default="/broker", help="ZooKeeper znode name, default /foo")
    parser.add_argument ("-v", "--zkVal", default="bar", help="ZooKeeper znode value at that node, default 'bar'")

    # parse the args
    args = parser.parse_args ()

    return args

#*****************************************************************
# main function
def main ():
    """ Main program """

    print ("Demo program for ZooKeeper")
    parsed_args = parseCmdLineArgs ()

    #
    # invoke the driver program
    driver = ZK_Driver (parsed_args)

    # initialize the driver
    driver.init_driver ()

    # start the driver
    driver.run_driver ()

#----------------------------------------------
if __name__ == '__main__':
    main ()


# my_id = uuid.uuid4()
#
# def leader_func():
#     print("I am the leader {}".format(str(my_id)))
#     for i in range(10):
#         print("{} is working! ".format(str(my_id)))
#         time.sleep(3)
#
# zk = KazooClient(hosts='127.0.0.1:2181')
# zk.start()
#
# election = zk.Election("/electionpath", my_id)
# print(election.contenders())
# # blocks until the election is won, then calls
# # leader_func()
# election.run(leader_func)
# print("I am leaving")
