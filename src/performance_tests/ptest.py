from mininet.net import Mininet
from mininet.topolib import TreeTopo
from .topologies.single_switch_topology import SingleSwitchTopo
import os
import time
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

class PerformanceTest:

    def __init__(self, num_events=50, event_interval=0.3, wait_factor=10):
        self.num_events = num_events
        self.event_interval = event_interval
        # Sleep self.wait_factor times longer than num events * event interval
        # to allow the network to initialize and all events to run.
        # Files not written by subscriber until it reaches num_events.
        self.wait_factor = wait_factor
        self.prefix = {'prefix': ''}
        self.successes = 0
        self.failures = 0
        self.comments = []
        self.WAIT_FOR_ZK_START = 15

    def cleanup(self):
        """ Method to run the shell command mn -c to clean up existing mininet networks/resources
        before creating a new one """
        os.system("mn -c")

    def debug(self, msg):
        """ Debug method with custom prefix for class """
        self.logger.debug(msg, extra=self.prefix)

    def info(self, msg):
        """ Info method with custom prefix for class """
        self.logger.info(msg, extra=self.prefix)

    def error(self, msg):
        """ Error method with custom prefix for class """
        self.logger.error(msg, extra=self.prefix)

    def setWaitFactor(self, factor):
        """ Method to update the wait factor (to wait <factor> times as long
        as num events * event interval for pub sub to generate data """
        self.wait_factor = factor

    def setup_backup_pool(self, network, log_folder):
        backup_pool_command = (
            f'python3 driver.py '
            f'--backup '
            f'--load_threshold 30 ' # auto provision new zone at 30 clients per zone
            f'--verbose --indefinite '
            f'-z {network.hosts[self.ZOOKEEPER_INDEX].IP()}:2181 &> '
            f'{log_folder}/backupPool.log &'
        )
        network.hosts[self.BACKUP_POOL_INDEX].cmd(backup_pool_command)
        time.sleep(3)
        return network.hosts[self.BACKUP_POOL_INDEX].IP()

    def setup_zookeeper_server(self, network, log_folder, zkStartWait):
        """ Make the first host in the network the zookeeper server. MUST be created first."""
        self.debug("Starting zookeeper service, just a moment...")
        zookeeper_start_command = (
            '/opt/zookeeper/bin/zkServer.sh start '
            f'&> {log_folder}/zkServerStart.log &'
        )
        network.hosts[self.ZOOKEEPER_INDEX].cmd(zookeeper_start_command)
        self.debug(f'Sleeping for {zkStartWait} seconds')
        time.sleep(zkStartWait)
        self.check_zookeeper_status(network, log_folder)
        return network.hosts[self.ZOOKEEPER_INDEX].IP()

    def check_zookeeper_status(self, network, log_folder):
        self.debug("checking zookeeper service status...")
        zookeeper_status_command = (
            '/opt/zookeeper/bin/zkServer.sh status '
            f'&> {log_folder}/zkServerStatus.log &'
        )
        network.hosts[self.ZOOKEEPER_INDEX].cmd(zookeeper_status_command)
        self.debug(f'Sleeping for 2 seconds')
        time.sleep(2)
        return network.hosts[self.ZOOKEEPER_INDEX].IP()

    def clear_zookeeper_nodes(self, network, log_folder):
        """ Method to clear out zookeeper data. Use localhost because the command will be run on the
        same host running zkServer.sh"""
        self.debug("Clearing zookeeper service, just a moment...")
        zookeeper_clear_command = (
            f'python3 driver.py --clear_zookeeper -z '
            f'127.0.0.1:2181 &> {log_folder}/zkServerClear.log &'
        )
        network.hosts[self.ZOOKEEPER_INDEX].cmd(zookeeper_clear_command)
        self.debug("Sleeping for 5 seconds")
        time.sleep(5)
        return network.hosts[self.ZOOKEEPER_INDEX].IP()

    def kill_zookeeper_server(self, network, log_folder):
        self.debug("Stopping zookeeper service, just a moment...")
        zookeeper_stop_command = (
            '/opt/zookeeper/bin/zkServer.sh stop '
            f'&>> {log_folder}/zkServerStop.log &'
        )
        network.hosts[self.ZOOKEEPER_INDEX].cmd(zookeeper_stop_command)
        time.sleep(3)
        self.check_zookeeper_status(network, log_folder)
        return network.hosts[self.ZOOKEEPER_INDEX].IP()

    def create_network(self, topo=None):
        """ Method to create a Mininet network with a provided topology;
        handles pre-cleanup if necessary """
        network = None
        if topo:
            try:
                network = Mininet(topo=topo)
            except Exception as e:
                os.system('mn -c')
                network = Mininet(topo=topo)
        return network

    def test_network(self, network=None, num_hosts=None, network_name=""):
        """ Interface method; add implementation in subclasses for
        centralized/decentralized performance testing
        args:
        network (Mininet object) - network to start and create hosts on
        num_hosts (int) - number of hosts in network
        network_name - alias of network, used to create folders for data/logs
        """
        pass

    def test_tree_topology(self, depth=2, fanout=2):
        """ Create and test Pub/Sub on a Tree topology with fanout^depth hosts,
        with one broker and an equal number of subscribers and publishers """
        tree = TreeTopo(depth=depth, fanout=fanout)
        network = self.create_network(topo=tree)
        results = self.test_network(
            network=network,
            network_name=f"tree-d{depth}f{fanout}-{fanout**depth}hosts",
            num_hosts=fanout ** depth
        )
        self.cleanup()
        return results

    def test_single_switch_topology(self, num_hosts=3):
        """ Create and test Pub/Sub on a Single Switch Topology mininet network
        with a variable number of subscribers and publishers """
        if num_hosts < 3:
            # Raise exception. You need at least one broker, one subscriber and one publisher.
            raise Exception("Topology must include at least 3 hosts")
        topo = SingleSwitchTopo(n=num_hosts)
        network = self.create_network(topo=topo)
        results = self.test_network(
            network=network,
            num_hosts=num_hosts,
            network_name=f"singleswitch-{num_hosts}-hosts"
        )
        self.cleanup()
        return results

    def data_file_written_successfully(self, filename):
        """
        Data files should have been produced with num_events + 1
        lines each, plus one \n blank line. If this is is true,
        then the Pub/Sub system worked. That's the only
        way the subscriber would be able to write the expected number of results.
        Return boolean
        """
        try:
            if os.path.exists(filename):
                # File exists, verify contents
                with open(filename, 'r') as f:
                    # first line should always be the same header
                    l = f.readline()
                    if l.strip() == 'publisher,topic,total_time_seconds':
                        # Good so far.
                        count = 0
                        for line in f.readlines():
                            if line.strip() != '':
                                count += 1
                        # Number of lines should be equal to the number of requested events.
                        comment = f'Line count in data file {filename}: {count}'
                        result = (count == self.num_events)
                    else:
                        comment = f'Header line: {l.strip()}'
                        result = False
            else:
                comment = f'File {filename} DNE'
                result = False
        except Exception as e:
            comment = str(e)
            result = False
        return (result, comment)