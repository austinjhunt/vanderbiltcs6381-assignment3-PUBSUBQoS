from .ptest import PerformanceTest
import time
import os
import logging
import sys
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
class CentralizedPerformanceTest(PerformanceTest):

    def __init__(self, num_events=50, event_interval=0.3, wait_factor=10):
        super().__init__(num_events=num_events,
            event_interval=event_interval,wait_factor=wait_factor)
        self.set_logger()
        # 2 brokers, 1 zookeeper server. THEN pubs/subs.
        self.OFFSET = 3
        self.ZOOKEEPER_INDEX = 0
        self.BROKER_1_INDEX = 1
        self.BROKER_2_INDEX = 2
        self.WAIT_FOR_ZK_START = 5

    def set_logger(self):
        self.prefix = {'prefix': f'CENTRALTEST-'}
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def setup_brokers(self,network, log_folder, zookeeper_host):
        # Start 2 brokers with above command for multi-broker ZooKeeper test
        # Set up broker 1 on first host (h1)
        broker_command_1 = (
            f'python3 driver.py '
            '--broker 1 --verbose '
            f'--zookeeper_host {zookeeper_host} '
            f'--indefinite ' # max event count only matters for subscribers who write files at end.
            f'--centralized ' # CENTRALIZED TESTING
            # ZooKeeper test - autokill first broker after 15 seconds to allow second leader election
            f'--autokill 15'
            f'&> {log_folder}/broker1.log &'
        )
        broker_host_1 = network.hosts[self.BROKER_1_INDEX]
        broker_host_1.cmd(broker_command_1)
        broker_ip_1 = broker_host_1.IP()
        self.debug(f'Broker 1 set up! (IP: {broker_ip_1})')

        # Set up broker 2 on second host (h2)
        # Backup, not used by pubs/subs initially until broker 1 dies.
        broker_command_2 = (
            f'python3 driver.py '
            '--broker 1 --verbose '
            f'--zookeeper_host {zookeeper_host} '
            f'--indefinite ' # max event count only matters for subscribers who write files at end.
            f'--centralized ' # CENTRALIZED TESTING
            f'&> {log_folder}/broker2.log &'
        )
        broker_host_2 = network.hosts[self.BROKER_2_INDEX]
        broker_host_2.cmd(broker_command_2)
        broker_ip_2 = broker_host_2.IP()
        self.debug(f'Broker 2 set up (WAITING, WARM PASSIVE BACKUP)! (IP: {broker_ip_2})')
        return broker_ip_1, broker_ip_2

    def setup_subscribers(self, network, num_subscribers,
        broker_ip,data_folder,log_folder, zookeeper_host):
        """ Create subscribers in Mininet topology; one host per subscriber """
        subscribers = [
            network.hosts[i] for i in range(self.OFFSET, num_subscribers + self.OFFSET)
        ]
        self.debug(f"Starting {num_subscribers} subscribers...")
        for index,host in enumerate(subscribers):
            host.cmd(
                'python3 driver.py '
                '--subscriber 1 '
                f'--zookeeper_host {zookeeper_host} '
                '--topics A --topics B --topics C '
                f'--max_event_count {self.num_events} '
                f'--broker_address {broker_ip} '
                f'--filename {data_folder}/subscriber-{index}.csv '
                '--centralized ' # CENTRALIZED TESTING
                f'--verbose &> {log_folder}/sub-{index}.log &'
            )
        self.debug("Subscribers created!")
        return subscribers

    def setup_publishers(self,network, num_subscribers,
        num_hosts,num_publishers,broker_ip,log_folder, zookeeper_host):
        """ Create publishers in Mininet topology; one host per publisher"""
        publishers = [
            network.hosts[i] for i in range(num_subscribers + self.OFFSET, num_hosts)
        ]
        self.debug(f"Creating {num_publishers} publishers...")
        for index,host in enumerate(publishers):
            host.cmd(
                f'python3 driver.py '
                '--publisher 1 '
                f'--zookeeper_host {zookeeper_host} '
                f'--sleep {self.event_interval} '
                f'--max_event_count {self.num_events} '
                f'--indefinite ' # max event count only matters for subscribers who write files at end.
                '--topics A --topics B --topics C '
                f'--broker_address {broker_ip} '
                f'--verbose  &> {log_folder}/pub-{index}.log &'
                )
        self.debug("Publishers created!")
        return publishers

    def verify_data_written(self, subscribers, data_folder, test_results_file):
        for index,host in enumerate(subscribers):
            ## Run assertions here. Data files should have been produced with num_events + 1
            # lines each. If this is is true, then the Pub/Sub system worked. That's the only
            # way the subscriber would be able to write the expected number of results.
            check_result = self.data_file_written_successfully(
                    filename=f'{data_folder}/subscriber-{index}.csv'
                )
            self.comments.append(check_result[1])
            if not check_result[0]:
                self.failures += 1
            else:
                self.successes += 1

        self.debug(f"Writing pass/fail test results to {test_results_file}")
        with open(test_results_file, 'w') as f:
            f.write('pass,fail,comments\n')
            f.write(f'{self.successes},{self.failures},{",".join(self.comments)}')

    def wait_for_execution(self):
        # Scale the wait time by a constant factor and with number of hosts
        wait_time = self.wait_factor * (self.num_events * self.event_interval)
        self.debug(f"Waiting for {wait_time} seconds for data to generate...")
        time.sleep(wait_time)
        self.debug(f"Finished waiting!")
        self.debug("Verifying that expected data was written...")

    def terminate_test(self, subscribers, publishers, network_name, network):
        self.debug("Killing processes...")
        # Kill the processes
        for index,host in enumerate(subscribers):
            host.cmd(f'kill %1')
        for index,host in enumerate(publishers):
            host.cmd(f'kill %1')

        self.debug(f"Processes killed. Stopping network '{network_name}'...")
        # Stop the network
        network.stop()
        self.debug(f"Network '{network_name}' stopped!")

    def prepare_folder_structure(self, network_name):
        # For subs, write data files into a folder within this folder
        # named after the network
        data_folder = os.path.join(__location__, f"data/centralized/{network_name}")
        log_folder = os.path.join(__location__, f"logs/centralized/{network_name}")
        # File for pass/fail check results
        test_results_file = os.path.join(__location__, f"test_results/centralized/{network_name}.csv")
        # Make folders dynamically since the names are variable
        try:
            os.mkdir(data_folder)
        except FileExistsError:
            pass
        try:
            os.mkdir(log_folder)
        except FileExistsError:
            pass
        return data_folder, log_folder, test_results_file

    def test_network(self, network=None, num_hosts=None, network_name=""):
        """ Interface method from PerformanceTest superclass; implementation for
        centralized performance testing of pub/sub system in provided network
        args:
        network (Mininet object) - network to start and create hosts on
        num_hosts (int) - number of hosts in network
        network_name - alias of network, used to create folders for data/logs
        """
        self.prefix['prefix'] = f'CENTRAL-NET-{network_name}-TEST - '
        self.successes = 0
        self.failures = 0
        self.comments = []
        if network and num_hosts:
            data_folder, log_folder, test_results_file = self.prepare_folder_structure(network_name)

            self.debug("Starting network...")
            # Start the network
            network.start()
            num_subscribers = (num_hosts - self.OFFSET) // 2
            num_publishers = num_hosts - self.OFFSET - num_subscribers

            self.debug(
                f'With {num_hosts} hosts, there will be 2 Brokers, '
                f'{num_publishers} Publishers, and {num_subscribers} Subscribers...'
            )
            zookeeper_host = f'{self.setup_zookeeper_server(network, log_folder, self.WAIT_FOR_ZK_START)}:2181'
            broker_ip_1, broker_ip_2 = self.setup_brokers(network,log_folder, zookeeper_host)
            subscribers = self.setup_subscribers(network,num_subscribers,broker_ip_1,
                data_folder,log_folder, zookeeper_host
            )
            publishers = self.setup_publishers(network, num_subscribers, num_hosts,
                num_publishers, broker_ip_1, log_folder, zookeeper_host)

            self.wait_for_execution()
            self.verify_data_written(subscribers,data_folder,test_results_file)
            self.kill_zookeeper_server(network, log_folder)
            self.terminate_test(subscribers, publishers, network_name, network)

            return {
                'successes': self.successes,
                'failures': self.failures,
                'comments': self.comments
            }
        else:
            logging.error("Need to pass network and num_hosts to initialize_network()")
            return {
                'successes': self.successes,
                'failures': self.failures,
                'comments': self.comments,
                'error': "Need to pass network and num_hosts to initialize_network()"
            }
