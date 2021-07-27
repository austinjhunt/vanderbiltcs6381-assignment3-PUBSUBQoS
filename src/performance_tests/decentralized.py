from .ptest import PerformanceTest
import time
import os
import logging
import sys
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
class DecentralizedPerformanceTest(PerformanceTest):

    def __init__(self, num_events=50, event_interval=0.3, wait_factor=10):
        super().__init__(num_events=num_events,
            event_interval=event_interval,wait_factor=wait_factor)
        self.set_logger()
        # 2 brokers, 1 zookeeper server. THEN pubs/subs.
        self.OFFSET = 4
        self.ZOOKEEPER_INDEX = 0
        self.BACKUP_POOL_INDEX = 1
        self.BROKER_1_INDEX = 2
        self.BROKER_2_INDEX = 3


    def set_logger(self):
        self.prefix = {'prefix': f'CENTRALTEST-'}
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def setup_brokers(self, log_folder, network, zookeeper_host):
        # Start 2 brokers with above command for multi-broker ZooKeeper test
        broker_command_1 = (
            f'python3 driver.py '
            '--broker 1 --verbose '
            f'--zookeeper_host {zookeeper_host} '
            '--zone 1 '
            f'--indefinite ' # max event count only matters for subscribers who write files at end.
            # Zookeeper test - autokill first broker after 15 seconds to allow second leader election
            f'--autokill 15'
            f'&> {log_folder}/broker1.log &'
        )
        broker_host_1 = network.hosts[self.BROKER_1_INDEX]
        broker_host_1.cmd(broker_command_1)
        broker_ip_1 = broker_host_1.IP()
        self.debug(f'Broker 1 set up! (IP: {broker_ip_1})')

        broker_command_2 = (
            f'python3 driver.py '
            '--broker 1 --verbose '
            '--zone 1 '
            f'--zookeeper_host {zookeeper_host} '
            f'--indefinite ' # max event count only matters for subscribers who write files at end.
            f'&> {log_folder}/broker2.log &'
        )
        broker_host_2 = network.hosts[self.BROKER_2_INDEX]
        broker_host_2.cmd(broker_command_2)
        broker_ip_2 = broker_host_2.IP()
        self.debug(f'Broker 2 set up! (IP: {broker_ip_2})')
        return broker_ip_1, broker_ip_2

    def setup_publishers(self, num_publishers, network, num_subscribers,
        num_hosts, broker_ip, log_folder, zookeeper_host):
        self.debug(f"Creating {num_publishers} publishers...")
        publishers = [
            network.hosts[i] for i in range(num_subscribers + self.OFFSET, num_hosts)
        ]
        for index,host in enumerate(publishers):
            host.cmd(
                f'python3 driver.py '
                '--publisher 1 '
                f'--sleep {self.event_interval} '
                f'--zookeeper_host {zookeeper_host} '
                f'--indefinite ' # max event count only matters for subscribers who write files at end.
                '--topics A --topics B --topics C '
                f'--broker_address {broker_ip} '
                f'--max_event_count {self.num_events} '
                f'--verbose  &> {log_folder}/pub-{index}.log &'
                )
        self.debug("Publishers created!")
        return publishers

    def setup_subscribers(self, num_subscribers, network, broker_ip, data_folder, log_folder,
        zookeeper_host):
        self.debug(f"Starting {num_subscribers} subscribers...")
        subscribers = [
            network.hosts[i] for i in range(self.OFFSET, num_subscribers + self.OFFSET)
        ]
        for index,host in enumerate(subscribers):
            host.cmd(
                'python3 driver.py '
                '--subscriber 1 '
                '--topics A --topics B --topics C '
                f'--zookeeper_host {zookeeper_host} '
                f'--max_event_count {self.num_events} '
                f'--broker_address {broker_ip} '
                f'--filename {data_folder}/subscriber-{index}.csv '
                f'--verbose &> {log_folder}/sub-{index}.log &'
            )
        self.debug("Subscribers created!")
        return subscribers

    def terminate_test(self, subscribers, publishers, network_name, network):
        self.debug("Killing processes...")
        for index,host in enumerate(subscribers):
            out = host.cmd(f'kill %1')

        for index,host in enumerate(publishers):
            out = host.cmd(f'kill %1')

        self.debug(f"Processes killed. Stopping network '{network_name}'...")
        # Stop the network
        network.stop()
        self.debug(f"Network '{network_name}' stopped!")

    def wait_for_execution(self):
        # Scale the wait time by a constant factor and with number of hosts
        wait_time = self.wait_factor * (self.num_events * self.event_interval)
        self.debug(f"Waiting for {wait_time} seconds for data to generate...")
        time.sleep(wait_time)
        self.debug(f"Finished waiting!")

    def verify_data_written(self, subscribers, data_folder, test_results_file):
        self.debug("Running unittest assertions to verify data was written...")
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

    def prepare_folder_structure(self, network_name):
        # For subs, write data files into a folder within this folder
        # named after the network
        data_folder = os.path.join(__location__, f"data/decentralized/{network_name}")
        log_folder = os.path.join(__location__, f"logs/decentralized/{network_name}")
        # File for pass/fail check results
        test_results_file = os.path.join(__location__, f"test_results/decentralized/{network_name}.csv")
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
        decentralized performance testing of pub/sub system in provided network
        args:
        network (Mininet object) - network to start and create hosts on
        num_hosts (int) - number of hosts in network
        network_name - alias of network, used to create folders for data/logs
        """
        self.prefix['prefix'] = f'DECENTRAL-NET-{network_name}-TEST - '
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

            # First host is zookeeper server.
            zookeeper_host = f'{self.setup_zookeeper_server(network, log_folder, self.WAIT_FOR_ZK_START)}:2181'
            backup_pool_server = self.setup_backup_pool(network, log_folder)
            broker_ip_1, broker_ip_2 = self.setup_brokers(log_folder, network, zookeeper_host)
            subscribers = self.setup_subscribers(num_subscribers, network,
                broker_ip_1, data_folder, log_folder, zookeeper_host)
            publishers = self.setup_publishers(num_publishers, network, num_subscribers,
                                num_hosts, broker_ip_1, log_folder,zookeeper_host)

            self.wait_for_execution()
            self.verify_data_written(subscribers, data_folder, test_results_file)
            self.clear_zookeeper_nodes(network, log_folder)
            self.kill_zookeeper_server(network, log_folder)
            self.terminate_test(subscribers, publishers, network_name, network)

            return {
                'successes': self.successes,
                'failures': self.failures,
                'comments': self.comments
            }
        else:
            self.logger.error("Need to pass network and num_hosts to initialize_network()")
            return {
                'successes': self.successes,
                'failures': self.failures,
                'comments': self.comments,
                'error': "Need to pass network and num_hosts to initialize_network()"
            }
