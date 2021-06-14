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

    def set_logger(self):
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger = logging.LoggerAdapter(self.logger, self.prefix)



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

            # Start the network
            network.start()
            # self.debug("Starting a pingAll test...")
            # network.pingAll()
            self.debug("Starting network...")
            num_subscribers = (num_hosts - 1) // 2
            num_publishers = num_hosts - 1 - num_subscribers

            self.debug(
                f'With {num_hosts} hosts, there will be 1 Broker, '
                f'{num_publishers} Publishers, and {num_subscribers} Subscribers...'
            )


            # Set up broker on first host (h1)
            broker_command = (
                f'python3 driver.py '
                '--broker 1 --verbose '
                f'--indefinite ' # max event count only matters for subscribers who write files at end.
                f'--centralized ' # CENTRALIZED TESTING
                f'&> {log_folder}/broker.log &'
            )
            broker_host = network.hosts[0]
            broker_host.cmd(broker_command)
            broker_ip = broker_host.IP()
            self.debug(f'Broker set up! (IP: {broker_ip})')

            subscribers = [
                network.hosts[i] for i in range(1, num_subscribers + 1)
            ]
            publishers = [
                network.hosts[i] for i in range(num_subscribers + 1, num_hosts)
            ]
            self.debug(f"Starting {num_subscribers} subscribers...")
            for index,host in enumerate(subscribers):
                host.cmd(
                    'python3 driver.py '
                    '--subscriber 1 '
                    '--topics A --topics B --topics C '
                    f'--max_event_count {self.num_events} '
                    f'--broker_address {broker_ip} '
                    f'--filename {data_folder}/subscriber-{index}.csv '
                    '--centralized ' # CENTRALIZED TESTING
                    f'--verbose &> {log_folder}/sub-{index}.log &'
                )
            self.debug("Subscribers created!")

            self.debug(f"Creating {num_publishers} publishers...")
            for index,host in enumerate(publishers):
                host.cmd(
                    f'python3 driver.py '
                    '--publisher 1 '
                    f'--sleep {self.event_interval} '
                    f'--max_event_count {self.num_events} '
                    f'--indefinite ' # max event count only matters for subscribers who write files at end.
                    '--topics A --topics B --topics C '
                    f'--broker_address {broker_ip} '
                    f'--verbose  &> {log_folder}/pub-{index}.log &'
                    )
            self.debug("Publishers created!")

            # Scale the wait time by a constant factor and with number of hosts
            wait_time = self.wait_factor * (self.num_events * self.event_interval)
            self.debug(f"Waiting for {wait_time} seconds for data to generate...")
            time.sleep(wait_time)
            self.debug(f"Finished waiting!")

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
