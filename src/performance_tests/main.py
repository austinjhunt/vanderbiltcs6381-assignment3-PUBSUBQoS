# Module to run tests / data collection for both centralized dissemination
# model and decentralized dissemination model in various network topologies

from .centralized import CentralizedPerformanceTest
from .decentralized import DecentralizedPerformanceTest
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

class TestDriver:
    def __init__(self):
        self.prefix = {'prefix': 'TestDriver - '}
        self.set_logger()

    def set_logger(self):
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(prefix)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger = logging.LoggerAdapter(self.logger, self.prefix)

    def check_success_rate(self, results=[]):
        """
        Determine percentage of tests that passed/failed globally.
        Args:
        results (list) - list of result dictionaries structured as {
                    'successes': int,
                    'failures': self.failures,
                    'comments': self.comments
                } where each dictionary represents the total successes and failures for a
                single pub/sub system (a single topology)
        """
        actual_successes = 0
        total_tests = 0
        for res in results:
            actual_successes += res['successes']
            total_tests += (res['successes'] + res['failures'])

        percent_success = round(actual_successes / total_tests, 3) * 100
        self.info(
            f'Out of {total_tests} total tests, {actual_successes} tests passed. '
            f'({percent_success}% success rate) '
            f'That is, {actual_successes} of the {total_tests} publish-subscribe '
            f'systems successfully produced the expected data files.')

    def debug(self,msg):
        self.logger.debug(msg,extra=self.prefix)

    def info(self, msg):
        self.logger.info(msg, extra=self.prefix)


    def run_centralized_tests(self):
        """ Run centralized dissemination model performance tests in various network topologies """

        centralized_perf_test = CentralizedPerformanceTest(
            num_events=100,
            event_interval=0.1,
            wait_factor=5
        )
        # Min = 4 hosts, Max = 256 hosts
        results = []
        for depth in range(2,5):
            for fanout in range(2,5):
                # Adjust the wait factor as number of hosts grows. fanout * 3 = (6->12)
                centralized_perf_test.setWaitFactor(factor=depth*fanout)
                result = centralized_perf_test.test_tree_topology(depth=depth, fanout=fanout)
                results.append(result)
        return results


    def run_decentralized_tests(self):
        """ Run decentralized dissemination model performance tests in various network topologies """

        decentralized_perf_test = DecentralizedPerformanceTest(
            num_events=100,
            event_interval=0.1,
            wait_factor=5
        )
        # Min = 4 hosts, Max = 256 hosts
        results = []
        for depth in range(2,5):
            for fanout in range(2,5):
                # Can't do 81 and 256 hosts. Too many for virtualbox to run.
                if fanout ** depth < 81:
                    # Adjust the wait factor as number of hosts grows. fanout * 3 = (6->12)
                    decentralized_perf_test.setWaitFactor(factor=depth*fanout)
                    result = decentralized_perf_test.test_tree_topology(depth=depth, fanout=fanout)
                    results.append(result)
        return results


    def generate_plots(self, model="centralized"):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

        if model == "global":
            # one is a global centralized dissemination boxplot
            # the other is a global decentralized dissemination boxplot
            for parent in ['centralized','decentralized']:
                combined = []
                parent_folder = os.path.join(__location__, f"data/{parent}")
                # all_data_files = glob.glob(parent_folder + '/*/*.csv')
                for network_folder in os.listdir(parent_folder):
                    num_hosts = int(network_folder.split('-')[-1].split('hosts')[0])
                    network_folder = os.path.join(__location__, f"data/{parent}/{network_folder}")
                    for f in os.listdir(network_folder):
                        fpath = os.path.join(__location__, f"{network_folder}/{f}")
                        data = pd.read_csv(fpath).assign(parent=parent,num_hosts=num_hosts)
                        combined.append(data)
                global_data = pd.concat(combined)
                plt.title(f'{parent} Latency w/ Respect to # Hosts')
                global_data.groupby('num_hosts').boxplot(column=['total_time_seconds'], figsize=(30,30))
                plot_folder = os.path.join(__location__, f"plots/{parent}")
                plot_file = f'{plot_folder}/latency.png'
                plt.savefig(plot_file)
                plt.clf()
                plt.close()
        else:
            data_parent_folder = os.path.join(__location__, f"data/{model}")
            for dir in os.listdir(data_parent_folder):
                plot_folder = os.path.join(__location__, f"plots/{model}/{dir}")
                try:
                    os.mkdir(plot_folder)
                except FileExistsError:
                    pass
                for file in os.listdir(f'{data_parent_folder}/{dir}'):
                    # Get the data for this specific pub/sub system from the
                    # received messages file written by the subscriber
                    data = pd.read_csv(f'{data_parent_folder}/{dir}/{file}')
                    # use the index of the row as x axis
                    data = data.reset_index()
                    # Generate multiline graph where each line represents a publisher
                    fig,ax = plt.subplots()
                    plt.ylabel('Latency (s)')
                    plt.title(f'{dir} - {file}')
                    for k, v in data.groupby('publisher'):
                        v.plot(x='index',y='total_time_seconds', ax=ax, label=k)
                    # Name of subscriber within this pub/sub system
                    subscriber_name = file.split('.')[0]
                    plot_file = f'{plot_folder}/{subscriber_name}-line.png'
                    plt.savefig(plot_file)
                    plt.clf()
                    # Generate boxplot for this subscriber. Don't group by publisher.
                    plt.title(f'{dir} - {file}')
                    data.boxplot(column=['total_time_seconds'])
                    plot_file = f'{plot_folder}/{subscriber_name}-box.png'
                    plt.savefig(plot_file)

                    plt.clf()
                    plt.close()
                    # plt.show()



if __name__ == "__main__":

    test_driver = TestDriver()
    # Run centralized dissemination tests
    centralized_results = test_driver.run_centralized_tests()
    # Run decentralized dissemination tests
    decentralized_results = test_driver.run_decentralized_tests()

    # Generate global plots for both centralized and decentralized
    test_driver.generate_plots(model="global")

    # Generate specific centralized/decentralized plots per subscriber
    test_driver.debug("Generating plots...")
    test_driver.generate_plots(model="centralized")
    test_driver.generate_plots(model="decentralized")

    test_driver.debug("Checking centralized results...")
    test_driver.check_success_rate(centralized_results)
    test_driver.debug("Checking decentralized results...")
    test_driver.check_success_rate(decentralized_results)
