# Module to run tests / data collection for both centralized dissemination
# model and decentralized dissemination model in various network topologies

from .centralized import CentralizedPerformanceTest
from .decentralized import DecentralizedPerformanceTest

def run_centralized_tests():
    """ Run centralized dissemination model performance tests in various network topologies """
    centralized_perf_test = CentralizedPerformanceTest(
        num_events=100,
        event_interval=0.1,
        wait_factor=5
    )

    # Min = 4 hosts, Max = 256 hosts
    for depth in range(2,5):
        for fanout in range(2,5):
            # Adjust the wait factor as number of hosts grows. fanout * 3 = (6->12)
            centralized_perf_test.setWaitFactor(factor=depth*fanout)
            centralized_perf_test.test_tree_topology(depth=depth, fanout=fanout)

def run_decentralized_tests():
    """ Run decentralized dissemination model performance tests in various network topologies """
    decentralized_perf_test = DecentralizedPerformanceTest(
        num_events=100,
        event_interval=0.1,
        wait_factor=5
    )

    # Min = 4 hosts, Max = 256 hosts
    for depth in range(2,5):
        for fanout in range(2,5):
            # Adjust the wait factor as number of hosts grows. fanout * 3 = (6->12)
            decentralized_perf_test.setWaitFactor(factor=depth*fanout)
            decentralized_perf_test.test_tree_topology(depth=depth, fanout=fanout)


if __name__ == "__main__":

    # Run centralized dissemination tests
    run_centralized_tests()
    # Run decentralized dissemination tests
    run_decentralized_tests()