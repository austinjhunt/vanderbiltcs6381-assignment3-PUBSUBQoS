# Unit Tests

The unit tests defined in this directory (which use [unittest](https://docs.python.org/3/library/unittest.html)) are designed to test those methods of the Publisher, Subscriber, and Broker classes whose functionality can be tested **independently** of a full Publish/Subscribe system. This module does not test things like registration, message sending, and message receiving, since those methods depend on the Publish/Subscribe system as a whole (those would not be unit tests; those would be integration tests). This module tests some foundational basic units responsible for things like randomized port selection, zookeeper client configuration, publish event generation, and file writing.

The network communication and related performance are analyzed in the [performance_tests](../performance_tests/main.py) directory.

## Running the Tests
**You should execute these steps within the Ubuntu VM that you set up using the main project README instructions. You need to be running the ZooKeeper service in the VM for these tests to run. If that service is not running, execute `/opt/zookeeper/bin/zkServer.sh start`**
To run the unit tests:
1. `cd` into the root of the project
2. Run the command `python -m unittest discover` to automatically discover all tests and run them


