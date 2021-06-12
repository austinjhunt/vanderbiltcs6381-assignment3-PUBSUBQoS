# Unit Tests

The unit tests defined in this directory (which use [unittest](https://docs.python.org/3/library/unittest.html)) are designed to test those methods of the Publisher, Subscriber, and Broker classes whose functionality can be tested **independently** of a full Publish/Subscribe system. This module does not test things like registration, message sending, and message receiving, since those methods depend on the Publish/Subscribe system as a whole. This module tests some foundational basic units responsible for things like randomized port selection, host IP address determination, publish event generation, and file writing.

The network communication and related performance are analyzed in the [performance_tests](../performance_tests/main.py) directory.

## Running the Tests
To run the unit tests:
1. Make sure you've installed the project requirements. We recommend using a Python 3.8 virtual environment.
2. `cd` into the root of the project
3. Run the command `python -m unittest discover` to automatically discover all tests and run them
4. You can disable the verbose logging by changing DEBUG to another log level in the [unit_tests/__init__.py](__init__.py) file

