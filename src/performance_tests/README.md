# [Performance Tests](main.py)
The [main](main.py) module runs both centralized and decentralized performance tests. To run the main performance tests module, follow the steps below:
1. **You should execute these steps within the Ubuntu VM that you set up with the main README instructions. Make sure there is not already a Zookeeper Service Running in your VM. If there is, stop that service with `/opt/zookeeper/bin/zkServer.sh stop`**
2. `cd` into the `src` directory
3. Run the following command:
   1. `python3 -m performance_tests.main`
   2. Wait for a while for the tests to run and write their data to their respective folders.
   3. These tests will generate, for **each individual Pub/Sub system** that it spins up:
      1. Data files (CSV) written by each subscriber (to `data/[centralized/decentralized]/[network name]/subscriber-<index>.csv`) in the system containing: `<publisher who sent message>,<topic of message>,<latency for message>`
      2. Log files (.log) written by each entity (including broker, publishers, and subscribers) in the system during execution (to `logs/[centralized/decentralized]/[network name]/`)
      3. Test Result Files (`test_results/[centralized,decentralized]/[network name].csv`) indicating how many tests passed/failed, where each test is **a check to ensure that the pub sub system generated the expected data files**. Each pub sub system with N subscribers should have N passing tests, since each subscriber must write a data file. If and only if the publish subscribe system works successfully, each subscriber in the system **will** write their messages to a file.
