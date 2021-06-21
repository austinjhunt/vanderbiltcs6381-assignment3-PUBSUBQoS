# Quick Test Steps (for simpler Peer Review)
The following is a list of steps you can take to perform two quick tests on the framework without dealing with all of the automation. The two tests are respectively for the centralized message dissemination (where the broker forwards all messages), and for decentralized message dissemination (where pubs and subs are in direct contact).

## These commands have been tested in MacOS and on an Ubuntu 20.04 VM. Each command can be executed in its own terminal window alongside other terminal windows.

### Steps for Centralized Testing
1. Navigate to the src directory of the project.
`cd src/`
2. Create a centralized broker
`python3 driver.py --broker 1 --centralized --verbose --indefinite`
3. Create a publisher of topics A and B (publisher doesn't care about centralized or not; everyone is a subscriber from its perspective) who sleeps 0.3 seconds between each publish event.
`python3 driver.py --publisher 1 --topics A --topics B --sleep 0.3 --verbose --broker_address 127.0.0.1 --indefinite`
4. Create a subscriber of topic A (cares about centralized or not)
`python3 driver.py --subscriber 1 --topics A --verbose --broker_address 127.0.0.1 --centralized --indefinite`

### Steps for Decentralized Testing
1. Cd into src directory of project
`cd src/`
2. Create a decentralized broker
`python3 driver.py --broker 1 --verbose --indefinite`
3. Create a publisher of topics A and B (doesn't care about centralized or not) who sleeps 0.3 seconds between each publish event.
`python3 driver.py --publisher 1 --topics A --topics B --sleep 0.3 --verbose --broker_address 127.0.0.1 --indefinite`
4. Create a subscriber of topic A (cares about centralized or not)
`python3 driver.py --subscriber 1 --topics A --verbose --broker_address 127.0.0.1 --indefinite`