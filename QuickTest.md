# Quick, easy test
## Centralized Dissemination Testing
Open 6 terminal windows. Paste each of the following commands into its own terminal window and run the command. Be sure to create the broker (--broker) first.
```
python driver.py --broker 1 -v -a 127.0.0.1 --indefinite --centralized
```
```
python driver.py -sub 1 -t A -t B -t D --indefinite -b 127.0.0.1 -v -a 127.0.0.1 --centralized
```
```
python driver.py -sub 1 -t A -t Q -t M --indefinite -b 127.0.0.1 -v -a 127.0.0.1 --centralized
```
```
python driver.py -sub 1 -t R --indefinite -b 127.0.0.1 -v -a 127.0.0.1 --centralized
```
```
python driver.py --publisher 1 -t A -t Z -t R --indefinite --broker_address 127.0.0.1 -a 127.0.0.1 -v --centralized
```
```
python driver.py --publisher 1 -t C -t D -t E --indefinite --broker_address 127.0.0.1 -a 127.0.0.1 -v --centralized
```

## Decentralized Dissemination Testing
Open 6 terminal windows. Paste each of the following commands into its own terminal window and run the command. Be sure to create the broker (--broker) first.
```
python driver.py --broker 1 -v -a 127.0.0.1 --indefinite
```
```
python driver.py -sub 1 -t A -t B -t D --indefinite -b 127.0.0.1 -v -a 127.0.0.1
```
```
python driver.py -sub 1 -t A -t Q -t M --indefinite -b 127.0.0.1 -v -a 127.0.0.1
```
```
python driver.py -sub 1 -t R --indefinite -b 127.0.0.1 -v -a 127.0.0.1
```
```
python driver.py --publisher 1 -t A -t Z -t R --indefinite --broker_address 127.0.0.1 -a 127.0.0.1 -v
```
```
python driver.py --publisher 1 -t C -t D -t E --indefinite --broker_address 127.0.0.1 -a 127.0.0.1 -v
```