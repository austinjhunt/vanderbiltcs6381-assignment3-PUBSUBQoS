#!/bin/bash

# Start a network with a tree topology of depth 2 and fanout 4
# (i.e. 64 hosts connected to 9 switches), using Open vSwitch switches
sudo mn --switch ovs --topo tree,depth=2,fanout=4 # no test --test pingall

# 16 hosts total. Divide in half, 8 pubs and 8 subs.
h1 ./setup_publisher.sh -c -t T -i 1 -b 5556 -s 1 -d 1