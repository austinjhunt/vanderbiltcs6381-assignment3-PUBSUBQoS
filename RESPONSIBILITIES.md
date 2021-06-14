# Collaborator Responsibilities

This file was added to outline the responsibilities taken on by the project contributors in developing the framework. Note that the responsibilities often overlapped and were discussed over scheduled Zoom meetings in order for contributions to be merged smoothly.

## Both members
Experiment and familiarize ourselves with Mininet, both the installation and the execution within VirtualBox; create Pub/Sub/Broker specific to a given dissemination model, to be merged afterward into single classes with ability to pass in arguments that govern which model to use

## Guoliang Ding
Centralized dissemination, testing, plot generation and pattern finding

## Austin Hunt
Decentralized dissemination, automation of testing/plot generation

## Meetings
The following is an outline of the meetings we held as a team for the project.
- **May 13th, 7PM** - introductions, getting to know each other and our skillsets
- **June 2nd, 6PM** - planning work distribution for Assignment 1; decided it would be easiest to initially divide and conquer between the two main message dissemination models, then to merge afterward
- **June 5th, 7PM** - initial progress report, discussing questions we were running into regarding Mininet, namely the problem of ZMQ Python dependencies not working within Mininet hosts; was resolved by professor
- **June 8th, 6:30PM** - second progress report, discussing the use of the Python Mininet library for the automation of testing, as well as issues related to the use of that library; had to install mininet from source with PYTHON environment variable set for python3 in order to fix
- **June 11th, 7PM** - longer meeting where we worked on laying out test plans and a framework, namely choosing variables and constants to use within the testing, e.g. topologies, host numbers, topics, etc; initially manual tests, then automation was introduced

