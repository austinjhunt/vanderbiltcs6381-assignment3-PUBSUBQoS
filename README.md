


## Classes

### Subscriber
Class representing a subscriber, where one or many subscribers can be created on a host. A subscriber requires a list of publisher addresses to which it can connect, and a list of topics which it should subscribe to / "listen for" on those connections. The subscriber class is **indifferent** to the information dissemination method, in that the list of publishers it "knows" could be a **single intermediate message broker** that anonymizes the publishers, **or** it could be an explicit list of **publisher IP addresses** that were created **before** the subscriber. The subscriber class provides methods for **ad-hoc additions** of new publisher connections if new publishers are added to the topology after it is initialized, and it also allows easy disconnection from either **all publishers** or **specific publishers**.