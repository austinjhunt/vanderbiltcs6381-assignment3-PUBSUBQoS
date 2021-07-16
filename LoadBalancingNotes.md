If we want to use the ratio of clients to brokers as the threshold for creating new replicas, then a broker cannot be responsible for triggering this because a broker does not know how many other brokers exist, it only knows how many subscribers/publishers have registered with itself. Can't use driver, because driver only ever creates one Sub / one Pub / one Broker for a given driver.py call. Does not maintain a count or a list of active brokers / clients.

## Idea.
When you create a primary replica broker, broker increments a /brokercount znode value. When you create a pub or sub, it increments a /brokerclientcount znode value. A separate LoadBalancer entity watches BOTH of these znodes for changes. When one changes, if (brokerclientcount / brokercount) ratio is equal to threshold and load is increasing, promote one of the backups. if (brokerclientcount / brokercount) ratio is equal to threshold and load is decreasing, demote one of the primaries to a backup and redistribute the load.

### What if a broker actually fails?
We already have it set up so that when a broker fails, the /broker znode changes, so the load balancer can also watch that znode and handle the promotion of another backup replica to a primary replica if an existing primary replica fails. The code can be similar to what's already used by the publisher/subscribers who are watching the /broker znode, except the action on change will be the backup-to-primary promotion implementation described below.

### How do we promote a broker backup replica to broker primary replica?
Perhaps load balancer can maintain a list of primary replicas and their zones, as well as a list of the available / promotable backup replicas. Then, when promoting, randomly choose a backup replica, and call a .promote() method (need to implement) on that replica to join it to system. Be sure to add the promoted backup to the list of primary replicas and remove it from the backup replicas list.

### How do we demote a primary broker replica to backup replica?
If load balancer is maintaining a list of primary replicas and their zones as well as a list of the promotable replicas, then randomly select a primary replica and call its demote() method (need to implement) to demote it to a primary replica. Don't kill, just stop its main event loop process. Remove it from the primary replicas list and add it to the backup replicas list. Once removed, also register the publishers and subscribers in that broker's now dissolved zone with one of the still-active primary broker replicas to redistribute the load.

### How should we define a "zone"?
Thinking of a zone as a subset of the pub/sub system for which a specific primary broker replica is responsible. When a new broker primary replica is added for additional load balancing, a new zone is created, the newly promoted broker becomes THE broker for that new zone, and the subsequent publishers and subscribers that join the system become part of that zone and register with that broker, until the next round of promotion. Or, on demotion of a given primary replica, the zone that replica is responsible for dissolves, and the contained publishers/subscribers in that dissolved zone get "re-assigned" to a random set of other active zones.


### How do we distinguish between increasing and decreasing load?
LoadBalancer can maintain an internal current_broker_count and current_broker_client_count state. When one of those znode values change, read it and if it's higher than current value, load is increasing, if it's less than current value, load is decreasing. Update internal counts on every watch change event.


<!--
## How do we handle promotion independently of the existing /broker leader election / fault tolerance znode?
This part is not necessary if load balancer randomly selects from list of backups and calls .promote() Create a /promoteelection znode on startup of system. Tell all of the BACKUP replicas (not active/promoted yet) to watch /promoteelection znode for changes. This znode will be changed by the LoadBalancer whenever it is handling a promotion. On change, each backup broker tries to create a /promoted znode. If successful, that is the broker that gets promoted to primary replica. Only one will be able to create it. Broker gets promoted, joins system and then deletes /promoted znode which is not watched (doesn't trigger anything) but will be used for next round of promotions. -->

