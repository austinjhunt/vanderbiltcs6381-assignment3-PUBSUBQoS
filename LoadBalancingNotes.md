If we want to use the ratio of clients to brokers as the threshold for creating new replicas, then a broker cannot be responsible for triggering this because a broker does not know how many other brokers exist, it only knows how many subscribers/publishers have registered with itself. Can't use driver, because driver only ever creates one Sub / one Pub / one Broker for a given driver.py call. Does not maintain a count or a list of active brokers / clients.

## Idea.
When you create a primary replica broker, broker increments a `/counts/primary_replica_count` znode value. When you create a pub or sub, it increments a `/counts/broker_client_count` znode value. A separate LoadBalancer entity watches BOTH of these znodes for changes to update its own respective counts. When the broker_client_count znode changes, if (broker_client_count / brokercount) ratio is equal to threshold and load is increasing, promote one of the backups. if (broker_client_count / primary_replica_count) ratio is equal to threshold and load is decreasing, demote one of the primaries to a backup and redistribute the load.

### What if a primary broker actually fails?
We already have it set up so that when a broker fails, the /broker znode changes. We should extend the same leader election logic to each zone, where our new structure would look like /leaders/zone_<zoneNumber>. Then,
1. the load balancer can watch those znodes for changes and handle the promotion of another backup replica to a primary replica for that zone if the existing one fails. The code can be similar to what's already used by the publisher/subscribers who are watching the /broker znode, except the action on change will be the backup-to-primary promotion implementation described below.
2. Each broker will get a zone number when created, and in the leader function, the broker will either update or create a `/leaders/zone_<zoneNumber>` znode.
3. Each publisher and subscriber will get a zone number when created and will watch the `/leaders/zone_<zoneNumber>` znode for changes instead of the original "/broker" znode that did not account for zones.
### How do we promote a broker backup replica to broker primary replica?
The load balancer class can maintain a list of primary replicas and their zones, as well as a list of the available / promotable backup replicas. Then, when promoting, randomly choose a backup replica, and call a .promote() method (need to implement) on that replica to join it to system. Be sure to add the promoted backup to the list of primary replicas and remove it from the backup replicas list. The promote() method of a broker will set its zone number assigned by the load balancer, and then will check the corresponding `/leaders/zone_<zoneNumber>` to become the leader of that zone. Also, it will be necessary to pass in the client (publisher or subscriber) that triggered the promotion into the new zone's "publishers" or "subscribers" list, stored in the LB.

### How do we demote a primary broker replica to backup replica?
First check if you can demote a broker - you can only demote if the post-demotion clients_per_broker ratio is still less than or equal to the threshold.
If you can demote one, select the zone with the lowest clients_per_broker ratio and dissolve that zone. Each zone has a "clients_per_ratio" attribute that the load balancer maintains for it for quick access. Stop the main event loop of the primary broker for that selected zone. Remove the primary broker from the primary brokers list, add it to the backups list.
NOTE: it is a guarantee that you will only be able to demote if the current zone only has one remaining client. So, you don't need to redistribute the load of this zone to the other zones, because this zone is now empty if you are demoting. Just delete the zone.
Other note; since we know a demotion is only triggered by decreasing the load/removing a client, then use the client being removed to identify which zone to dissolve instead of filtering by min clients_per_broker ratio.

### How should we define a "zone"?
Thinking of a zone as a subset of the pub/sub system for which a specific primary broker replica is responsible. When a new broker primary replica is added for additional load balancing, a new zone is created, the newly promoted broker becomes THE broker for that new zone, and the subsequent publishers and subscribers that join the system become part of that zone and register with that broker, until the next round of promotion. Or, on demotion of a given primary replica, the zone that replica is responsible for dissolves, and the contained publishers/subscribers in that dissolved zone get "re-assigned" to a random set of other active zones.


### How do we distinguish between increasing and decreasing load?
LoadBalancer can maintain an internal current_broker_count and current_broker_client_count state. When current_broker_client_count znode changes, read it and if it's higher than current internal current_broker_client_count value, load is increasing. if it's less than current value, load is decreasing. Update internal counts on every watch change event.


<!--
## How do we handle promotion independently of the existing /broker leader election / fault tolerance znode?
This part is not necessary if load balancer randomly selects from list of backups and calls .promote() Create a /promoteelection znode on startup of system. Tell all of the BACKUP replicas (not active/promoted yet) to watch /promoteelection znode for changes. This znode will be changed by the LoadBalancer whenever it is handling a promotion. On change, each backup broker tries to create a /promoted znode. If successful, that is the broker that gets promoted to primary replica. Only one will be able to create it. Broker gets promoted, joins system and then deletes /promoted znode which is not watched (doesn't trigger anything) but will be used for next round of promotions. -->

