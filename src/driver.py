import argparse
from json import load
import logging
from lib.zookeeper_client import ZookeeperClient
from lib.publisher import Publisher
from lib.subscriber import Subscriber
from lib.broker import Broker
from lib.loadbalancer import LoadBalancer

def create_publisher_with_zookeeper(publisher):
    """ Method to handle creation of publisher using zookeeper coordination"""
    publisher.connect_zk()
    publisher.start_session()
    publisher.assign_to_zone()
    publisher.update_broker_info(
        znode_value=publisher.get_znode_value(znode_name=publisher.broker_leader_znode)
        )
    publisher.watch_znode_data_change()
    # publisher.publish()
    publisher.publish_with_priority_control()
    # Will call if not running indefinitely
    publisher.disconnect()

def create_publisher_without_zookeeper(publisher):
    """ Method to handle creation of publisher without zookeeper coordination """
    publisher.configure()
    publisher.publish()
    # Will call if not running indefinitely
    publisher.disconnect()

def create_publishers(count=1, topics=[], broker_address='127.0.0.1',
    sleep_period=1, bind_port=5556, indefinite=False, max_event_count=15,
    zookeeper_hosts=['127.0.0.1:2181'],verbose=False, offered=1):
    """ Method to create a set of publishers.
    In order to run multiple subscribers simultaneously,
    need to use multiprocessing library, because Publisher.publish() will block for i in range(count)
    if run sequentially. E.g. publisher 2 on the same host will not ever get to publish updates
    if publisher 1 publishes indefinitely. Multiprocessing not yet implemented, so limit count to 1 for now

    EVENTUALLY: If you create more than one, first publisher will be bound to first port specified,
    next will bind to port+1, next to port+2, etc. No need to specify port for every publisher if
    creating multiple on one host."""

    pubs = {}
    for i in range(count):
        pubs[0] = Publisher(
            topics=topics,
            broker_address=broker_address,
            sleep_period=sleep_period,
            bind_port=bind_port + i,
            indefinite=indefinite,
            max_event_count=max_event_count,
            zookeeper_hosts=zookeeper_hosts,
            verbose=verbose,
            offered=offered
        )
        try:
            create_publisher_with_zookeeper(pubs[i])
            # create_publisher_without_zookeeper(pubs[i])
        except KeyboardInterrupt:
            # If you interrupt/cancel a publisher, be sure to disconnect properly
            # to tell broker it's no longer active
            pubs[i].disconnect()
    return pubs

def create_subscriber_with_zookeeper(subscriber):
    """ Method to handle creation of a subscriber using ZooKeeper coordination """
    subscriber.connect_zk()
    subscriber.start_session()
    subscriber.assign_to_zone()
    subscriber.update_broker_info(
        znode_value=subscriber.get_znode_value(znode_name=subscriber.broker_leader_znode)
    )
    subscriber.watch_znode_data_change()
    subscriber.notify()
    subscriber.write_stored_messages()
    # Will call if not running indefinitely
    subscriber.disconnect()

def create_subscriber_without_zookeeper(subscriber):
    """ Method to handle creating subscriber without zookeeper coordination"""
    subscriber.configure()
    subscriber.notify()
    # This will call if notify is not indefinite
    subscriber.disconnect()

def create_subscribers(count=1, filename=None, broker_address='127.0.0.1',
     centralized=False, topics=[], indefinite=False, max_event_count=15,
     zookeeper_hosts=['127.0.0.1:2181'],verbose=False,requested=1):
    """ Method to create a set of subscribers. In order to run multiple subscribers simultaneously,
    need to use multiprocessing library, because Subscriber.listen() will block for i in range(count)
    if run sequentially. E.g. subscriber 2 on the same host will not ever get to listen for updates
    if subscriber 1 listens indefinitely. Multiprocessing not yet implemented, so limit count to 1 for now."""
    subs = {}
    for i in range(count):
        subs[i] = Subscriber(
            topics=topics,
            filename=filename,
            broker_address=broker_address,
            centralized=centralized,
            indefinite=indefinite,
            max_event_count=max_event_count,
            zookeeper_hosts=zookeeper_hosts,
            verbose=verbose,
            requested=requested
        )
        try:
            create_subscriber_with_zookeeper(subs[i])
            # create_subscriber_without_zookeeper(subs[i])
        except KeyboardInterrupt:
            # If you interrupt/cancel a subscriber, be sure to disconnect properly
            # to tell broker it's no longer active
            subs[i].disconnect()

        # If filename provided (only works with finite notify() loop), write to file
        if filename:
            subs[i].write_stored_messages()
    return subs

def create_broker_with_zookeeper(broker):
    """ Method to handle creation of broker using zookeeper coordination
    Args:
    - broker object to create
    """
    broker.connect_zk()
    broker.start_session()
    broker.setup_fault_tolerance_znode()
    broker.setup_shared_state_znode()
    broker.setup_load_balancing_znode()
    broker.watch_shared_state_publishers()
    broker.watch_shared_state_subscribers()
    broker.zk_run_election()

def create_broker_without_zookeeper(broker):
    """" Method to handle creation of broker without zookeeper coordination """
    broker.configure()
    broker.event_loop()
    # Will call if broker event loop not indefinite
    broker.disconnect()

def create_brokers(indefinite=False, centralized=False, pub_reg_port=5555,
    sub_reg_port=5556, autokill=None, max_event_count=15, zookeeper_hosts=['127.0.0.1:2181'],
    verbose=False,primary=False,zone=1):

    broker = Broker(
        centralized=centralized,
        indefinite=indefinite,
        pub_reg_port=pub_reg_port,
        sub_reg_port=sub_reg_port,
        max_event_count=max_event_count,
        autokill=autokill,
        zookeeper_hosts=zookeeper_hosts,
        verbose=verbose,
        primary=primary,
        zone=zone
    )
    try:
        create_broker_with_zookeeper(broker)
        # create_broker_without_zookeeper(broker)
    except KeyboardInterrupt:
        # If you interrupt/cancel a broker, be sure to disconnect/clean all sockets
        broker.disconnect()

def create_load_balancer(backup_pool_size=5, load_threshold=3,verbose=False,
    zookeeper_hosts=['127.0.0.1:2181']):
    """ Create a load balancer to auto scale primary broker replicas with increased/decreased
    load, where load is quantified as clients (pubs + subs) per primary broker. Each primary
    broker has its own zone. Each zone must also stay within threshold. """
    # When creating a zone with a primary broker, go ahead and assign one backup to that same zone
    # so it (itself) can watch the leader election for that zone and jump in when necessary without the
    # load balancer.
    load_balancer = LoadBalancer(
        clients_per_primary_threshold=load_threshold,
        verbose=verbose,
        zookeeper_hosts=zookeeper_hosts
        )
    load_balancer.watch_primary_brokers_leave_join()
    load_balancer.watch_client_count_change()
    load_balancer.watch_primary_broker_count_change()
    load_balancer.balance_act()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Pass arguments to create publishers, subscribers, or an intermediate message broker')
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')

    # LOAD BALANCING
    parser.add_argument('-lb', '--load_balancer', action='store_true',
    help='create a load balancer to automatically scale primary broker replicas with increased/decreased load')

    parser.add_argument('-load', '--load_threshold', type=int,
        help=(
            'threshold of num_clients/num_brokers ratio at which to promote backup '
            'replicas to primary replicas for new load distribution'), default=3, required=False)
    parser.add_argument('-bs', '--backup_pool_size', type=int, default=5, required=False,
        help=(
            'how many broker replicas to include in the backup pool that are '
            'promotable to primary replicas when load thresholds are met')
    )

    # Choose type of entity
    parser.add_argument('-pub', '--publisher',  type=int,
        help='pass this followed by an integer N to create N publishers on this host')
    parser.add_argument('-sub', '--subscriber', type=int,
        help='pass this followed by an integer N to create N subscribers on this host')
    parser.add_argument('--broker', type=int,
        help='pass this followed by 1 to create 1 (max) broker on this host')

    ## new argument for ZooKeeper
    parser.add_argument('-z', '--zookeeper_hosts', action='append',
        help=('zookeeper hosts and the port. Typical are 127.0.0.1:2181 for localhosts'))

    ## For --subscriber; file to write stored messages to only if not using --indefinite
    parser.add_argument('-f', '--filename', type=str, help=(
        'optional filename to write stored subscriber messages to; '
        'only works with --subscriber if not using --indefinite'
    ))

    ## Required with --subscriber and --broker but not with --publisher because publisher is
    ## purely indifferent to dissemination method. To a publisher, everyone is a subscriber.
    parser.add_argument('-c', '--centralized', action='store_true', help=(
        'whether to use centralized message dissemination (broker anonymizes pub and sub); '
        'if not passed, will use direct message dissemination (subscriber connects directly to publisher)'
    ))
    # Required with --publisher and --subscriber
    parser.add_argument('-t', '--topics', action='append',
        help=('if creating a pub or sub, provide list of topics to either publish or subscribe to.'
        ' required if using -sub or -pub '))

    parser.add_argument('-m', '--max_event_count', type=int,
        help=(
            'if used with --sub, max num of published events to receive. '
            'if used with --pub, max number of events to publish. '
            'this only matters if --indefinite is not used'))
    parser.add_argument('-i', '--indefinite', action='store_true',
        help=(
            'if used with -pub, publish events indefinitely from created publisher(s). '
            'if used with -sub, receive published events indefinitely with created subscriber(s)'
        ))
    parser.add_argument('-b', '--broker_address', type=str, help=(
        'required with --publisher/--subscriber; provide the IP address '
        'of the broker; not needed with ZooKeeper usage'
    ) )
    parser.add_argument('-hist', '--history', type=int, default=1, required=False,
        help=(
            'Optional with --publisher/--subscriber. Provide a sliding window size for messages. Applied to all --topics arguments. '
            'If used with pub, it is the number of offered historical messages. If '
            'used with sub, it is the number of requested historical messages. Offered must be >= requested for a given topic '
            'for broker to match a pub with a sub for that topic.'))


    # Required with --publisher
    parser.add_argument('-bp', '--bind_port', type=int,
        help='(for use with -pub port on which to publish. If not provided with --pub, port 5556 used.')
    parser.add_argument('-s', '--sleep', type=float,
        help='Number of seconds to sleep between publish events. If not provided, 1 second used.')

    #################################################################
    # Required with --broker
    parser.add_argument('-prp', '--pub_reg_port', type=int, default=5555,
        help="which port of the broker will be used to receive pub registration")
    parser.add_argument('-srp', '--sub_reg_port', type=int, default=5556,
        help="which port of the broker will be used to receive sub registration")
    parser.add_argument('-p', '--primary', action='store_true',
    help=(
        'Use with --broker. If passed, use broker as a primary replica. '
        'If not used, broker runs in background as backup, promotable by LB to primary.'))
    parser.add_argument('-zo', '--zone', type=int,
    help=(
        'required with --broker, both with and without --primary. indicate which zone '
        'the broker should be assigned to for fault tolerance in that zone (e.g. 0, 1, 2, etc.). '
        'Load is balanced across zones.'))

    # Optional with --broker (for ZooKeeper testing; auto kill a broker after
    # N seconds to trigger new leader election)
    parser.add_argument('-ak', '--autokill', type=int, required=False,
        help=(
            'Optional with --broker. Auto kill a broker after N (--autokill N) seconds '
            '(to test leader election with multiple brokers)'))

    parser.add_argument('-clearzk', '--clear_zookeeper', action='store_true', required=False,
        help='utility to empty out the zookeeper znodes')

    args = parser.parse_args()

    if args.broker and args.broker > 1:
        raise argparse.ArgumentTypeError('Maximum broker count is 1 (one)')

    logger = logging.getLogger('driver')
    formatter = logging.Formatter('%(prefix)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    driver_logging_prefix = {'prefix': 'DRIVER'}
    logger.addHandler(handler)
    logger = logging.LoggerAdapter(logger, driver_logging_prefix)
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug('Debug mode enabled', extra=driver_logging_prefix)
    else:
        logger.setLevel(logging.INFO)

    if (args.publisher and args.subscriber) or (args.publisher and args.broker) or \
        (args.broker and args.subscriber):
        raise argparse.ArgumentTypeError(
            'Host should have:\n'
            '- only publishers,\n'
            '- only subscribers, or\n'
            '- only a broker. \n'
            'Cannot use mix of --publisher , --subscriber , --broker on single host.'
            )

    if args.publisher:
        if not args.topics:
            raise argparse.ArgumentTypeError(
                'If creating a publisher with --publisher you must provide a set of topics to '
                'publish with -t <topic> [-t <topic> ...]'
                )
        if args.filename:
            raise argparse.ArgumentTypeError(
                '--filename not a valid argument with --publisher type. only works with --subscriber'
                )
        publishers = create_publishers(
            count=args.publisher,
            broker_address=args.broker_address,
            topics=args.topics,
            sleep_period=args.sleep if args.sleep else 1,
            bind_port=args.bind_port if args.bind_port else 5556,
            indefinite=args.indefinite if args.indefinite else False,
            max_event_count=args.max_event_count if args.max_event_count else 15,
            zookeeper_hosts=args.zookeeper_hosts,
            verbose=args.verbose,
            offered=args.history,
            )

    elif args.subscriber:
        if not args.topics:
            raise argparse.ArgumentTypeError(
                'If creating a subscriber with --subscriber, you must provide a set of topics to '
                'subscribe to with -t <topic> [-t <topic> ...]'
                )
        # # no need to provide broker address anymore. It will be obtained from the znode
        # if not args.broker_address:
        #     raise argparse.ArgumentTypeError(
        #         'You need to provide a broker IP address with --broker_address [-b] <IP ADDRESS>'
        #         )
        if args.indefinite and args.filename:
            raise argparse.ArgumentTypeError(
                'Cannot write to file (--filename) if using indefinite loop; file write only '
                'happens at end of finite loop'
                )
        subscribers = create_subscribers(
            count=args.subscriber,
            filename=args.filename if args.filename else None,
            broker_address=args.broker_address,
            centralized=args.centralized,
            topics=args.topics,
            indefinite=args.indefinite if args.indefinite else False,
            max_event_count=args.max_event_count if args.max_event_count else 15,
            zookeeper_hosts=args.zookeeper_hosts,
            verbose=args.verbose,
            requested=args.history
            )
    if args.broker:
        if args.filename:
            raise argparse.ArgumentTypeError(
                '--filename not a valid argument with --publisher type. only works with --subscriber'
                )
        if not args.zone:
            raise Exception("the --zone/-zo argument is required with --broker")
        autokill = None
        if args.autokill:
            autokill = args.autokill
            logger.debug(f"Will autokill broker after {autokill} seconds", extra=driver_logging_prefix)
        create_brokers(
            centralized=args.centralized,
            pub_reg_port=args.pub_reg_port,
            sub_reg_port=args.sub_reg_port,
            indefinite=args.indefinite if args.indefinite else False,
            max_event_count=args.max_event_count if args.max_event_count else 15,
            autokill=autokill,
            zookeeper_hosts=args.zookeeper_hosts,
            verbose=args.verbose,
            primary=args.primary,
            zone=args.zone
        )

    if args.load_balancer:
        create_load_balancer(
            backup_pool_size=args.backup_pool_size,
            load_threshold=args.load_threshold,
            verbose=args.verbose,
            zookeeper_hosts=args.zookeeper_hosts)

    if args.clear_zookeeper:
        if len(args.zookeeper_hosts) == 0:
            raise Exception("Please provide zookeeper host address with --clear_zookeeper, e.g. 127.0.0.1:2181")
        zk = ZookeeperClient(zookeeper_hosts=args.zookeeper_hosts,verbose=True)
        zk.set_logger()
        zk.connect_zk()
        zk.start_session()
        zk.clear_zookeeper_nodes()
        zk.stop_session()
