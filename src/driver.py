import argparse
import logging
from lib.publisher import Publisher
from lib.subscriber import Subscriber
from lib.broker import Broker
from multiprocessing import Process

driver_logging_prefix = {'prefix': 'DRIVER'}

def create_publishers(count=1, topics=[], broker_address='127.0.0.1',
    sleep_period=1, bind_port=5556, indefinite=False, max_event_count=15):
    """ Method to create a set of publishers.
    In order to run multiple subscribers simultaneously,
    need to use multiprocessing library, because Publisher.publish() will block for i in range(count)
    if run sequentially. E.g. publisher 2 on the same host will not ever get to publish updates
    if publisher 1 publishes indefinitely. Multiprocessing not yet implemented, so limit count to 1 for now

    EVENTUALLY: If you create more than one, first publisher will be bound to first port specified,
    next will bind to port+1, next to port+2, etc. No need to specify port for every publisher if
    creating multiple on one host."""

    logging.info(f'Creating {count} publishers for topics {",".join(topics)}', extra=driver_logging_prefix )
    pubs = {}
    for i in range(count):
        pubs[i] = Publisher(
            topics=topics,
            broker_address=broker_address,
            sleep_period=sleep_period,
            bind_port=bind_port + i,
            indefinite=indefinite,
            max_event_count=max_event_count
        )
        try:
            pubs[i].configure()
            pubs[i].publish()
        except KeyboardInterrupt:
            # If you interrupt/cancel a publisher, be sure to disconnect properly
            # to tell broker it's no longer active
            pubs[i].disconnect()

    return pubs

def create_subscribers(count=1, filename=None, broker_address='127.0.0.1',
     centralized=False, topics=[], indefinite=False, max_event_count=15):
    """ Method to create a set of subscribers. In order to run multiple subscribers simultaneously,
    need to use multiprocessing library, because Subscriber.listen() will block for i in range(count)
    if run sequentially. E.g. subscriber 2 on the same host will not ever get to listen for updates
    if subscriber 1 listens indefinitely. Multiprocessing not yet implemented, so limit count to 1 for now."""
    logging.info(f'Creating {count} subscribers subscribed to topics <{",".join(topics)}>', extra=driver_logging_prefix )
    subs = {}
    for i in range(count):
        subs[i] = Subscriber(
            topics=topics,
            filename=filename,
            broker_address=broker_address,
            centralized=centralized,
            indefinite=indefinite,
            max_event_count=max_event_count
        )
        try:
            subs[i].configure()
            subs[i].notify()
            # This will call if notify is not indefinite
            subs[i].disconnect()
        except KeyboardInterrupt:
            # If you interrupt/cancel a subscriber, be sure to disconnect properly
            # to tell broker it's no longer active
            subs[i].disconnect()
        # If filename provided (only works with finite notify() loop), write to file
        if filename:
            subs[i].write_stored_messages()
    return subs

def create_broker(indefinite=False, centralized=False):
    broker = Broker(
        centralized=centralized,
        indefinite=indefinite
    )
    broker.configure()
    try:
        broker.event_loop()
    except KeyboardInterrupt:
        # If you interrupt/cancel a broker, be sure to disconnect/clean all sockets
        broker.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Pass arguments to create publishers, subscribers, or an intermediate message broker')
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    # Choose type of entity
    parser.add_argument('-pub', '--publisher',  type=int,
        help='pass this followed by an integer N to create N publishers on this host')
    parser.add_argument('-sub', '--subscriber', type=int,
        help='pass this followed by an integer N to create N subscribers on this host')
    parser.add_argument('--broker', type=int,
        help='pass this followed by 1 to create 1 (max) broker on this host')

    # parser.add_argument('-a', '--address', type=str, required=True, help=(
    #     'IP address of the host on which you are creating the entity; required with -pub, -sub, and --broker'
    # ))

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
    
    # Required with --publisher and --subscriber
    parser.add_argument('-b', '--broker_address', type=str, help=(
        'required with --publisher/--subscriber; provide the IP address of the broker'
    ) )

    # Required with --publisher
    parser.add_argument('-bp', '--bind_port', type=int,
        help='(for use with -pub port on which to publish. If not provided with --pub, port 5556 used.')
    parser.add_argument('-s', '--sleep', type=float,
        help='Number of seconds to sleep between publish events. If not provided, 1 second used.')

    args = parser.parse_args()

    if args.broker and args.broker > 1:
        raise argparse.ArgumentTypeError('Maximum broker count is 1 (one)')

    # Default log level = warning
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(prefix)s - %(message)s')
        logging.debug('Debug mode enabled', extra=driver_logging_prefix)
    else:
        logging.basicConfig(format='%(prefix)s - %(message)s')

    logging.debug(F'Creating {args.publisher if args.publisher else 0} publishers on this host', extra=driver_logging_prefix)
    logging.debug(F'Creating {args.subscriber if args.subscriber else 0} subscribers on this host', extra=driver_logging_prefix)
    logging.debug(F'Creating {args.broker if args.broker else 0} broker on this host', extra=driver_logging_prefix)

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
        if not args.broker_address:
            raise argparse.ArgumentTypeError(
                'You need to provide a broker IP address with --broker_address [-b] <IP ADDRESS>'
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
            max_event_count=args.max_event_count if args.max_event_count else 15
            )

    elif args.subscriber:
        if not args.topics:
            raise argparse.ArgumentTypeError(
                'If creating a subscriber with --subscriber, you must provide a set of topics to '
                'subscribe to with -t <topic> [-t <topic> ...]'
                )
        if not args.broker_address:
            raise argparse.ArgumentTypeError(
                'You need to provide a broker IP address with --broker_address [-b] <IP ADDRESS>'
                )
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
            max_event_count=args.max_event_count if args.max_event_count else 15
            )
    if args.broker:
        if args.filename:
            raise argparse.ArgumentTypeError(
                '--filename not a valid argument with --publisher type. only works with --subscriber'
                )
        create_broker(
            centralized=args.centralized,
            indefinite=args.indefinite
        )


