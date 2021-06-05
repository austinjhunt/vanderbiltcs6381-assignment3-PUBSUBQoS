import argparse
import logging
from publisher import Publisher
from subscriber import Subscriber
from broker import Broker
from multiprocessing import Process

def create_publishers(count=1, topics=[], sleep_period=1, bind_port=5556,
    indefinite=False, max_event_count=15):
    """ Method to create a set of publishers. If you create more than one,
    it will bind the first at the port specified, it will bind the next to the following port,
    and so on. No need to specify port for every publisher if creating multiple on one host."""
    logging.info(f'Creating {count} publishers')
    pubs = {}
    for i in range(count):
        pubs[i] = Publisher(
            topics,
            sleep_period,
            bind_port + i,
            indefinite,
            max_event_count
        )
        pubs[i].publish()
    return pubs

def create_subscribers(count=1, publishers=[], topics=[], indefinite=False, max_event_count=15):
    """ Method to create a set of subscribers. In order to run multiple subscribers simultaneously,
    need to use multiprocessing library, because Subscriber.listen() will block for i in range(count)
    if run sequentially. E.g. subscriber 2 on the same host will not ever get to listen for updates
    if subscriber 1 listens indefinitely. Multiprocessing not yet implemented, so limit count to 1 for now."""
    logging.info(f'Creating {count} subscribers subscribed to topics <{",".join(topics)}>')
    subs = {}
    processes = []
    for i in range(count):
        subs[i] = Subscriber(
            publishers,
            topics,
            indefinite,
            max_event_count
        )
        subs[i].listen()
    #     process = Process(target=subs[i].listen)
    #     process.start()
    #     processes.append(process)
    # for p in processes:
    #     p.join()
    return subs

def create_broker():
    broker = Broker()

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
        help='pass this followed by an integer N to create N publishers on this host')

    # Required with either -pub or -sub
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

    # Required with --publisher
    parser.add_argument('-b', '--bind_port', type=int,
        help='(for use with -pub port on which to publish. If not provided with --pub, port 5556 used.')
    parser.add_argument('-s', '--sleep', type=float,
        help='Number of seconds to sleep between publish events. If not provided, 1 second used.')


    # Required with --subscriber
    parser.add_argument('-p', '--producers', action='append',
        help=('required with --sub. if creating a sub, provide a list of '
        'addresses of producers that subscriber should listen to, formatted as IP:PORT. Example: '
        '-p 192.168.1.14:5556 -p 192.168.1.15:5557 -p 192.168.1.15:5556'))
    args = parser.parse_args()

    if args.broker and args.broker > 1:
        raise argparse.ArgumentTypeError('Maximum broker count is 1 (one)')

    # Default log level = warning
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('Debug mode enabled')

    logging.debug(F'Creating {args.publisher if args.publisher else 0} publishers on this host')
    logging.debug(F'Creating {args.subscriber if args.subscriber else 0} subscribers on this host')
    logging.debug(F'Creating {args.broker if args.broker else 0} broker on this host')

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
        publishers = create_publishers(
            count=args.publisher,
            topics=args.topics,
            sleep_period=args.sleep,
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
        if not args.producers:
            raise argparse.ArgumentTypeError(
                'If creating a subscriber with --subscriber, you must provide a set of producer '
                'addresses to listen to for publish events with -p PRODUCER_IP1:PORT [-p PRODUCER_IP2:PORT...]'
                )
        subscribers = create_subscribers(
            count=args.subscriber,
            publishers=args.producers,
            topics=args.topics,
            indefinite=args.indefinite if args.indefinite else False,
            max_event_count=args.max_event_count if args.max_event_count else 15
            )
    if args.broker:
        create_broker()

