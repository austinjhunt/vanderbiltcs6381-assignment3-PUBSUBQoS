import argparse
import logging
from publisher import Publisher
from subscriber import Subscriber

def create_publishers(count=1):
    logging.info(f'Creating {count} publishers')
    pubs = {}
    for i in range(count):
        pubs[i] = Publisher()
    return pubs

def create_subscribers(count=1):
    logging.info(f'Creating {count} subscribers')
    subs = {}
    for i in range(count):
        subs[i] = Subscriber()
    return subs

def create_broker(count=1):
    broker = Broker()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pass arguments to create publishers, subscribers, or an intermediate message broker')
    parser.add_argument('-v', '--verbose', help='increase output verbosity', action='store_true')
    parser.add_argument('-pub', '--publisher',  type=int, help='pass this followed by an integer N to create N publishers on this host')
    parser.add_argument('-sub', '--subscriber', type=int, help='pass this followed by an integer N to create N subscribers on this host')
    parser.add_argument('--broker', type=int, help='pass this followed by an integer N to create N publishers on this host')
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

    if (args.publisher and args.subscriber) or (args.publisher and args.broker) or (args.broker and args.subscriber):
        raise argparse.ArgumentTypeError('Host should have 1) only publishers, 2) only subscribers, or 3) only a broker, not a mix')

    if args.publisher:
        create_publishers(count=args.publisher)
    if args.subscriber:
        create_subscribers(count=args.subscriber)
    if args.broker:
        create_broker()

