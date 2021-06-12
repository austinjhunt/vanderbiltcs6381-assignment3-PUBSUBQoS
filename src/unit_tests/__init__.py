import logging
import sys
logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    format='%(prefix)s - %(message)s')