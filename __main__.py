import logging
import os

from qmgr import CeleryQueue

debug = os.environ.get('DEBUG', False) in ['true', 'yes', '1', 'True', 'Yes', 'Y', 'y']
logging.basicConfig(level=logging.INFO if not debug else logging.DEBUG)

log = logging.getLogger(__name__)


def main():
    celery_queue = CeleryQueue()

    celery_queue.run()


if __name__ == '__main__':
    main()
