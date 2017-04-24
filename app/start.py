from helpers.utils.Bootstrapper import Bootstrapper
import server.frontend.frontend as frontend
import logging
import os

log = logging.getLogger(__name__)


def main():
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = os.path.abspath(os.path.join(base_path, os.pardir))
    bootstrapper = Bootstrapper()
    config = bootstrapper.bootstrap(base_path)
    frontend.start(config)


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s',
                        level=logging.DEBUG)
    main()
