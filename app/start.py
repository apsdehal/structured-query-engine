from helpers.utils.Bootstrapper import Bootstrapper
import server.frontend.frontend as frontend
import logging

log = logging.getLogger(__name__)


def main():
    bootstrapper = Bootstrapper()
    config = bootstrapper.bootstrap()
    frontend.start(config)


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s',
                        level=logging.DEBUG)
    main()
