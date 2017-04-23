from app.helpers.utils.Bootstrapper import Bootstrapper
import app.server.frontend.frontend as frontend
import app.server.indexserver.indexserver as indexserver
import app.server.docserver.docserver as docserver
import app.server.idfserver.idfserver as idfserver


def main():
    bootstrapper = Bootstrapper()
    config = bootstrapper.bootstrap()
    docserver.start(config)
    indexserver.start(config)
    idfserver.start(config)
    frontend.start(config)


if __name__ == '__main__':
    main()
