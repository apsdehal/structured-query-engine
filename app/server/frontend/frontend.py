from tornado.ioloop import IOLoop
from tornado import web, httpserver, process, netutil
from server.frontend.handlers.IndexHandler import IndexHandler
from server.frontend.handlers.IndexQueryHandler import IndexQueryHandler
from server.frontend.handlers.InfoHandler import InfoHandler

import logging

log = logging.getLogger(__name__)


def start(config):
    SERVER_PORT = config["base_port"]
    task_id = process.fork_processes(None)

    application = web.Application([
        (r"/", InfoHandler, dict(config=config)),
        (r"/([^/]+)", IndexHandler, dict(config=config)),
        (r"/([^/]+)/([^/]+)", IndexQueryHandler, dict(config=config)),
        (r"/([^/]+)/([^/]+)/([^/]+)", IndexQueryHandler, dict(config=config))
    ])

    http_server = httpserver.HTTPServer(application)
    http_server.add_sockets(netutil.bind_sockets(SERVER_PORT + task_id))
    log.info("Frontend listening on %d", SERVER_PORT + task_id)
    IOLoop.current().start()
