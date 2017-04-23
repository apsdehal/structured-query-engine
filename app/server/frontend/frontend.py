from tornado.ioloop import IOLoop
from tornado import web, httpserver, process, netutil
from app.server.frontend.handlers.IndexHandler import IndexHandler
from app.server.frontend.handlers.IndexQueryHandler import IndexQueryHandler
from app.server.frontend.handlers.InfoHandler import InfoHandler


def startFrontend(config):
    SERVER_PORT = config["base_port"]
    task_id = process.fork_processes(None)

    application = web.Application([
        (r"/", InfoHandler),
        (r"/([^/]+)", IndexHandler),
        (r"/([^/]+)/([^/]+)/([^/]+)", IndexQueryHandler)
    ])

    http_server = httpserver.HTTPServer(application)
    http_server.add_sockets(netutil.bind_sockets(SERVER_PORT))
    log.info("Frontend listening on %d", SERVER_PORT + task_id)
    IOLoop.current().start()
