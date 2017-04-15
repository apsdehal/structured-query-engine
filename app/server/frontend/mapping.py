import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process
import socket
import logging

log = logging.getLogger(__name__)

class DefaultHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello World!")

class MappingHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        self.write("Mapping Handler!")


def main():
    BASE_PORT = 50000
    task_id = process.fork_processes(3)
    #starting mapping server
    if task_id==0 :
        app = tornado.web.Application([(r"/", DefaultHandler),(r"/setting", MappingHandler),])
        app.listen(BASE_PORT)
        log.info("Mapping server listening on " + str(BASE_PORT))
    # starting document servers
    # elif task_id ==1:
    #     server_id = task_id-1
    #     app = tornado.web.Application([(r"/", DefaultHandler),(r"/doc", DocumentServerHandler,dict(server_id=server_id))])
    #     app.listen(inventory.doc_server_ports[server_id])
    #     log.info("Document server "+str(server_id)+" listening on " + str(inventory.doc_server_ports[server_id]))
    # #starting index servers
    # else:
    #     server_id = task_id-inventory.document_partitions-1
    #     app = tornado.web.Application([(r"/", DefaultHandler),(r"/index", IndexServerHandler,dict(server_id=server_id))])
    #     app.listen(inventory.index_server_ports[server_id])
    #     log.info("Index server "+str(server_id)+" listening on " + str(inventory.index_server_ports[server_id]))
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.DEBUG)
    main()
    

