import tornado.ioloop
import tornado.web
import socket
import logging
import json
from app.retriever.Retriever import Retriever
from app.indexer.Indexer import Indexer
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process, escape


class IndexQueryHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config["config"]
        self.indexers = self.config["indexers"]
        self.retrievers = self.config["retrievers"]

    def get(self, index_name, type_name, search_param):
        if search_param != "_search":
            if index_name not in self.indexers:
                self.indexers[index_name] = Indexer(self.config, index_name)
            self.write(self.indexers[index_name].get_doc(type_name, search_param))
            return

        body = escape.json_decode(self.request.body.decode('utf-8'))
        if index_name not in self.retrievers:
            self.retrievers[index_name] = Retriever(self.config, index_name)
        response = self.retrievers[index_name].query(type_name, body)
        self.write(response)

    def post(self, index_name, type_name, search_param=None):
        doc = escape.json_decode(self.request.body.decode('utf-8'))
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        doc_saved = self.indexers[index_name].add(type_name, doc)
        self.write(json.dumps(doc_saved))

    def put(self, index_name, type_name, doc_id):
        doc = escape.json_decode(self.request.body.decode('utf-8'))
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        doc_updated = self.indexers[index_name].update(type_name, doc_id, doc)
        self.write(json.dumps(doc_updated))

    def delete(self, index_name, type_name, doc_id):
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        success = self.indexers[index_name].delete(type_name, doc_id)
        self.write(json.dumps({"success": success}))
