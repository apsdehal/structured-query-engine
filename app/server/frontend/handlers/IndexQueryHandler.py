import tornado.ioloop
import tornado.web
import socket
import logging
import json
from app.retreiver.Retreiver import Retreiver
from app.indexer.Indexer import Indexer
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process


class IndexQueryHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config
        self.indexers = {}
        self.retreivers = {}
        return

    def get(self, index_name, type_name, search_param):
        body = json.loads(self.request.body)
        if index_name not in self.retreivers:
            self.retreivers[index_name] = Retreiver(self.config, index_name)
        response = self.retreivers[index_name].query(index_name, type_name, search_param)
        self.write(response)

    def post(self, index_name, type_name, search_param=None):
        doc = json.loads(self.request.body)
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        doc_saved = self.indexers[index_name].add(type_name, doc)
        self.write(json.dumps(doc_saved))

    def put(self, index_name, type_name, doc_id):
        doc = json.loads(self.request.body)
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        doc_updated = self.indexers[index_name].update(type_name, doc_id, doc)
        self.write(json.dumps(doc_updated))

    def delete(self, index_name, type_name, doc_id):
        if index_name not in self.indexers:
            self.indexers[index_name] = Indexer(self.config, index_name)

        success = self.indexers[index_name].delete(type_name, doc_id)
        self.write(json.dumps({"success": success}))
