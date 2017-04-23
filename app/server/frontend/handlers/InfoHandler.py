import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process, escape
import socket
import logging
import json


class InfoHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config
        return

    def get(self):
        data = {}
        data["name"] = config["name"]
        data["cluster_name"] = config["cluster_name"]
        data["cluster_uuid"] = config["cluster_uuid"]
        data["tagline"] = "You know for structured search"

        self.write(json.dumps(data))
