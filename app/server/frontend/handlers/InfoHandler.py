import tornado.ioloop
import tornado.web
from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process, escape
import socket
import logging
import json


class InfoHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config["config"]
        return

    def get(self):
        data = {}
        data["name"] = self.config["name"]
        data["cluster_name"] = self.config["cluster_name"]
        data["cluster_uuid"] = self.config["cluster_uuid"]
        data["tagline"] = "You know, for structured search"

        self.write(json.dumps(data))
