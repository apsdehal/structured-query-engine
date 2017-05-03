import tornado.ioloop
import tornado.web
import socket
import logging
import copy
import json
import uuid
import time
import os
import shutil

from tornado.httpclient import AsyncHTTPClient
from tornado import gen, process, escape
from app.indexer.Indexer import Indexer
from app.retriever.Retriever import Retriever


class IndexHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config["config"]
        return

    def head(self, index_name):
        if index_name not in self.config["indices"]:
            self.set_status(404)
            self.write("")
            return
        else:
            self.set_status(200)
            self.write("")

    def get(self, index_name):
        if index_name not in self.config["indices"]:
            self.set_status(404)
            self.write(json.dumps({"error": "Index not found"}))
            return
        save_path = os.path.join(self.config["data_path"], "indices", index_name, "info")

        with open(save_path, "r") as f:
            index_info = f.read()

        self.write(index_info)

    def post(self):
        return

    def put(self, index_name):
        body = escape.json_decode(self.request.body.decode('utf-8'))
        if index_name in self.config["indices"]:
            self.set_status(400)
            self.write(json.dumps({"error": "Index already exists"}))
            return

        mappings = body.get("mappings", None)
        settings = body.get("settings", None)

        if settings is None or "number_of_shards" not in settings:
            settings = {"number_of_shards": 1}

        settings_copy = {}
        settings_copy["index"] = copy.deepcopy(settings)
        settings = settings_copy
        settings["index"]["uuid"] = uuid.uuid4().hex
        settings["index"]["creation_time"] = time.time()
        # We don't support replicas as of now
        settings["index"]["number_of_replicas"] = 1
        settings["index"]["provided_name"] = index_name

        info = {}
        info["mappings"] = mappings
        info["settings"] = settings

        data_path = self.config["data_path"]
        save_path = os.path.join(data_path, "indices", index_name)

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        save_path = os.path.join(save_path, "info")

        with open(save_path, "w") as f:
            f.write(json.dumps(info))
        self.config["indices"][index_name] = info
        self.config["indexers"][index_name] = Indexer(self.config, index_name)
        self.config["retrievers"][index_name] = Retriever(self.config, index_name)
        self.write(json.dumps({"acknowledged": True}))

    def delete(self):
        if index_name not in self.config["indices"]:
            self.set_status(404)
            self.write(json.dumps({"error": "Index doesn't exists"}))
            return

        index_path = os.path.join(self.config["data_path"], "indices", index_name)
        shutil.rmtree(index_path)
