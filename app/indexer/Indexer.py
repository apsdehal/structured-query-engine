import json
from app.indexer.Flattener import Flattener
from app.indexer.Tokenizer import Tokenizer


class Indexer:
    def __init__(self, config, index):
        self.config = config
        self.mapping = json.load(open(config["mapping_path"], "r"))
        self.flattener = Flattener(self.mapping)
        self.tokenizer = Tokenizer(config, self.flattener.getFlattenedMapping())
        return

    def add(self, doc_type, doc):
        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        # self.generate(doc, inverted_index)

    def generate(self, doc, ii):
        self.generate_inverted_index(self, ii)
        self.generate_doc_store(self, doc, ii)
        self.generate_inverted_doc_frequency(self, ii)
        return

    def generate_inverted_index(self, ii):
        return

    def generate_inverted_doc_frequency(self, ii):
        return

    def generate_doc_store(self, doc, ii):
        return
