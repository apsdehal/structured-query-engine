import json


class Indexer:
    def __init__(self, config, index):
        self.config = config
        self.mapping = json.loads(config["mapping_path"])
        self.flattener = Flattener(self.mapping)
        return

    def add(self, doc_type, doc):
        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(flattened)
        self.generate(doc, inverted_index)

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
