import json
import pickle
import os
import logging
from collections import Counter
from collections import defaultdict
from indexer.Flattener import Flattener
from indexer.Tokenizer import Tokenizer
from helpers.utils.Compressor import Compressor
from helpers.utils.General import AutoVivification, loadDocStoreAndInvertedIndex

log = logging.getLogger(__name__)


class Indexer:

    def __init__(self, config, index):
        self.num_docs = 0
        self.new_doc_id = 0
        self.index = index
        self.index_doc_type = set()
        self.del_docs = []
        self.config = config
        self.flattener = Flattener(config["indices"][index]["mappings"])
        self.mapping = self.flattener.getFlattenedMapping()
        self.tokenizer = Tokenizer(config, self.flattener.getFlattenedMapping())
        self.compressor = Compressor()
        self.dir_path = os.path.join(self.config["indices_path"], self.index)
        self.number_of_shards = config["indices"][index]["settings"]["index"]["number_of_shards"]
        self.tfTable = AutoVivification()
        self.document_store, self.tfTable = loadDocStoreAndInvertedIndex(index, self.number_of_shards, config, self.mapping)
        print(self.document_store)

    def __enter__(self):
        log.info('in enter')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush_to_file()
        with open(self.index, 'wb') as f:
            pickle.dump(self.__dict__, f, pickle.HIGHEST_PROTOCOL)
        log.info('in exit')

    def update(self, doc_type, doc_id, doc):
        if type(doc_id) != int:
            doc_id = int(doc_id)
        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        self.degenerate_inverted_index(doc_id, doc_type, ii)
        self.degenerate_doc_store(doc_id, doc_type, doc)
        doc['doc_id'] = doc_id
        doc['is_deleted'] = False
        doc_updated = self.add(doc_type, doc, True)
        log.info(str(doc_id) + ' updated')
        return doc_updated

    def delete(self, doc_type, doc_id):
        if type(doc_id) != int:
            doc_id = int(doc_id)
        if doc_type not in self.index_doc_type:
            log.info('Invalid doc_type')
            return False
        shard_ds = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        try:
            if shard_ds[doc_id]['is_deleted'] is False:
                shard_ds[doc_id]['is_deleted'] = True
                self.num_docs -= 1
                self.del_docs.append([doc_type, doc_id])
                log.info(str(doc_id) + ' deleted')
            else:
                log.info('Document already marked for deletion')
                return False
        except:
            log.info('Invalid doc_id')
            return False

        return True

    def add(self, doc_type, doc, isUpdate=False):
        if doc_type not in self.index_doc_type:
            self.document_store[doc_type] = [dict() for x in range(self.number_of_shards)]
            for field in self.mapping[doc_type]:
                if self.mapping[doc_type][field].get('index', True):
                    self.tfTable[doc_type] = [AutoVivification() for x in range(self.number_of_shards)] # defaultdict(lambda: defaultdict(list))
            self.index_doc_type.add(doc_type)
        if isUpdate is False:
            self.new_doc_id += 1
            self.num_docs += 1
            doc['doc_id'] = self.new_doc_id
            doc['is_deleted'] = False
        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        self.generate(doc['doc_id'], doc_type, doc, inverted_index)
        log.info(str(doc['doc_id']) + ' added')
        # if self.num_docs % 1000 == 0:
        #     self.flush_to_file()
        #     print('flushed to file')
        return doc

    def generate(self,doc_id, doc_type, doc, ii):
        self.generate_inverted_index(doc_id, doc_type, ii)
        self.generate_doc_store(doc_id, doc_type, doc)

    def generate_shard_number(self, doc_id):
        if type(doc_id) != int:
            doc_id = int(doc_id)
        return doc_id % self.number_of_shards

    def generate_inverted_index(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index', True):
                type_field = ii.get(field, [])
                if all(isinstance(elem, list) for elem in type_field):
                    type_field = [item for sublist in type_field for item in sublist]
                dictionary = Counter(type_field)
                field_tf = self.tfTable[doc_type][self.generate_shard_number(doc_id)][field]
                for key in dictionary:
                    try:
                        field_tf[key]['num_docs'] += 1
                    except:
                        field_tf[key]['num_docs'] = 1
                    field_tf[key][doc_id] = dictionary[key]

    def generate_doc_store(self, doc_id, doc_type, doc):
        ds_type = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        try:
            ds_type['num_docs'] += 1
        except:
            ds_type['num_docs'] = 1

        ds_type[doc_id] = doc

    def get_doc(self, doc_type, doc_id):
        return self.document_store[doc_type][self.generate_shard_number(doc_id)].get(doc_id, {})

    def degenerate(self):
        for doc_type, doc_id in self.del_docs:
            doc = self.document_store[doc_type][self.generate_shard_number(doc_id)][doc_id]
            flattened = self.flattener.flatten(doc_type, doc)
            inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
            self.degenerate_inverted_index(doc_id, doc_type, inverted_index)
            self.degenerate_doc_store(doc_id, doc_type, doc)
        self.del_docs = []

    def degenerate_inverted_index(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index',True):
                type_field = ii.get(field, [])
                if all(isinstance(elem, list) for elem in type_field):
                    type_field = [item for sublist in type_field for item in sublist]
                dictionary = Counter(type_field)
                field_tf = self.tfTable[doc_type][self.generate_shard_number(doc_id)][field]
                for key in dictionary:
                    del field_tf[key][doc_id]
                    field_tf[key]['num_docs'] -= 1
                    if (field_tf[key]['num_docs'] == 0):
                        del field_tf[key]
                        if not field_tf:
                            del field_tf

    def degenerate_doc_store(self, doc_id, doc_type, doc):
        shard_ds = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        del shard_ds[doc_id]
        shard_ds['num_docs'] -= 1
        if shard_ds['num_docs'] == 0:
            del shard_ds['num_docs']

    def flush_to_file(self):
        self.degenerate()
        for i in self.tfTable:
            for j in range(self.number_of_shards):
                file_name = self.index + "_" + i + "_" + str(j) + ".tf"
                file_name = os.path.join(self.dir_path, file_name)
                log.info(file_name)
                # print(self.tfTable[i][j])
                with open(file_name, 'wb') as f:
                    f.write(self.compressor.compress(json.dumps(self.tfTable[i][j]).encode()))
        for i in self.document_store:
            for j in range(self.number_of_shards):
                file_name = self.index + "_" + i + "_" + str(j) + ".ds"
                file_name = os.path.join(self.dir_path, file_name)
                log.info(file_name)
                # print(self.document_store[i][j])
                with open(file_name, 'wb') as f:
                    f.write(self.compressor.compress(json.dumps(self.document_store[i][j]).encode()))
