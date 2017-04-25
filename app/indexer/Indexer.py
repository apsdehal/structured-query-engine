import json
import pickle
import os
import logging
from collections import Counter
from collections import defaultdict
from indexer.Flattener import Flattener
from indexer.Tokenizer import Tokenizer
from helpers.utils.Compressor import Compressor

log = logging.getLogger(__name__)


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class Indexer:
    tfTable = AutoVivification()
    idfTable = AutoVivification()
    document_store = AutoVivification()

    def __init__(self, config, index):
        self.config = config
        self.flattener = Flattener(config["indices"][index]["mappings"])
        self.mapping = self.flattener.getFlattenedMapping()
        self.tokenizer = Tokenizer(config, self.flattener.getFlattenedMapping())
        self.compressor = Compressor()
        self.num_docs = 0
        self.new_doc_id = 0
        self.index = index
        self.index_doc_type = set()
        self.del_docs = []
        self.dir_path = os.path.join(self.config["indices_path"], self.index)
        self.number_of_shards = config["indices"][index]["settings"]["index"]["number_of_shards"]

    def update(self, doc_type, doc_id, doc):
        deleted = self.delete(doc_type, doc_id)
        self.degenerate()
        doc_updated = self.add(doc_type, doc, doc_id, True)
        log.info(doc_id, ' updated')

        return doc_updated

    def delete(self, doc_type, doc_id):
        if type(doc_id) != int:
            doc_id = int(doc_id)

        if doc_type not in self.index_doc_type:
            log.info('Invalid doc_type')
            return False
        ds_shard = self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]

        try:
            if ds_shard[doc_id]['is_deleted'] is False:
                ds_shard[doc_id]['is_deleted'] = True
                self.num_docs -= 1
                self.del_docs.append([doc_type, doc_id])
                log.info(doc_id, ' deleted')
            else:
                log.info('Document already marked for deletion')
                return False
        except:
            log.info('Invalid doc_id')
            return False

        return True

    def add(self, doc_type, doc, doc_id=0, isUpdate=False):

        if doc_type not in self.index_doc_type:
            self.document_store[self.index][doc_type] = [dict() for x in range(self.number_of_shards)]
            for field in self.mapping[doc_type]:
                if self.mapping[doc_type][field].get('index', True):
                    self.tfTable[self.index][doc_type] = [AutoVivification() for x in range(self.number_of_shards)] # defaultdict(lambda: defaultdict(list))
                    self.idfTable[self.index][doc_type][field] = defaultdict(int)
            self.index_doc_type.add(doc_type)

        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        if isUpdate is False:
            self.new_doc_id += 1
            doc['doc_id'] = self.new_doc_id
        self.num_docs += 1

        doc['is_deleted'] = False
        self.generate(doc['doc_id'], doc_type, inverted_index, doc)

        # if self.num_docs % 1000 == 0:
        #     self.flush_to_file()
        #     print('flushed to file')
        return doc

    def generate(self,doc_id, doc_type, ii, doc):
        # doc_id = self.generate_doc_id()
        # self.generate_index_number(doc_id)
        self.generate_inverted_index(doc_id, doc_type, ii)
        self.generate_doc_store(doc_id, doc_type, doc)
        # self.generate_inverted_doc_frequency(doc_id, doc_type, ii)

    def generate_shard_number(self, doc_id):
        return doc_id % self.number_of_shards

    def generate_inverted_index(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index', True):
                type_field = ii[field]
                if all(isinstance(elem, list) for elem in type_field):
                    type_field = [item for sublist in type_field for item in sublist]

                dictionary = Counter(type_field)
                field_tf = self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field]
                for key in dictionary:
                    # posting = [doc_id, dictionary[key]]
                    try:
                        field_tf[key]['num_docs'] += 1
                    except:
                        field_tf[key]['num_docs'] = 1
                    field_tf[key][doc_id] = dictionary[key]

    def generate_inverted_doc_frequency(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index', True):
                type_field = ii[field]
                if all(isinstance(elem, list) for elem in type_field):
                    type_field = [item for sublist in type_field for item in sublist]

                keys = set(type_field)
                field_idf = self.idfTable[self.index][doc_type][field]
                for key in keys:
                    field_idf[key] += 1
                field_idf['total_number_of_docs'] = self.num_docs

    def generate_doc_store(self, doc_id, doc_type, doc):
        ds_type = self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]
        try:
            ds_type['num_docs'] += 1
        except:
            ds_type['num_docs'] = 1

        ds_type[doc_id] = doc

    def get_doc(self, doc_type, doc_id):
        doc_id = int(doc_id)
        return self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)].get(doc_id, {})


    def degenerate(self):
        for doc_type, doc_id in self.del_docs:
            doc = self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)][doc_id]
            flattened = self.flattener.flatten(doc_type, doc)
            inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
            self.degenerate_inverted_index(doc_id, doc_type, inverted_index)
            self.degenerate_doc_store(doc_id, doc_type, doc)

        self.del_docs = []

    def degenerate_inverted_index(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index',True):
                type_field = ii[field]
                if all(isinstance(elem, list) for elem in type_field):
                    type_field = [item for sublist in type_field for item in sublist]

                dictionary = Counter(type_field)
                tf_field = self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field]
                for key in dictionary:
                    # posting = [doc_id, dictionary[key]]
                    del tf_field[key][doc_id]
                    tf_field[key]['num_docs'] -= 1
                    if (tf_field[key]['num_docs'] == 0):
                        del tf_field[key]
                        if not tf_field:
                            del self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field]

    def degenerate_doc_store(self, doc_id, doc_type, doc):
        shard_ds = self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]
        del shard_ds[doc_id]
        shard_ds['num_docs'] -= 1
        if shard_ds['num_docs'] == 0:
            del shard_ds['num_docs']

    def flush_to_file(self):
        self.degenerate()
        for i in self.tfTable:
            for j in self.tfTable[i]:
                for k in range(self.number_of_shards):
                    file_name = i + "_" + j + "_" + str(k) + ".tf"
                    file_name = os.path.join(self.dir_path, file_name)
                    with open(file_name, 'wb') as f:
                        f.write(self.compressor.compress(json.dumps(self.tfTable[i][j][k]).encode()))

        # for i in self.idfTable:
        #     for j in self.idfTable[i]:
        #         file_name = i+"_"+j+".idf"
        #         print(file_name)
        #         print(self.idfTable[i][j])
        #         with open(file_name,'wb') as f:
        #             f.write(self.compressor.compress(json.dumps(self.idfTable[i][j]).encode()))

        for i in self.document_store:
            for j in self.document_store[i]:
                for k in range(self.number_of_shards):
                    file_name = i + "_" + j + "_" + str(k) + ".ds"
                    file_name = os.path.join(self.dir_path, file_name)
                    with open(file_name, 'wb') as f:
                        f.write(self.compressor.compress(json.dumps(self.document_store[i][j][k]).encode()))
