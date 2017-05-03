import json
import pickle
import os
import logging
from collections import Counter
from collections import defaultdict
from app.indexer.Flattener import Flattener
from app.indexer.Tokenizer import Tokenizer
from app.helpers.utils.Compressor import Compressor
from app.helpers.utils.General import loadDocStoreAndInvertedIndex
from app.helpers.utils.Debounce import Debounce
from concurrent.futures import ProcessPoolExecutor

log = logging.getLogger(__name__)

class Indexer:

    def __init__(self, config, index):
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
        self.document_store, self.tfTable = loadDocStoreAndInvertedIndex(index, self.number_of_shards, config, self.mapping)
        self.set_new_doc_ids()
        self.set_index_doc_types()

    def set_new_doc_ids(self):
        self.new_doc_ids = defaultdict(int)
        for t in self.document_store:
            for i in range(self.number_of_shards):
                try:
                    if self.new_doc_ids[t] < self.document_store[t][i]['new_doc_id']:
                        self.new_doc_ids[t] = self.document_store[t][i]['new_doc_id']
                except:
                    pass

    def set_index_doc_types(self):
        for i in self.document_store:
            self.index_doc_type.add(i)

    def update(self, doc_type, doc_id, doc):
        str_doc_id = str(doc_id)
        try:
            old_doc = self.document_store[doc_type][self.generate_shard_number(doc_id)][str_doc_id]
            flattened = self.flattener.flatten(doc_type, old_doc)
            inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
            self.degenerate_inverted_index(str_doc_id, doc_type, inverted_index)
            self.degenerate_doc_store(str_doc_id, doc_type)
            isUpdate = True
        except:
            old_doc = dict()
            isUpdate = False
        new_doc = old_doc.copy()
        new_doc.update(doc)
        new_doc['doc_id'] = str_doc_id
        new_doc['is_deleted'] = False
        return_doc = self.add(doc_type, new_doc, isUpdate, False)
        if isUpdate:
            log.info(doc['doc_id'] + ' updated')
            return_doc['created'] = False
            return_doc['result'] = 'updated'

        return return_doc

    def delete(self, doc_type, doc_id):
        str_doc_id = str(doc_id)
        if doc_type not in self.index_doc_type:
            log.info('Invalid doc_type')
            return False
        shard_ds = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        try:
            if shard_ds[str_doc_id]['is_deleted'] is False:
                shard_ds[str_doc_id]['is_deleted'] = True
                self.del_docs.append([doc_type, str_doc_id])
                log.info(str_doc_id + ' deleted')
            else:
                log.info('Document already marked for deletion')
                return False
        except:
            log.info('Invalid doc_id')
            return False

        self.future_flush()
        return True

    def future_flush(self):
        with ProcessPoolExecutor() as executor:
            executor.submit(self.flush_to_file())

    def add(self, doc_type, doc, isUpdate=False, gen_new_doc_id=True):
        return_doc = dict()
        return_doc['_index'] = self.index
        return_doc['_type'] = doc_type
        if doc_type not in self.index_doc_type:
            self.document_store[doc_type] = [dict() for x in range(self.number_of_shards)]
            self.tfTable[doc_type] = [dict() for x in range(self.number_of_shards)]
            self.index_doc_type.add(doc_type)
        if gen_new_doc_id is True:
            self.new_doc_ids[doc_type] += 1
            doc['doc_id'] = str(self.new_doc_ids[doc_type])
            doc['is_deleted'] = False
        return_doc['_id'] = doc['doc_id']
        flattened = self.flattener.flatten(doc_type, doc)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        self.generate(doc['doc_id'], doc_type, doc, inverted_index)
        if isUpdate is False:
            log.info(doc['doc_id'] + ' created')
            return_doc['result'] = 'created'
            return_doc['created'] = True
        self.future_flush()
        return return_doc

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
                shard_tf = self.tfTable[doc_type][self.generate_shard_number(doc_id)]
                shard_tf[field] = shard_tf.get(field, dict())

                for key in dictionary:
                    shard_tf[field][key] = shard_tf[field].get(key, [0, dict()])
                    shard_tf[field][key][0] += 1
                    shard_tf[field][key][1][doc_id] = dictionary[key]

    def generate_doc_store(self, doc_id, doc_type, doc):
        ds_type = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        try:
            ds_type['num_docs'] += 1
        except:
            ds_type['num_docs'] = 1
        ds_type['new_doc_id'] = int(doc_id)
        ds_type[doc_id] = doc

    def get_doc(self, doc_type, doc_id):
        doc = self.document_store[doc_type][self.generate_shard_number(doc_id)].get(doc_id, dict())
        return_doc = dict()
        return_doc['_index'] = self.index
        return_doc['_type'] = doc_type
        return_doc['_source'] = dict()
        try:
            if doc['is_deleted'] is False:
                return_doc['_source'] = doc
                return_doc['_id'] = doc_id
                return return_doc
            else:
                return return_doc
        except:
            return return_doc

    def degenerate(self):
        for doc_type, doc_id in self.del_docs:
            doc = self.document_store[doc_type][self.generate_shard_number(doc_id)][doc_id]
            flattened = self.flattener.flatten(doc_type, doc)
            inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
            self.degenerate_inverted_index(doc_id, doc_type, inverted_index)
            self.degenerate_doc_store(doc_id, doc_type)
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
                    del field_tf[key][1][doc_id]
                    field_tf[key][0] -= 1
                    if (field_tf[key][0] == 0):
                            del field_tf[key]
                            if not field_tf:
                                del field_tf

    def degenerate_doc_store(self, doc_id, doc_type):
        shard_ds = self.document_store[doc_type][self.generate_shard_number(doc_id)]
        try:
            del shard_ds[doc_id]
            shard_ds['num_docs'] -= 1
            if shard_ds['num_docs'] == 0:
                del shard_ds['num_docs']
        except:
            pass

    def get_doc_store_ii(self):
        return self.document_store, self.tfTable

    @Debounce(seconds=10)
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
