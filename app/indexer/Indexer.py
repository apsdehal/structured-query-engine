import json
import pickle
from collections import Counter
from collections import defaultdict
from indexer.Flattener import Flattener
from indexer.Tokenizer import Tokenizer
from helpers.utils.Compressor import Compressor


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class Indexer:
    # defaultdict(lambda: defaultdict(int))
    tfTable = AutoVivification()
    # defaultdict(lambda: defaultdict(int))
    idfTable = AutoVivification()
    # defaultdict(list)
    document_store = AutoVivification()

    def __init__(self, config, index):
        self.config = config
        self.flattener = Flattener(config[index]["mappings"])
        self.mapping = self.flattener.getFlattenedMapping()
        self.tokenizer = Tokenizer(config, self.flattener.getFlattenedMapping())
        self.compressor = Compressor()
        self.num_docs = 0
        self.new_doc_id = 0
        self.index = index
        self.index_doc_type = set()
        self.del_docs = []
        self.number_of_shards = config[index]["settings"]["number_of_shards"]
        return

    def update(self, doc_type, doc_id, doc):
        deleted = self.delete(doc_type, doc_id)

        self.degenerate()

        doc_updated = self.add(doc_type, doc, doc_id, True)

        print(doc_id, ' updated')

        return doc_updated

    def delete(self, doc_type, doc_id):
        if doc_type not in self.index_doc_type:
            print('Invalid doc_type')
            return False

        try:
            if self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)][doc_id]['is_deleted'] is False:
                self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)][doc_id]['is_deleted'] = True
                self.num_docs -= 1
                self.del_docs.append([doc_type, doc_id])
                print(doc_id, ' deleted')
            else:
                print('Document already marked for deletion')
                return False
        except:
            print('Invalid doc_id')
            return False

        return True

    def add(self, doc_type, doc, doc_id=0, isUpdate=False):

        if doc_type not in self.index_doc_type:
            # print('new doc_type')
            self.document_store[self.index][doc_type] = [dict() for x in range(self.number_of_shards)]
            for field in self.mapping[doc_type]:
                if self.mapping[doc_type][field].get('index', True):
                    self.tfTable[self.index][doc_type] = [AutoVivification() for x in range(self.number_of_shards)] # defaultdict(lambda: defaultdict(list))
                    self.idfTable[self.index][doc_type][field] = defaultdict(int)
            self.index_doc_type.add(doc_type)
            # print('doc_type added')

        flattened = self.flattener.flatten(doc_type, doc)
        # print('flattened')
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        # print('tokenized')
        if isUpdate is False:
            self.new_doc_id += 1
            doc['doc_id'] = self.new_doc_id
        self.num_docs += 1

        doc['is_deleted'] = False
        # print('call generate')
        self.generate(doc['doc_id'], doc_type, inverted_index, doc)
        # print('return from generate')

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
                movie_field = ii[field]
                if all(isinstance(elem, list) for elem in movie_field):
                    movie_field = [item for sublist in movie_field for item in sublist]

                dictionary = Counter(movie_field)

                for key in dictionary:
                    # posting = [doc_id, dictionary[key]]
                    try:
                        self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key]['num_docs'] += 1
                    except:
                        self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key]['num_docs'] = 1
                    self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key][doc_id] = dictionary[key]

    def generate_inverted_doc_frequency(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]:
            if self.mapping[doc_type][field].get('index', True):
                movie_field = ii[field]
                if all(isinstance(elem, list) for elem in movie_field):
                    movie_field = [item for sublist in movie_field for item in sublist]

                keys = set(movie_field)

                for key in keys:
                    self.idfTable[self.index][doc_type][field][key] += 1
                self.idfTable[self.index][doc_type][field]['total_number_of_docs'] = self.num_docs

    def generate_doc_store(self, doc_id, doc_type, doc):
        try:
            self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]['num_docs'] += 1
        except:
            self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]['num_docs'] = 1

        self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)][doc_id] = doc

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
                movie_field = ii[field]
                if all(isinstance(elem, list) for elem in movie_field):
                    movie_field = [item for sublist in movie_field for item in sublist]

                dictionary = Counter(movie_field)

                for key in dictionary:
                    # posting = [doc_id, dictionary[key]]
                    del self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key][doc_id]
                    self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key]['num_docs'] -= 1
                    if (self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key]['num_docs'] == 0):
                        del self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field][key]
                        if not self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field]:
                            del self.tfTable[self.index][doc_type][self.generate_shard_number(doc_id)][field]

    def degenerate_doc_store(self, doc_id, doc_type, doc):
        del self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)][doc_id]

        self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]['num_docs'] -= 1

        if self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]['num_docs'] == 0:
            del self.document_store[self.index][doc_type][self.generate_shard_number(doc_id)]['num_docs']

    def flush_to_file(self):
        self.degenerate()

        for i in self.tfTable:
            for j in self.tfTable[i]:
                for k in range(self.number_of_shards):
                    file_name = i + "_" + j + "_" + str(k) + ".tf"
                    print(file_name)
                    # print(self.tfTable[i][j][k])
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
                    print(file_name)
                    # print(self.document_store[i][j][k])
                    with open(file_name, 'wb') as f:
                        f.write(self.compressor.compress(json.dumps(self.document_store[i][j][k]).encode()))
