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
    tfTable = AutoVivification() # defaultdict(lambda : defaultdict(int))
    idfTable = AutoVivification() # defaultdict(lambda : defaultdict(int))
    document_store = AutoVivification() # defaultdict(list)

    def __init__(self, config, index):
        self.config = config
        self.mapping = json.load(open(config["mapping_path"], "r"))
        self.flattener = Flattener(self.mapping)
        self.tokenizer = Tokenizer(config, self.flattener.getFlattenedMapping())
        self.compressor = Compressor()
        self.num_docs = 0
        self.index_number = 0
        self.index = index
        self.index_doc_type = set()
        return



    def add(self, doc_type, doc):



        if(doc_type not in self.index_doc_type):
            self.document_store[self.index][doc_type] = [dict() for x in range(self.config['num_shards'])]
            for field in self.mapping[doc_type]['properties']:
                if(self.mapping[doc_type]['properties'][field].get('index',True)):
                    self.tfTable[self.index][doc_type] = [defaultdict(lambda : defaultdict(list)) for x in range(self.config['num_shards'])]
                    self.idfTable[self.index][doc_type][field] = defaultdict(int)
            self.index_doc_type.add(doc_type)

        self.num_docs += 1
        flattened = self.flattener.flatten(doc_type, doc)
        #print(flattened)
        inverted_index = self.tokenizer.tokenizeFlattened(doc_type, flattened)
        self.generate(doc_type, inverted_index)
        # if(self.num_docs % 1000 == 0):
        self.flush_to_file()

    def generate(self, doc_type, ii):
        doc_id = self.generate_doc_id()
        self.generate_index_number(doc_id)
        self.generate_inverted_index(doc_id, doc_type, ii)
        self.generate_doc_store(doc_id, doc_type, ii)
        self.generate_inverted_doc_frequency(doc_id, doc_type, ii)
        return

    def generate_doc_id(self):
        return self.num_docs

    def generate_index_number(self, doc_id):
        self.index_number = doc_id % self.config['num_shards']

    def generate_inverted_index(self, doc_id, doc_type, ii):

        for field in self.mapping[doc_type]['properties']:
            if(self.mapping[doc_type]['properties'][field].get('index',True)):
                movie_field = ii[field]
                if(all(isinstance(elem, list) for elem in movie_field)):
                    movie_field = [item for sublist in movie_field for item in sublist]

                dictionary = Counter(movie_field)

                for key in dictionary:
                    posting = [doc_id, dictionary[key]]
                    self.tfTable[self.index][doc_type][self.index_number][field][key].append(posting)
        return

    def generate_inverted_doc_frequency(self, doc_id, doc_type, ii):
        for field in self.mapping[doc_type]['properties']:
            if(self.mapping[doc_type]['properties'][field].get('index',True)):
                movie_field = ii[field]
                if(all(isinstance(elem, list) for elem in movie_field)):
                    movie_field = [item for sublist in movie_field for item in sublist]

                keys = set(movie_field)

                for key in keys:
                    self.idfTable[self.index][doc_type][field][key] += 1
                self.idfTable[self.index][doc_type][field]['total_number_of_docs'] = self.num_docs
        return

    def generate_doc_store(self, doc_id, doc_type, ii):
        self.document_store[self.index][doc_type][self.index_number][doc_id] = ii
        return

    def flush_to_file(self):
        for i in self.tfTable:
            for j in self.tfTable[i]:
                for k in range(self.config['num_shards']):
                    file_name = i+"_"+j+"_"+str(k)+".tf"
                    print(file_name)
                    print(self.tfTable[i][j][k])
                    with open(file_name,'wb') as f:
                        pickle.dump(self.compressor.compress(json.dumps(self.tfTable[i][j][k]).encode()), f)

        for i in self.idfTable:
            for j in self.idfTable[i]:
                file_name = i+"_"+j+".idf"
                print(file_name)
                print(self.idfTable[i][j])
                with open(file_name,'wb') as f:
                    pickle.dump(self.compressor.compress(json.dumps(self.idfTable[i][j]).encode()), f)

        for i in self.document_store:
            for j in self.document_store[i]:
                for k in range(self.config['num_shards']):
                    file_name = i+"_"+j+"_"+str(k)+".ds"
                    print(file_name)
                    print(self.document_store[i][j][k])
                    with open(file_name,'wb') as f:
                        pickle.dump(self.compressor.compress(json.dumps(self.document_store[i][j][k]).encode()), f)
