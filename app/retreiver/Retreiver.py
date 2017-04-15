import json
from nltk.tokenize import RegexpTokenizer

### typical query structure ###
### {
###     "query": {
###         "query_string" : {
###             "fields" : ["field_1", "field_2"],
###             "query" : "this AND that OR thus",
###         }
###     }
### }

class Retreiver:
    def __init__(self, config):
        self.config = config
        self.MAX_ITEMS_RETURNED = 10
        return

    def dot_product(self, vector1, vector2):
        result = 0
        for key,value in vector1.items():
            result = result + value*vector2.get(key,0)
        return result

    def query(self, q):
        tokenizer = RegexpTokenizer(r'\w+')

        data = json.loads(q)
        fields = data['query']['query_string']['fields']
        query_string = data['query']['query_string']['query']
        query_tokens = tokenizer.tokenize(query_string.lower())

        with open(self.config.mapping_path) as handler:    
            mapping = json.load(handler)

        query_vector = {}
        document_vectors = {}
        json_output = {}
        posting_list = []

        for token in query_tokens:
            #if query contains a word twice then it will be ignored second time
            if token in query_vector:
                continue
            query_vector[token] = 1.0*(self.config.term_inv_doc_freq.get(token,1.0))
            tf_list = self.config.tf_list.get(token,[])
            for doc_id,freq in tf_list:
                if doc_id in document_vectors:
                    inner_dict = document_vectors[doc_id]
                    inner_dict[token] = freq*(self.term_inv_doc_freq.get(token,1))
                else:
                    inner_dict = {}
                    inner_dict[token] = freq*(self.term_inv_doc_freq.get(token,1))
                    document_vectors[doc_id] = inner_dict

        for doc_id, document_vector in document_vectors.items():
            score = self.dot_product(document_vector,query_vector)
            posting_list.append([doc_id,score])

        posting_list.sort(key=lambda tup: tup[1],reverse=True)
        json_output['postings'] = posting_list[:self.MAX_ITEMS_RETURNED]
        self.write(json.dumps(json_output))

