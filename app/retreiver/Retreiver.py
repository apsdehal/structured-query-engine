import json
import app.helpers.utils.General as utils
import requests

class Retreiver:
    def __init__(self, config):
        self.STANDARD_ANALYZER = 'standard'
        self.TERM_QUERY = 'term'
        self.MATCH_QUERY = 'match'
        self.BOOL_QUERY = 'bool'
        self.config = config
        return


    def dot_product(self, vector1, vector2):
        result = 0
        for key,value in vector1.items():
            result = result + value*vector2.get(key,0)
        return result


    def process_query(self, data):
        if self.TERM_QUERY in data:
            items = data[self.TERM_QUERY].items()
            if len(items) > 1:
                print('[term] query doesnt support multiple fields')
                return
            [(f,q)] = items
            fields = [f]
            query_strings = [q]
            query_type = self.TERM_QUERY
        elif self.MATCH_QUERY in data:
            items = data[self.MATCH_QUERY].items()
            if len(items) > 1:
                print('[match] query doesnt support multiple fields')
                return
            [(f,q)] = items
            fields = [f]
            query_strings = [q]
            query_type = self.MATCH_QUERY
        elif self.BOOL_QUERY in data:
            items = data[self.BOOL_QUERY].items()
            must_items = items.get('must',[])
            must_not_items = items.get('must_not',[])
            should_items = items.get('should',[])
            range_items = items.get('range',[])

            # should query
            should_fields = []
            should_field_querys = []
            if len(should_items) == 0 :
                should_query = None
            else :
                for should_item in should_items :
                    if len(should_item[self.MATCH_QUERY].items()) > 1:
                        print('[match] query doesnt support multiple fields')
                        return
                    [(f,q)] = should_item[self.MATCH_QUERY].items()
                    should_fields.append(f)
                    should_field_querys.append(q)

            # must query
            must_fields = []
            must_field_querys = []
            if len(must_items) == 0 :
                must_query = None
            else :
                for must_item in must_items :
                    if len(must_item[self.MATCH_QUERY].items()) > 1:
                        print('[match] query doesnt support multiple fields')
                        return
                    [(f,q)] = must_item[self.MATCH_QUERY].items()
                    must_fields.append(f)
                    must_field_querys.append(q)

            # only implementing "should" for now
            fields = []
            query_strings = []
            for f,q in zip(should_fields,should_field_querys):
                fields.append(f)
                query_strings.append(q)

            query_type = self.BOOL_QUERY
        else:
            print('unknown query type')
            return
        return fields, query_strings, query_type


    def query(self, index_name, type_name, q):
        try:
            data = q['query']
        except KeyError:
            print('invalid query')
            return
        try:
            fields, query_strings, query_type = self.process_query(data)
        except TypeError:
            print('Exception occured while processing query')
            return

        mapping = self.config[index_name]['mapping']

        scores = {}

        for field,query_string in zip(fields,query_strings):
            if query_type == self.TERM_QUERY:
                query_tokens = [query_string.lower()]
            else :
                analyzer_type = mapping['movie']['properties'][field].get('analyzer',self.STANDARD_ANALYZER)
                analyzer = utils.getAnalyzer(analyzer_type)
                query_tokens = analyzer.analyze(query_string)

            query_vector = {}
            document_vectors = {}
            posting_list = []

            for token in query_tokens:
                #if query contains a word twice then it will be ignored second time
                if token in query_vector:
                    continue
                for index_server_url in self.config['index_server_url']:
                    # need to get idf somehow
                    self.term_inv_doc_freq = float(requests.get(query_url))
                    query_vector[token] = 1.0 * self.term_inv_doc_freq
                    tf_list = self.config['tf_list'][field].get(token,[])
                    for doc_id,freq in tf_list:
                        if doc_id in document_vectors:
                            inner_dict = document_vectors[doc_id]
                            inner_dict[token] = freq * self.term_inv_doc_freq
                        else:
                            inner_dict = {}
                            inner_dict[token] = freq * self.term_inv_doc_freq
                            document_vectors[doc_id] = inner_dict

            for doc_id, document_vector in document_vectors.items():
                score = self.dot_product(document_vector,query_vector)
                scores[doc_id] = scores.get(doc_id,0) + score

        for doc_id, score in scores.items():    
            posting_list.append([doc_id,score])

        posting_list.sort(key=lambda tup: tup[1],reverse=True)
        return posting_list

