import json
import app.utils.General as utils
from nltk.tokenize import RegexpTokenizer

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

    def query(self, q):
        data = json.loads(q)['query']
        if TERM_QUERY in data:
            items = data[TERM_QUERY].items()
            if len(items) > 1:
                print('[term] query doesnt support multiple fields')
                return
            [(f,q)] = items
            fields = [f]
            query_strings = [q]
            query_type = TERM_QUERY
        elif MATCH_QUERY in data:
            items = data[MATCH_QUERY].items()
            if len(items) > 1:
                print('[match] query doesnt support multiple fields')
                return
            [(f,q)] = items
            fields = [f]
            query_strings = [q]
            query_type = MATCH_QUERY
        elif BOOL_QUERY in data:
            items = data[BOOL_QUERY].items()
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
                    if len(should_item[MATCH_QUERY].items()) > 1:
                        print('[match] query doesnt support multiple fields')
                        return
                    [(f,q)] = should_item[MATCH_QUERY].items()
                    should_fields.append(f)
                    should_field_querys.append(q)

            # must query
            must_fields = []
            must_field_querys = []
            if len(must_items) == 0 :
                must_query = None
            else :
                for must_item in must_items :
                    if len(must_item[MATCH_QUERY].items()) > 1:
                        print('[match] query doesnt support multiple fields')
                        return
                    [(f,q)] = must_item[MATCH_QUERY].items()
                    must_fields.append(f)
                    must_field_querys.append(q)

            # only implementing "should" for now
            fields = []
            query_strings = []
            for f,q in zip(should_fields,should_field_querys):
                fields.append(f)
                query_strings.append(q)

            query_type = BOOL_QUERY
        else:
            print('unknown query type')
            return

        with open(self.config.mapping_path) as handler:    
            mapping = json.load(handler)

        scores = {}

        for field,query_string in zip(fields,query_strings):
            if query_type == TERM_QUERY:
                query_tokens = [query_string.lower()]
            else :
                analyzer_type = mapping[field]['movie']['properties'].get('analyzer',STANDARD_ANALYZER)
                tokenizer = utils.getAnalyzer(analyzer_type)
                query_tokens = tokenizer.tokenize(query_string.lower())

            query_vector = {}
            document_vectors = {}
            json_output = {}
            posting_list = []

            for token in query_tokens:
                #if query contains a word twice then it will be ignored second time
                if token in query_vector:
                    continue
                query_vector[token] = 1.0*(self.config.term_inv_doc_freq.get(token,1.0))
                try:
                    tf_list = self.config.tf_list[field].get(token,[])
                except KeyError:
                    print('field {0} doesnt exist in tf_list'.format(field))
                    break
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
                scores[doc_id] = scores.get(doc_id,0) + score

        for doc_id, score in scores.items:    
            posting_list.append([doc_id,score])

        posting_list.sort(key=lambda tup: tup[1],reverse=True)
        return posting_list

