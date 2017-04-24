import json
import helpers.utils.General as utils
import requests
import math


class Retreiver:
    def __init__(self, config, index_name):
        self.STANDARD_ANALYZER = 'standard'
        self.TERM_QUERY = 'term'
        self.MATCH_QUERY = 'match'
        self.BOOL_QUERY = 'bool'
        self.NUM_RESULTS_TO_RETURN = 10
        self.index_name = index_name
        self.config = config
        self.mapping = config['indices'][index_name]["mappings"]
        self.flattener = Flattener(self.mapping)
        self.mapping = self.flattener.getFlattenedMapping()
        self.number_of_shards = config['indices'][index_name]["settings"]["number_of_shards"]
        self.doc_stores, self.inverted_indices = utils.loadDocStoreAndInvertedIndex(index_name, self.number_of_shards, config, self.mapping)

    def dot_product(self, vector1, vector2):
        result = 0
        for key, value in vector1.items():
            result = result + value * vector2.get(key, 0)
        return result

    def process_query(self, data):
        if self.TERM_QUERY in data:
            items = data[self.TERM_QUERY].items()
            if len(items) > 1:
                raise Exception('[term] query doesnt support multiple fields')
                return
            [(f, q)] = items
            fields = [f]
            query_strings = [q]
            query_type = self.TERM_QUERY
        elif self.MATCH_QUERY in data:
            items = data[self.MATCH_QUERY].items()
            if len(items) > 1:
                raise Exception('[match] query doesnt support multiple fields')
                return
            [(f, q)] = items
            fields = [f]
            query_strings = [q]
            query_type = self.MATCH_QUERY
        elif self.BOOL_QUERY in data:
            items = data[self.BOOL_QUERY].items()
            must_items = items.get('must', [])
            must_not_items = items.get('must_not', [])
            should_items = items.get('should', [])
            range_items = items.get('range', [])

            # should query
            should_fields = []
            should_field_querys = []
            if len(should_items) == 0:
                should_query = None
            else:
                for should_item in should_items:
                    if len(should_item[self.MATCH_QUERY].items()) > 1:
                        raise Exception('[match] query doesnt support multiple fields')
                        return
                    [(f, q)] = should_item[self.MATCH_QUERY].items()
                    should_fields.append(f)
                    should_field_querys.append(q)

            # must query
            must_fields = []
            must_field_querys = []
            if len(must_items) == 0:
                must_query = None
            else:
                for must_item in must_items:
                    if len(must_item[self.MATCH_QUERY].items()) > 1:
                        raise Exception('[match] query doesnt support multiple fields')
                        return
                    [(f, q)] = must_item[self.MATCH_QUERY].items()
                    must_fields.append(f)
                    must_field_querys.append(q)

            # only implementing "should" for now
            fields = []
            query_strings = []
            for f, q in zip(should_fields, should_field_querys):
                fields.append(f)
                query_strings.append(q)

            query_type = self.BOOL_QUERY
        else:
            raise Exception('unknown query type')
            return
        return fields, query_strings, query_type

    def get_docs(self, posting_list, type_name):
        results = {}
        sub_results = {}
        hits = []
        max_score = -1
        total_results = min(self.NUM_RESULTS_TO_RETURN, len(posting_list))

        for doc_id, score in posting_list[:self.NUM_RESULTS_TO_RETURN]:
            max_score = max(score,max_score)
            shard_num = doc_id % self.number_of_shards
            doc = {}
            doc['_index'] = self.index_name
            doc['_type'] = type_name
            doc['_source'] = self.doc_stores[type_name][self.index_name][type_name][shard_num][doc_id]
            doc['_score'] = score
            doc['_id'] = doc_id
            hits.append(doc)

        sub_results['hits'] = hits
        sub_results['max_score'] = max_score
        sub_results['total'] = total_results
        results['hits'] = sub_results
        return results

    def query(self, type_name, q):
        try:
            data = q['query']
        except KeyError:
            raise KeyError("invalid query, 'query' key not present in passed parameter")
            return
        try:
            fields, query_strings, query_type = self.process_query(data)
        except TypeError:
            raise TypeError("Exception occured while processing query")
            return

        scores = {}

        for field, query_string in zip(fields, query_strings):
            if query_type == self.TERM_QUERY:
                query_tokens = [query_string.lower()]
            else:
                analyzer_type = self.mapping[type_name][field].get('analyzer', self.STANDARD_ANALYZER)
                analyzer = utils.getAnalyzer(analyzer_type)
                query_tokens = analyzer.analyze(query_string)

            query_vector = {}
            document_vectors = {}
            posting_list = []

            for token in query_tokens:
                # if query contains a word twice then it will be ignored second time
                if token in query_vector:
                    continue
                for i in range(self.number_of_shards):
                    query_vector[token] = 1.0 * self.term_inv_doc_freq
                    tf_dict = self.inverted_indices[type_name][self.index_name][type_name][i][field].get(token, {})

                    total_docs = float(self.doc_stores[type_name][self.index_name][type_name][i]['num_docs'])
                    self.term_inv_doc_freq = math.log(total_docs / tf_dict['num_docs'])
                    my_dict.pop('num_docs', None)

                    for doc_id, freq in tf_dict.items():
                        if doc_id in document_vectors:
                            inner_dict = document_vectors[doc_id]
                            inner_dict[token] = freq * self.term_inv_doc_freq
                        else:
                            inner_dict = {}
                            inner_dict[token] = freq * self.term_inv_doc_freq
                            document_vectors[doc_id] = inner_dict

            for doc_id, document_vector in document_vectors.items():
                score = self.dot_product(document_vector, query_vector)
                scores[doc_id] = scores.get(doc_id, 0) + score

        for doc_id, score in scores.items():
            posting_list.append((doc_id, score))

        posting_list.sort(key=lambda tup: tup[1], reverse=True)

        return self.get_docs(posting_list, type_name)
