import json
import app.helpers.utils.General as utils
import requests
import math
from app.indexer.Flattener import Flattener
import sys


class Retriever:
    def __init__(self, config, index_name):
        self.STANDARD_ANALYZER = 'standard'
        self.TERM_QUERY = 'term'
        self.MATCH_QUERY = 'match'
        self.BOOL_QUERY = 'bool'
        self.index_name = index_name
        self.config = config
        self.mapping = config['indices'][index_name]["mappings"]
        self.flattener = Flattener(self.mapping)
        self.mapping = self.flattener.getFlattenedMapping()
        self.number_of_shards = config['indices'][index_name]["settings"]["index"]["number_of_shards"]
        if index_name in self.config["indexers"]:
            self.doc_stores, self.inverted_indices = self.config["indexers"][index_name].get_doc_store_ii()
        else:
            self.doc_stores, self.inverted_indices = utils.loadDocStoreAndInvertedIndex(index_name, self.number_of_shards, config, self.mapping)

    def update(self, doc_stores, inverted_indices):
        self.doc_stores = doc_stores
        self.inverted_indices = inverted_indices

    def dot_product(self, vector1, vector2):
        result = 0
        for key, value in vector1.items():
            result = result + value * vector2.get(key, 0)
        return result

    def process_should_query(self, should_item):
        if self.MATCH_QUERY in should_item:
            should_item = should_item[self.MATCH_QUERY]
            q_type = self.MATCH_QUERY
        elif self.TERM_QUERY in should_item:
            should_item = should_item[self.TERM_QUERY]
            q_type = self.TERM_QUERY
        boost = 1
        [(f, q)] = should_item.items()
        if type(q) is dict:
            boost = float(q.get('boost',1))
            q = q.get('query','')

        return f, q, boost, q_type

    def process_query(self, data):
        range_filter = []
        weights = []
        if self.TERM_QUERY in data:
            items = data[self.TERM_QUERY].items()
            if len(items) > 1:
                raise Exception('[term] query doesnt support multiple fields')
                return
            [(f, q)] = items
            fields = [f]
            query_strings = [q]
            query_types = [self.TERM_QUERY]
            weights = [1.0]
        elif self.MATCH_QUERY in data:
            items = data[self.MATCH_QUERY].items()
            if len(items) > 1:
                raise Exception('[match] query doesnt support multiple fields')
                return
            [(f, q)] = items
            fields = [f]
            query_strings = [q]
            query_types = [self.MATCH_QUERY]
            weights = [1.0]
        elif self.BOOL_QUERY in data:
            items = data[self.BOOL_QUERY]
            query_types = []
            must_items = items.get('must', [])
            must_not_items = items.get('must_not', [])
            should_items = items.get('should', [])
            filter_items = items.get('filter', [])

            # should query
            should_fields = []
            should_field_querys = []
            boost = []
            if len(should_items) == 0:
                should_query = None
            else:
                for should_item in should_items:
                    if len(should_item.get(self.MATCH_QUERY,{}).items()) > 1:
                        raise Exception('[match] query doesnt support multiple fields')
                        return
                    if len(should_item.get(self.TERM_QUERY,{}).items()) > 1:
                        raise Exception('[term] query doesnt support multiple fields')
                        return
                    f, q, b, q_type = self.process_should_query(should_item)
                    should_fields.append(f)
                    should_field_querys.append(q)
                    boost.append(b)
                    query_types.append(q_type)

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

            # filter query
            if len(filter_items) > 0:
                for filter_item in filter_items:
                    range_items = filter_item.get('range',{}).items()
                    if len(range_items) > 1:
                        raise Exception('[range] query doesnt support multiple fields')
                        return
                    if len(range_items) != 0:
                        [(range_query_field, q)] = range_items
                        lt = float(q.get('lt',sys.maxsize))
                        lte = float(q.get('lte',sys.maxsize))
                        gt = float(q.get('gt',-sys.maxsize))
                        gte = float(q.get('gte',-sys.maxsize))
                        range_filter.append({range_query_field:{'lt': lt, 'lte': lte, 'gt': gt, 'gte': gte }})

            # only implementing "should" and filter query for now
            # filter query will only have range as parameter
            fields = should_fields
            query_strings = should_field_querys
            weights = boost

        else:
            raise Exception('unknown query type')
            return
        return fields, query_strings, weights, query_types, range_filter

    def get_docs(self, posting_list, type_name, range_filter):
        results = {}
        sub_results = {}
        hits = []
        max_score = -1
        total_results = 0
        filtered_posting_list = []

        if len(range_filter) != 0:
            for doc_id, score in posting_list:
                filter_out = False
                shard_num = int(doc_id) % self.number_of_shards
                doc = {'_source': self.doc_stores[type_name][shard_num][doc_id] }

                for _filter in range_filter:
                    [(range_field, conds)] = _filter.items()
                    field_val_in_doc = float(doc['_source'][range_field])
                    if not field_val_in_doc <= conds['lte']:
                        filter_out = True
                        break
                    if not field_val_in_doc < conds['lt']:
                        filter_out = True
                        break
                    if not field_val_in_doc >= conds['gte']:
                        filter_out = True
                        break
                    if not field_val_in_doc > conds['gt']:
                        filter_out = True
                        break
                if not filter_out:
                    filtered_posting_list.append((doc_id, score))
        else:
            filtered_posting_list = posting_list

        for doc_id, score in filtered_posting_list[self.offset : ]:
            max_score = max(score,max_score)
            shard_num = int(doc_id) % self.number_of_shards
            doc = {}
            doc['_index'] = self.index_name
            doc['_type'] = type_name
            doc['_source'] = self.doc_stores[type_name][shard_num][doc_id]
            doc['_score'] = score
            doc['_id'] = doc_id

            hits.append(doc)
            total_results += 1
            if total_results == self.num_results:
                break

        sub_results['hits'] = hits
        sub_results['max_score'] = max_score
        sub_results['total'] = len(filtered_posting_list)
        results['hits'] = sub_results
        return results

    def query(self, type_name, q, optional_args={}):
        self.num_results = q.get('size', 10)
        self.offset = q.get('from', 0)
        # print(q)
        try:
            data = q['query']
        except KeyError:
            raise KeyError("invalid query, 'query' key not present in passed parameter")
            return
        try:
            fields, query_strings, weights, query_types, range_filter = self.process_query(data)
        except TypeError:
            raise TypeError("Exception occured while processing query")
            return

        scores = {}
        posting_list = []

        for field, query_string, weight, query_type in zip(fields, query_strings, weights, query_types):
            field_type = self.mapping[type_name][field]['type']
            if field_type == 'text':
                if query_type == self.TERM_QUERY:
                    query_tokens = [query_string.lower()]
                else:
                    analyzer_type = self.mapping[type_name][field].get('search_analyzer', None)
                    if not analyzer_type:
                        analyzer_type = self.mapping[type_name][field].get('analyzer', self.STANDARD_ANALYZER)

                    analyzer = utils.getAnalyzer(analyzer_type)
                    query_tokens = analyzer.analyze(query_string)
            else:
                query_tokens = [query_string]

            query_vector = {}
            document_vectors = {}

            for token in query_tokens:
                # if query contains a word twice then it will be ignored second time
                if token in query_vector:
                    continue

                total_docs = 0.0
                term_doc_freq = 0.0
                is_term_present = False
                for i in range(self.number_of_shards):
                    total_docs += float(self.doc_stores[type_name][i]['num_docs'])
                    result = self.inverted_indices[type_name][i][field].get(token, {})
                    if len(result) == 0:
                        continue
                    is_term_present = True
                    num_docs, tf_dict = result
                    term_doc_freq += num_docs
                if is_term_present:
                    term_inv_doc_freq = math.log(total_docs / term_doc_freq)


                for i in range(self.number_of_shards):
                    result = self.inverted_indices[type_name][i][field].get(token, {})
                    if len(result) == 0:
                        continue

                    num_docs, tf_dict = result 
                    query_vector[token] = weight * term_inv_doc_freq

                    for doc_id, freq in tf_dict.items():
                        if doc_id in document_vectors:
                            inner_dict = document_vectors[doc_id]
                            inner_dict[token] = freq * term_inv_doc_freq
                        else:
                            inner_dict = {}
                            inner_dict[token] = freq * term_inv_doc_freq
                            document_vectors[doc_id] = inner_dict

            for doc_id, document_vector in document_vectors.items():
                score = self.dot_product(document_vector, query_vector)
                scores[doc_id] = scores.get(doc_id, 0) + score

        for doc_id, score in scores.items():
            posting_list.append((doc_id, score))

        posting_list.sort(key=lambda tup: tup[1], reverse=True)

        return self.get_docs(posting_list, type_name, range_filter)
