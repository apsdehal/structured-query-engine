"""
N-gram Analyzer
Generate ngrams based on min (3) and max len (query_length) and return them
Currently doesn't support config for min and max len

Use analyzer: "n_gram" in mapping
"""
import nltk
from nltk.util import ngrams
from nltk.tokenize import word_tokenize
from . import BaseAnalyzer


class NgramAnalyzer(BaseAnalyzer.BaseAnalyzer):

    def __init__(self, config={}):
        super().__init__(config)

    def n_grams(self, query):
        terms = super().analyze(query)
        final_ngrams = list()
        for term in terms:
            if len(term) > 2:
                for n in range(3,len(term)+1):
                    final_ngrams.append(term[:n])
        min_len = 3
        for term in terms:
            max_len = max(4, len(term)+1)
            for n in range(min_len, max_len):
                col = ngrams(list(term), n)
                for ngram in col:
                    ngram = "".join(ngram)
                    if len(ngram):
                        final_ngrams.append(ngram)
        return final_ngrams

    def analyze(self, query):
        query = query.lower().strip()
        return self.n_grams(query)
