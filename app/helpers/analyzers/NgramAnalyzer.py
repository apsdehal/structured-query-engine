"""
N-gram Analyzer
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
        for term in terms:
            for n in range(len(term)):
                terms.append(term[:n])
        min_len = 3
        max_len = max(4,len(query)+1)
        for n in range(min_len, max_len):
            for ngram in ngrams(query, n):
                if ngram:
                    terms.append(''.join(str(i) for i in ngram))
        for n in range(len(query)):
            terms.append(query[:n])
        return terms

    def analyze(self, query):
        query = query.lower().strip()
        return self.n_grams(query)