"""
Base Analyzer: Parent class for all analyzers
Converts string to simple token based array on whitespace and removes
puncutations
"""

from nltk.tokenize import word_tokenize


class BaseAnalyzer:
    def __init__(self, config={}):
        self.config = config

    def analyze(self, query):
        return word_tokenize(query)
