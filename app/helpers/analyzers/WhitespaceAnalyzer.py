"""
Whitespace Analyzer
Divides on whitespace and doesn't lowercase
Use analyzer: "whitespace" in mapping
"""

from . import BaseAnalyzer


class WhitespaceAnalyzer(BaseAnalyzer.BaseAnalyzer):

    def __init__(self, config={}):
        super().__init__(config)

    def analyze(self, query):
        query = query.strip()
        return query.split(" ")
