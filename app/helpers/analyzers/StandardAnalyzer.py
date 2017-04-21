"""
Standard Analyzer
"""

from . import BaseAnalyzer


class StandardAnalyzer(BaseAnalyzer.BaseAnalyzer):

    def __init__(self, config={}):
        super().__init__(config)

    def analyze(self, query):
        query = query.lower()
        return super().analyze(query)
