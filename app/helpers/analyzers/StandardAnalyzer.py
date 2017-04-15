"""
Standard Analyzer
"""

from . import BaseAnalyzer


class StandardAnalyzer(BaseAnalyzer):

    def __init__(self, config):
        super().__init__(config)

    def analyze(self, query):
        return super().analyze(query)
