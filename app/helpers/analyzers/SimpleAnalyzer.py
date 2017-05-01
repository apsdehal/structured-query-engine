"""
Simple Analyzer

Takes in a string a splits it whenever it finds a non character word

Use analyzer: "simple" in mapping
"""

from . import BaseAnalyzer
import re


class SimpleAnalyzer(BaseAnalyzer.BaseAnalyzer):

    def __init__(self, config={}):
        super().__init__(config)

    def analyze(self, query):
        query = query.lower().strip()
        return re.split("[^a-zA-Z]", query)
