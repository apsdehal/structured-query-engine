import os
import json
from collections import defaultdict
from helpers.utils.Compressor import Compressor

compressor = Compressor()


class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


def getAnalyzer(analyzer_type="standard"):
    analyzer_type = analyzer_type.lower()
    if analyzer_type == "standard":
        from helpers.analyzers import StandardAnalyzer
        analyzer = StandardAnalyzer.StandardAnalyzer()

    return analyzer


def loadDocStoreAndInvertedIndex(index_name, num_shards, config, mapping):
    indices_path = config["indices_path"]
    inverted_indices = defaultdict(list) 
    document_stores = ddefaultict(list) 

    for type_name in mapping:

        file_path = os.path.join(indices_path, index_name)

        if os.path.exists(file_path) and len(os.listdir(file_path)) > 1:
            for i in range(num_shards):
                type_file_path = "%s_%s_%s.tf" % (index_name, type_name, i)
                type_file_path = os.path.join(file_path, type_file_path)
                with open(type_file_path, "rb") as f:
                    inverted_indices[type_name].append(json.loads(compressor.decompress(f.read()).decode()))

                type_file_path = "%s_%s_%s.ds" % (index_name, type_name, i)
                type_file_path = os.path.join(file_path, type_file_path)
                with open(type_file_path, "rb") as f:
                    document_stores[type_name].append(json.loads(compressor.decompress(f.read()).decode()))

    return document_stores, inverted_indices
