import os
import json
from app.helpers.utils.Compressor import Compressor

compressor = Compressor()


def getAnalyzer(analyzer_type="standard"):
    analyzer_type = analyzer_type.lower()
    from app.helpers.analyzers import StandardAnalyzer
    analyzer = StandardAnalyzer.StandardAnalyzer()

    if analyzer_type == "n_gram":
        from app.helpers.analyzers import NgramAnalyzer
        analyzer = NgramAnalyzer.NgramAnalyzer()
    elif analyzer_type == "whitespace":
        from app.helpers.analyzers import WhitespaceAnalyzer
        analyzer = WhitespaceAnalyzer.WhitespaceAnalyzer()
    elif analyzer_type == "simple":
        from app.helpers.analyzers import SimpleAnalyzer
        analyzer = SimpleAnalyzer.SimpleAnalyzer()
    return analyzer


def loadDocStoreAndInvertedIndex(index_name, num_shards, config, mapping):
    indices_path = config["indices_path"]
    inverted_indices = dict()
    document_stores = dict()

    file_path = os.path.join(indices_path, index_name)
    if os.path.exists(file_path) and len(os.listdir(file_path)) > 1:
        for type_name in mapping:
            inverted_indices[type_name] = [dict() for x in range(num_shards)]

            document_stores[type_name] = [dict() for x in range(num_shards)]

            for i in range(num_shards):
                type_file_path = "%s_%s_%s.tf" % (index_name, type_name, i)
                type_file_path = os.path.join(file_path, type_file_path)
                with open(type_file_path, "rb") as f:
                    inverted_indices[type_name][i] = json.loads(compressor.decompress(f.read()).decode())

                type_file_path = "%s_%s_%s.ds" % (index_name, type_name, i)
                type_file_path = os.path.join(file_path, type_file_path)
                with open(type_file_path, "rb") as f:
                    document_stores[type_name][i] = json.loads(compressor.decompress(f.read()).decode())

    return document_stores, inverted_indices
