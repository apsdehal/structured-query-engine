import os
from app.helpers.utils.Compressor import Compressor

compressor = Compressor()


def getAnalyzer(analyzer_type="standard"):
    analyzer_type = analyzer_type.lower()
    if analyzer_type == "standard":
        from app.helpers.analyzers import StandardAnalyzer
        analyzer = StandardAnalyzer.StandardAnalyzer()

    return analyzer


def loadDocStoreAndInvertedIndex(index_name, num_shards, config, mapping):
    indices_path = config["indices_path"]
    inverted_indices = {}
    document_stores = {}

    for type_name in mapping:
        if type_name not in inverted_indices:
            inverted_indices[type_name] = []

        if type_name not in document_stores:
            document_stores[type_name] = []

        file_path = os.path.join(indices_path, index_name)
        for i in range(num_shards):
            type_file_path = "%s_%s_%s.tf" % (index_name, type_name, i)
            type_file_path = os.path.join(file_path, type_file_path)
            with open(type_file_path, "rb") as f:
                inverted_indices[type_name].append(compressor.decompress(f.read()))

            type_file_path = "%s_%s_%s.ds" % (index_name, type_name, i)
            type_file_path = os.path.join(file_path, type_file_path)
            with open(type_file_path, "rb") as f:
                document_stores[type_name].append(compressor.decompress(f.read()))

    return document_stores, inverted_indices
