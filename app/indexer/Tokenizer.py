import app.helpers.utils.General as utils


class Tokenizer:
    def __init__(self, config, mapping):
        self.mapping_ = mapping
        return

    def tokenizeFlattened(self, doc_type, doc):
        out = {}
        type_mapping = self.mapping_.get(doc_type, None)

        if not type_mapping:
            raise ValueError(doc_type + " type not present in mapping")

        for key in list(doc):
            if key not in doc:
                raise ValueError(key + " not present in mapping")

            analyzer_type = type_mapping[key].get("analyzer", "standard")
            subtype = type_mapping[key].get("type", None)

            if not subtype:
                raise ValueError(key + " doesn't have type parameter")

            should_index = type_mapping[key].get("index", True)

            if not should_index:
                out[key] = doc[key]
                continue

            if type(doc[key]) is not list:
                doc[key] = [doc[key]]
            out[key] = []
            analyzer = utils.getAnalyzer(analyzer_type)
            for val in doc[key]:
                if subtype != "text":
                    out[key].append(val)
                else:
                    out[key].append(analyzer.analyze(val))

        return out
