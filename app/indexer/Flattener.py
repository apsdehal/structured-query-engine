import copy


class Flattener:
    def __init__(self, mapping):
        self._original_mapping = mapping
        self.flattened_mapping_ = {}

        for i in mapping:
            self.flattened_mapping_[i] = self.flattenMapping(mapping[i])

    def getFlattenedMapping(self):
        return self.flattened_mapping_

    def flattenMapping(self, config_mapping):
        mapping = copy.deepcopy(config_mapping)
        out = {}

        def flatten(x, name=''):
            keys = x.keys()
            if len(keys) == 1 and "properties" in keys:
                flatten(x["properties"], name)
            elif "type" in keys and type(x["type"]) is not dict:
                if x["type"] != "nested":
                    out[name[:-1]] = x
                else:
                    flatten(x["properties"], name)
            elif type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '.')
            else:
                out[name[:-1]] = x

        flatten(mapping)
        return out

    def flatten(self, doc_type, doc):
        current_type_mapping = self.flattened_mapping_.get(doc_type, None)

        if not current_type_mapping:
            raise ValueError(type + " not found in mapping")

        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '.')
            else:
                out[name[:-1]] = x

        flatten(doc)

        for key in list(out):
            if not current_type_mapping.get(key, None):
                out.pop(key, None)

        return out
