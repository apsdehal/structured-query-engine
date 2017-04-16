import copy


class Flattener:
    def __init__(self, mapping):
        self.flattened_mapping = {}

        for i in mapping:
            self.flattened_mapping[i] = self.flattenMapping(mapping[i])
        return

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
