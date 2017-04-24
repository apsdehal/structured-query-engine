import os
import json


class Bootstrapper:
    def bootstrap(self):
        config = {}
        # We can change this later if we want
        data_path = os.path.join(".", ".data")
        config["data_path"] = data_path
        indices_path = os.path.join(indices_path, "indices")
        config["indices_path"] = indices_path

        if not os.path.exists(indices_path):
            os.mkdirs(indices_path)

        files = os.listdir(indices_path)

        for d in dirs:
            info_path = os.path.join(d, "info")
            with open(info_path, "r") as info:
                config[os.path.dirname(d)] = json.loads(info.read())

        config_path = os.path.join(data_path, "config")

        if not os.path.exists(config_path):
            general_config = {}
            BASE_PORT = 9300
            general_config["base_port"] = BASE_PORT
            general_config["base_url"] = "http://localhost"
            with open(config_path, "w+") as c:
                c.write(json.dumps(general_config))
        else:
            with open(config_path, "r") as c:
                general_config = json.loads(c.read())

        config.update(general_config)
        return config
