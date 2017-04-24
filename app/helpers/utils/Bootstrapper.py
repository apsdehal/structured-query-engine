import os
import json
import uuid


class Bootstrapper:
    def bootstrap(self, base_path):
        config = {}
        config["base_path"] = base_path
        # We can change this later if we want
        data_path = os.path.join(base_path, ".data")
        config["data_path"] = data_path
        indices_path = os.path.join(data_path, "indices")
        config["indices_path"] = indices_path

        if not os.path.exists(indices_path):
            os.makedirs(indices_path)

        files = os.listdir(indices_path)
        config["indices"] = {}
        for d in files:
            info_path = os.path.join(indices_path, d, "info")
            with open(info_path, "r") as info:
                config["indices"][d] = json.loads(info.read())

        config_path = os.path.join(data_path, "config")

        if not os.path.exists(config_path):
            general_config = {}
            BASE_PORT = 9400
            general_config["cluster_name"] = "elasticsearch"
            general_config["cluster_uuid"] = uuid.uuid4().hex
            general_config["base_port"] = BASE_PORT
            general_config["base_url"] = "http://localhost"
            with open(config_path, "w+") as c:
                c.write(json.dumps(general_config))
        else:
            with open(config_path, "r") as c:
                general_config = json.loads(c.read())

        config["name"] = uuid.uuid4().hex
        config.update(general_config)
        return config
