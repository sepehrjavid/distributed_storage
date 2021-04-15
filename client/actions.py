from client.exceptions import InvalidClientActionConfigFile


class ClientActions:
    CONFIG_FILE_PATH = "/Users/sepehrjavid/Desktop/distributed_storage/client/dfs.conf"
    MANDATORY_FIELDS = ["data_node_network"]

    def __init__(self, username):
        self.username = username
        self.configuration = None
        self.update_config_file()
        print(self.configuration)

    def update_config_file(self):
        with open(self.CONFIG_FILE_PATH, "r") as config_file:
            config = config_file.read().replace('\n', '').replace('\t', '').replace(' ', '')

        if config[-2] == ",":
            config = config[:-2] + config[-1]

        self.configuration = self.parse_config(config)

    @staticmethod
    def parse_config(config):
        config_data = config[1:-1]

        properties = config_data.split(',')
        result_config = {}
        for prop in properties:
            result_config[prop.split(':')[0]] = prop.split(':')[1]

        ClientActions.validate_config(result_config)
        return result_config

    @staticmethod
    def validate_config(config):
        keys = config.keys()
        for field in ClientActions.MANDATORY_FIELDS:
            if field not in keys:
                print(str(InvalidClientActionConfigFile(field)))
                raise InvalidClientActionConfigFile(field)

    def send_file(self):
        pass
