class InvalidDataNodeConfigFile(Exception):
    def __init__(self):
        super().__init__("Invalid Config File")
