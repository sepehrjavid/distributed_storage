class InvalidClientActionConfigFile(Exception):
    def __init__(self, filed):
        message = "{fields} is missing in the dfs.conf file".format(fields=filed)
        super().__init__(message)
