class InvalidClientActionConfigFile(Exception):
    def __init__(self, filed):
        message = "{fields} is missing in the dfs.conf file".format(fields=filed)
        super().__init__(message)


class FileSystemDown(Exception):
    def __init__(self):
        super().__init__("File system appears to be down")
