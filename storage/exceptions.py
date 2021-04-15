class ChunkNotFound(Exception):
    message = "Chunk was not found"


class InvalidFilePath(Exception):
    message = "Invalid file path"


class InvalidFileChunk(Exception):
    message = "Invalid file chunk"


class DataNodeNotSaved(Exception):
    message = "The passed data node is not saved in the database"


class NotEnoughSpace(Exception):
    message = "Not enough space in the data node"
