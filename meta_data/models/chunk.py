from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from meta_data.models.file import File


class Chunk:
    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.sequence = kwargs.get("sequence")
        self.local_path = kwargs.get("local_path")
        self.chunk_size = kwargs.get("chunk_size")
        self.data_node_id = kwargs.get("data_node_id")
        self.file_id = kwargs.get("file_id")

    def __create(self):
        self.id = self.db.create(
            "INSERT INTO chunk (sequence, local_path, chunk_size, data_node_id, file_id) VALUES (?,?,?,?,?);",
            self.sequence, self.local_path, self.chunk_size, self.data_node_id, self.file_id)

    def __update(self):
        pass

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def delete(self):
        self.db.execute("DELETE FROM chunk WHERE id = ?;", self.id)

    @property
    def file(self):
        return File.fetch_by_id(self.file_id, self.db)

    @property
    def data_node(self):
        return DataNode.fetch_by_id(self.data_node_id, self.db)
