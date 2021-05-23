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

    @staticmethod
    def fetch_by_file_id(file_id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM chunk WHERE file_id=?;", file_id)

        if len(result) == 0:
            return None

        chunks = []
        for data in result:
            chunks.append(
                Chunk(db=db, id=data[0], sequence=int(data[1]), local_path=data[2], chunk_size=data[3],
                      data_node_id=data[4], file_id=data[5])
            )

        return chunks

    @staticmethod
    def fetch_by_file_id_data_node_id_sequence(file_id, data_node_id, sequence, db: MetaDatabase):
        result = db.fetch("SELECT * FROM chunk WHERE file_id=? AND data_node_id=? AND sequence=?;", file_id,
                          data_node_id, sequence)

        if len(result) == 0:
            return None

        data = result[0]
        return Chunk(db=db, id=data[0], sequence=int(data[1]), local_path=data[2], chunk_size=data[3],
                     data_node_id=data[4], file_id=data[5])
