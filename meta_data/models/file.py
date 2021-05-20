from meta_data.database import MetaDatabase
from meta_data.models.directory import Directory


class File:
    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.title = kwargs.get("title")
        self.extension = kwargs.get("extension")
        self.is_complete = False if kwargs.get("is_complete") is None else kwargs.get("is_complete")
        self.directory_id = kwargs.get("directory_id")
        self.sequence_num = kwargs.get("sequence_num")

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def __create(self):
        temp = 1 if self.is_complete else 0
        self.id = self.db.create("""INSERT INTO file (title, extension, is_complete, directory_id, sequence_num) VALUES
                                (?, ?, ?, ?, ?);""",
                                 self.title, self.extension, temp, self.directory_id, self.sequence_num)

    def __update(self):
        temp = 1 if self.is_complete else 0
        self.db.execute("UPDATE file SET title=?, is_complete=?, directory_id=? WHERE id=?;", self.title, temp,
                        self.directory_id, self.id)

    def delete(self):
        self.db.execute("DELETE FROM file WHERE id=?;", self.id)

    @property
    def directory(self):
        return Directory.fetch_by_id(self.directory_id, self.db)

    @staticmethod
    def fetch_by_id(id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM file WHERE id=?;", id)[0]

        temp = True if result[3] == 1 else False

        return File(db=db, id=result[0], title=result[1], extension=result[2], is_complete=temp, directory_id=result[4],
                    sequence_num=result[5])

    @staticmethod
    def fetch_by_dir_id(dir_id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM file WHERE directory_id=?;", dir_id)

        files = []
        for data in result:
            temp = True if data[3] == 1 else False
            files.append(
                File(db=db, id=data[0], title=data[1], extension=data[2], is_complete=temp, directory_id=data[4],
                     sequence_num=result[5])
            )

        return files

    def __del__(self):
        self.db.close()
