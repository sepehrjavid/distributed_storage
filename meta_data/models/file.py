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
        self.permission = kwargs.get("permission")

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def __create(self):
        temp = 1 if self.is_complete else 0
        self.id = self.db.create("INSERT INTO file (title, extension, is_complete, directory_id) VALUES (?, ?, ?, ?);",
                                 self.title, self.extension, temp, self.directory_id)

    def __update(self):
        temp = 1 if self.is_complete else 0
        self.db.execute("UPDATE file SET title=?, is_complete=?, directory_id=? WHERE id=?;", self.title, temp,
                        self.directory_id, self.id)

    def delete(self):
        self.db.execute("DELETE FROM file WHERE id=?;", self.id)

    @property
    def directory(self):
        return Directory.fetch_by_id(self.directory_id, self.db)
