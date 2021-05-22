from meta_data.database import MetaDatabase
from meta_data.models.directory import Directory
from meta_data.models.file import File
from meta_data.models.user import User


class Permission:
    READ_ONLY = "r_"
    WRITE_ONLY = "_w"
    READ_WRITE = "rw"
    ALLOWED_PERMISSIONS = [READ_ONLY, WRITE_ONLY, READ_WRITE]

    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.perm = kwargs.get("perm")
        self.user_id = kwargs.get("user_id")
        self.directory_id = kwargs.get("directory_id")
        self.file_id = kwargs.get("file_id")

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def __create(self):
        self.id = self.db.create("INSERT INTO permission (perm, file_id, directory_id, user_id) VALUES (?, ?, ?, ?);",
                                 self.perm, self.file_id, self.directory_id, self.user_id)

    def __update(self):
        self.db.execute("UPDATE permission SET perm=? WHERE id=?;", self.perm, self.id)

    def delete(self):
        self.db.execute("DELETE FROM permission WHERE id=?;", self.id)

    @property
    def directory(self):
        return Directory.fetch_by_id(self.directory_id, self.db)

    @property
    def file(self):
        return File.fetch_by_id(self.file_id, self.db)

    @property
    def user(self):
        return User.fetch_by_id(self.user_id, self.db)
