from meta_data.database import MetaDatabase
from meta_data.models.directory import Directory
from meta_data.models.file import File
from meta_data.models.user import User


class Permission:
    READ_ONLY = "_r_"
    WRITE_ONLY = "__w"
    READ_WRITE = "_rw"
    OWNER = "o__"
    ALLOWED_PERMISSIONS = [READ_ONLY, WRITE_ONLY, READ_WRITE, OWNER]

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

    @staticmethod
    def fetch_by_username_directory_id(username, directory_id, db: MetaDatabase):
        result = db.fetch("""SELECT permission.* FROM permission INNER JOIN users u ON permission.user_id = u.id 
        WHERE directory_id=? AND username=?;""", directory_id, username)

        if len(result) == 0:
            return None

        data = result[0]
        print(data)
        return Permission(db=db, id=data[0], perm=data[1], file_id=data[2], directory_id=data[3], user_id=data[4])
