from meta_data.database import MetaDatabase
from meta_data.models.directory import Directory


class User:
    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")

    def __create(self):
        self.id = self.db.create("INSERT INTO users (username, password) VALUES (?, ?);", self.username, self.password)

    def __update(self):
        self.db.execute("UPDATE data_node SET password=? WHERE id=?;", self.password, self.id)

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def delete(self):
        self.db.execute("DELETE FROM users WHERE id=?", self.id)
        self.id = None

    def get_directories(self):
        return Directory.fetch_by_username(username=self.username, db=self.db)

    @staticmethod
    def fetch_by_id(id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM users WHERE id=?;", id)[0]
        return User(db=db, id=result[0], username=result[1], password=result[2])

    @staticmethod
    def fetch_by_username(username, db: MetaDatabase):
        result = db.fetch("SELECT * FROM users WHERE username=?;", username)

        if len(result) == 0:
            return None

        result = result[0]
        return User(db=db, id=result[0], username=result[1], password=result[2])

    @staticmethod
    def fetch_all(db: MetaDatabase):
        result = db.fetch("SELECT * FROM users;")

        users = []
        for user in result:
            users.append(User(db=db, id=user[0], username=user[1], password=user[2]))

        return users
