from meta_data.database import MetaDatabase


class User:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")

    def __create(self):
        db = MetaDatabase()
        self.id = db.create("INSERT INTO users (username, password) VALUES (?, ?);", self.username, self.password)

    def __update(self):
        db = MetaDatabase()
        db.execute("UPDATE data_node SET username=?, password=? WHERE id=?;", self.username, self.password, self.id)

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def delete(self):
        db = MetaDatabase()
        db.execute("DELETE FROM users WHERE id=?", self.id)
        self.id = None

    @staticmethod
    def fetch_all():
        db = MetaDatabase()
        result = db.fetch("SELECT * FROM users;")

        users = []
        for user in result:
            users.append(User(id=user[0], username=user[1], password=user[2]))

        return users
