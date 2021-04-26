from meta_data.database import MetaDatabase


class Directory:
    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.title = kwargs.get("title")
        self.parent_directory_id = kwargs.get("parent_directory_id")

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def __create(self):
        self.id = self.db.create("INSERT INTO directory (title, parent_directory_id) VALUES (?, ?);", self.title,
                                 self.parent_directory_id)

    def __update(self):
        self.db.execute("UPDATE directory SET title=?, parent_directory_id=? WHERE id=?;", self.title,
                        self.parent_directory_id, self.id)

    def delete(self):
        self.db.execute("DELETE FROM directory WHERE id=?;", self.id)

    @property
    def parent_directory(self):
        return Directory.fetch_by_id(self.parent_directory_id, self.db)

    @staticmethod
    def fetch_by_id(id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM directory WHERE id=?;", id)[0]
        return Directory(db=db, id=result[0], title=result[1], parent_directory_id=result[2])

    @staticmethod
    def fetch_by_username(username, db: MetaDatabase):
        result = db.fetch(
            """SELECT directory.id, directory.title, directory.parent_directory_id, p.perm FROM directory INNER JOIN 
            permission p ON directory.id = p.directory_id INNER JOIN users u on p.user_id = u.id WHERE u.username=?;""",
            username)

        if len(result) == 0:
            return None

        directories = []
        for data in result:
            directories.append(
                Directory(db=db, id=data[0], title=data[1], parent_directory_id=data[2],
                          permission={"username": username, "perm": data[3]}))
        return directories

    def __del__(self):
        self.db.close()
