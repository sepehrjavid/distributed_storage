from meta_data.database import MetaDatabase
from meta_data.models.file import File


class Directory:
    MAIN_DIR_NAME = "main"

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

    def get_user_permission(self, username):
        result = self.db.fetch("""SELECT permission.perm FROM permission INNER JOIN directory d ON 
                                permission.directory_id = d.id WHERE d.id=?;""", self.id)[0]
        return result[0]

    @property
    def parent_directory(self):
        return Directory.fetch_by_id(self.parent_directory_id, self.db)

    @property
    def children(self):
        result = self.db.execute("SELECT * FROM directory WHERE parent_directory_id=?;", self.id)

        directories = []
        for data in result:
            directories.append(
                Directory(db=self.db, id=data[0], title=data[1], parent_directory_id=data[2])
            )
        return directories

    @property
    def files(self):
        if self.id is None:
            return []
        return File.fetch_by_dir_id(dir_id=self.id, db=self.db)

    @staticmethod
    def fetch_by_id(dir_id, db: MetaDatabase):
        result = db.fetch("SELECT * FROM directory WHERE id=?;", dir_id)[0]
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
                Directory(db=db, id=data[0], title=data[1], parent_directory_id=data[2])
            )
        return directories

    @staticmethod
    def fetch_user_main_directory(username, db: MetaDatabase):
        result = db.fetch("""SELECT directory.id, directory.title, directory.parent_directory_id, p.perm FROM directory
                            INNER JOIN permission p ON directory.id = p.directory_id INNER JOIN 
                            users u on p.user_id = u.id WHERE u.username=? AND directory.title=?;""", username,
                          Directory.MAIN_DIR_NAME)[0]
        return Directory(db=db, id=result[0], title=result[1], parent_directory_id=result[2])

    @staticmethod
    def find_path_directory(main_dir, path):
        path_dirs = path.split("/")

        if path_dirs[0] != Directory.MAIN_DIR_NAME:
            return

        path_dirs = path_dirs[1:]

        parent = main_dir
        for directory_name in path_dirs:
            child = list(filter(lambda x: x.title == directory_name, parent.children))
            if len(child) == 0:
                return
            parent = child[0]

        return parent

    def __del__(self):
        self.db.close()
