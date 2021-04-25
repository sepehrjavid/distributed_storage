import sqlite3


class MetaDatabase:
    DATABASE_PATH = "meta_data.db"

    def __init__(self):
        self.connection = sqlite3.connect(MetaDatabase.DATABASE_PATH)
        self.cursor = self.connection.cursor()

    @staticmethod
    def initialize_tables():
        connection = sqlite3.connect(MetaDatabase.DATABASE_PATH)
        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS users (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                    username VARCHAR(100) NOT NULL UNIQUE,
                                    password VARCHAR(100) NOT NULL 
                                        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS directory (
                                                    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                                    title VARCHAR(200) NOT NULL,
                                                    parent_directory_id INTEGER,
                                                    FOREIGN KEY (parent_directory_id) REFERENCES directory (id)
                                                        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS file (
                                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                            title VARCHAR(200) NOT NULL,
                                            extension VARCHAR(10),
                                            is_complete INTEGER NOT NULL DEFAULT 0,
                                            directory_id INTEGER NOT NULL,
                                            sequence_num INTEGER NOT NULL,
                                            FOREIGN KEY (directory_id) REFERENCES directory (id)
                                                );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS permission (
                                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                            perm VARCHAR(2) NOT NULL,
                                            file_id INTEGER,
                                            directory_id INTEGER,
                                            user_id INTEGER NOT NULL,
                                            FOREIGN KEY (user_id) REFERENCES users (id),
                                            FOREIGN KEY (file_id) REFERENCES file (id),
                                            FOREIGN KEY (directory_id) REFERENCES directory (id),
                                            UNIQUE (user_id, directory_id),
                                            UNIQUE (user_id, file_id)
                                                );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS data_node (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            ip_address VARCHAR(15) NOT NULL UNIQUE,
                            rack_number INTEGER NOT NULL,
                            available_byte_size INTEGER NOT NULL,
                            last_seen VARCHAR(17) NOT NULL DEFAULT 0
                                );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS chunk (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            sequence INTEGER NOT NULL,
                            local_path VARCHAR(500) NOT NULL,
                            chunk_size INTEGER NOT NULL,
                            data_node_id INTEGER NOT NULL,
                            file_id INTEGER NOT NULL,
                            FOREIGN KEY (data_node_id) REFERENCES data_node (id),
                            FOREIGN KEY (file_id) REFERENCES file (id)
                                );""")

        connection.close()

    def create(self, command, *args):
        row_id = self.cursor.execute(command, args).lastrowid
        self.connection.commit()
        return row_id

    def execute(self, command, *args):
        self.cursor.execute(command, args)
        self.connection.commit()

    def fetch(self, command, *args):
        return self.cursor.execute(command, args).fetchall()

    def close(self):
        self.connection.close()
