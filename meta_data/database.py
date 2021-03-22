import sqlite3


class MetaDatabase:
    DATABASE_PATH = "meta_data.db"

    def __init__(self):
        self.connection = sqlite3.connect(self.DATABASE_PATH)
        self.initialize_tables()

    def initialize_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_node (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            ip_address VARCHAR(15) NOT NULL,
                            available_size_byte INTEGER NOT NULL
                        );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS chunk_metadata (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            local_path VARCHAR(250) NOT NULL,
                            chunk_size INTEGER NOT NULL,
                            data_node_id INTEGER NOT NULL,
                            FOREIGN KEY (peer_id) REFERENCES data_node (id)
                        );""")

    def insert_meta(self):
        pass

    def delete_meta(self):
        pass

    def edit_meta(self):
        pass

    def retrieve_meta(self):
        pass

    def insert_peer(self):
        pass

    def delete_peer(self):
        pass
