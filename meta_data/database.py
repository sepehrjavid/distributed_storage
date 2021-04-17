import sqlite3


class MetaDatabase:
    DATABASE_PATH = "meta_data.db"

    def __init__(self):
        self.initialize_tables()

    def initialize_tables(self):
        connection = sqlite3.connect(self.DATABASE_PATH)
        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS data_node (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            ip_address VARCHAR(15) NOT NULL UNIQUE,
                            rack_number INTEGER NOT NULL,
                            available_byte_size INTEGER NOT NULL,
                            last_seen VARCHAR(17) NOT NULL DEFAULT 0
                                );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS chunk_metadata (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            sequence INTEGER NOT NULL,
                            title VARCHAR(250) NOT NULL,
                            local_path VARCHAR(500) NOT NULL,
                            chunk_size INTEGER NOT NULL,
                            permission VARCHAR(32) NOT NULL,
                            data_node_id INTEGER NOT NULL,
                            FOREIGN KEY (data_node_id) REFERENCES data_node (id),
                            UNIQUE(permission, title, data_node_id, sequence),
                            UNIQUE(data_node_id, local_path)
                                );""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS next_chunk (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            current_chunk_id INTEGER NOT NULL,
                            next_chunk_id INTEGER NOT NULL,
                            FOREIGN KEY (current_chunk_id) REFERENCES chunk_metadata (id),
                            FOREIGN KEY (next_chunk_id) REFERENCES chunk_metadata (id)
                                );""")

        connection.close()
