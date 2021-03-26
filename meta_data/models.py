from meta_data.database import MetaDatabase


class DataNode:
    db = MetaDatabase.get_database_instance()

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.ip_address = kwargs.get("ip_address")
        self.rack_number = kwargs.get("rack_number")
        self.available_byte_size = kwargs.get("available_byte_size")
        self.last_seen = kwargs.get("last_seen")

    def __create(self):
        connection = self.db.connection
        self.id = connection.cursor().execute(
            "INSERT INTO data_node (ip_address, rack_number, available_byte_size) VALUES (?,?,?);",
            (self.ip_address, self.rack_number, self.available_byte_size)
        ).lastrowid
        connection.commit()

    def __update(self):
        connection = self.db.connection
        connection.cursor().execute("UPDATE data_node SET available_byte_size=?, last_seen=? WHERE id=?",
                                    (self.available_byte_size, self.last_seen, self.id))
        connection.commit()

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def delete(self, **kwargs):
        connection = self.db.connection
        connection.cursor().execute("DELETE FROM data_node WHERE id = ?;", (self.id,))
        connection.commit()

    def __str__(self):
        return f"{self.id} | {self.ip_address} | {self.rack_number} | {self.available_byte_size} | {self.last_seen}"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def fetch_all():
        connection = DataNode.db.connection
        sql_result = connection.cursor().execute("SELECT * FROM data_node;").fetchall()

        data_nodes = []
        for data_node in sql_result:
            data_nodes.append(DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                                       available_byte_size=data_node[3], last_seen=float(data_node[4])))

        return data_nodes

    @staticmethod
    def fetch_by_ip(ip_address):
        connection = DataNode.db.connection
        sql_result = connection.cursor().execute("SELECT * FROM data_node WHERE ip_address=?;",
                                                 (ip_address,)).fetchall()
        if len(sql_result) == 0:
            return None

        data_node = sql_result[0]
        return DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                        available_byte_size=data_node[3], last_seen=float(data_node[4]))

    @staticmethod
    def fetch_by_id(id):
        connection = DataNode.db.connection
        sql_result = connection.cursor().execute("SELECT * FROM data_node WHERE id=?;",
                                                 (id,)).fetchall()
        if len(sql_result) == 0:
            return None

        data_node = sql_result[0]
        return DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                        available_byte_size=data_node[3], last_seen=float(data_node[4]))


class ChunkMetadata:
    db = MetaDatabase.get_database_instance()

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.sequence = kwargs.get("sequence")
        self.local_path = kwargs.get("local_path")
        self.chunk_size = kwargs.get("chunk_size")
        self.data_node = kwargs.get("data_node")

    def __create(self):
        connection = self.db.connection
        self.id = connection.cursor().execute(
            "INSERT INTO chunk_metadata (sequence, local_path, chunk_size, data_node_id) VALUES (?,?,?,?);",
            (self.sequence, self.local_path, self.chunk_size, self.data_node.id)
        ).lastrowid
        connection.commit()

    def __update(self):
        connection = self.db.connection
        connection.cursor().execute(
            "UPDATE data_node SET sequence=?, local_path=?, chunk_size=?, data_node_id=? WHERE id=?",
            (self.sequence, self.local_path, self.chunk_size, self.data_node.id, self.id))
        connection.commit()

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()