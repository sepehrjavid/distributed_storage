from meta_data.database import MetaDatabase


class DataNode:
    def __init__(self, db: MetaDatabase, **kwargs):
        self.db = db
        self.id = kwargs.get("id")
        self.ip_address = kwargs.get("ip_address")
        self.rack_number = kwargs.get("rack_number")
        self.available_byte_size = kwargs.get("available_byte_size")
        self.last_seen = kwargs.get("last_seen")

    def __create(self):
        self.id = self.db.create(
            "INSERT INTO data_node (ip_address, rack_number, available_byte_size) VALUES (?,?,?);",
            self.ip_address, self.rack_number, self.available_byte_size)

    def __update(self):
        self.db.execute("UPDATE data_node SET available_byte_size=?, last_seen=? WHERE id=?",
                        self.available_byte_size, self.last_seen, self.id)

    def save(self):
        if self.id is None:
            self.__create()
        else:
            self.__update()

    def delete(self):
        self.db.execute("DELETE FROM data_node WHERE id = ?;", self.id)

    def __str__(self):
        return f"{self.id} | {self.ip_address} | {self.rack_number} | {self.available_byte_size} | {self.last_seen}"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def fetch_all(db: MetaDatabase):
        sql_result = db.fetch("SELECT * FROM data_node ORDER BY available_byte_size DESC;")

        data_nodes = []
        for data_node in sql_result:
            data_nodes.append(DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                                       available_byte_size=data_node[3], last_seen=float(data_node[4])))

        return data_nodes

    @staticmethod
    def fetch_by_ip(ip_address, db: MetaDatabase):
        sql_result = db.fetch("SELECT * FROM data_node WHERE ip_address=?;", ip_address)

        if len(sql_result) == 0:
            return None

        data_node = sql_result[0]
        return DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                        available_byte_size=data_node[3], last_seen=float(data_node[4]))

    @staticmethod
    def fetch_by_id(id, db: MetaDatabase):
        sql_result = db.fetch("SELECT * FROM data_node WHERE id=?;", id)

        if len(sql_result) == 0:
            return None

        data_node = sql_result[0]
        return DataNode(id=data_node[0], ip_address=data_node[1], rack_number=data_node[2],
                        available_byte_size=data_node[3], last_seen=float(data_node[4]))
