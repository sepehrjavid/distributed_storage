from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
import os
import uuid

from singleton.singleton import Singleton
from storage.exceptions import DataNodeNotSaved, NotEnoughSpace
from valid_messages import UPDATE_DATA_NODE, MESSAGE_SEPARATOR


class Storage(metaclass=Singleton):
    CHUNK_SIZE = 64 * (10 ** 6)
    REPLICATION_FACTOR = 3

    def __init__(self, storage_path, current_data_node: DataNode, controller, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_path = storage_path
        self.db = MetaDatabase()
        self.controller = controller

        if DataNode.fetch_by_ip(current_data_node.ip_address, self.db) is not None:
            self.current_data_node = current_data_node
        elif current_data_node is not None:
            raise DataNodeNotSaved

    def is_valid_path(self, path):
        return self.storage_path in path and os.path.isfile(path)

    def choose_data_node_to_save(self, file_size, db):
        all_nodes = DataNode.fetch_all(db=db)

        if sum([x.available_byte_size for x in all_nodes]) < file_size:
            return None

        chunk_list = []
        assigned_nodes = set()
        i = 0
        bytes_assigned = 0
        while bytes_assigned < file_size:
            data_node = all_nodes[i % len(all_nodes)]
            if (file_size - bytes_assigned) >= self.CHUNK_SIZE and data_node.available_byte_size >= self.CHUNK_SIZE:
                chunk_list.append((self.CHUNK_SIZE, data_node.ip_address))
                assigned_nodes.add(data_node)
                bytes_assigned += self.CHUNK_SIZE
                data_node.available_byte_size -= self.CHUNK_SIZE
            elif (file_size - bytes_assigned) < self.CHUNK_SIZE and data_node.available_byte_size >= (
                    file_size - bytes_assigned):
                chunk_list.append((file_size - bytes_assigned, data_node.ip_address))
                assigned_nodes.add(data_node)
                bytes_assigned += (file_size - bytes_assigned)
                data_node.available_byte_size -= (file_size - bytes_assigned)
            else:
                chunk_list.append((data_node.available_byte_size, data_node.ip_address))
                assigned_nodes.add(data_node)
                bytes_assigned += data_node.available_byte_size
                data_node.available_byte_size = 0
            i += 1

        for d_node in assigned_nodes:
            d_node.save()

        return chunk_list

    def get_new_file_path(self):
        filepath = self.storage_path + str(uuid.uuid4()).replace(MESSAGE_SEPARATOR, "_")
        while os.path.isfile(filepath):
            filepath = self.storage_path + str(uuid.uuid4()).replace(MESSAGE_SEPARATOR, "_")

        return filepath

    def update_byte_size(self, byte_size, db: MetaDatabase):
        self.current_data_node.db = db
        self.current_data_node.available_byte_size += byte_size

        if self.current_data_node.available_byte_size < 0:
            raise NotEnoughSpace

        self.current_data_node.save()
        self.controller.inform_modification(
            UPDATE_DATA_NODE.format(available_byte_size=self.current_data_node.available_byte_size,
                                    ip_address=self.current_data_node.ip_address,
                                    rack_number=self.current_data_node.rack_number,
                                    priority=self.current_data_node.priority,
                                    signature=self.current_data_node.ip_address
                                    ))

    def remove_chunk_file(self, path, db: MetaDatabase):
        if self.is_valid_path(path):
            chunk_size = os.path.getsize(path)
            os.remove(path)
            self.update_byte_size(chunk_size, db)

    def get_replication_data_nodes(self, chunk_size, db: MetaDatabase):
        all_data_nodes = DataNode.fetch_all(db=db)
        other_data_nodes = [x for x in all_data_nodes if x.id != self.current_data_node.id]
        racks = {}

        if len(other_data_nodes) == 0:
            return []

        for data_node in other_data_nodes:
            if data_node.rack_number not in racks:
                racks[data_node.rack_number] = []
            racks[data_node.rack_number].append(data_node)

        if len(racks) == 1:
            result = []
            for data_node in racks[other_data_nodes[0].rack_number]:
                if data_node.available_byte_size > chunk_size:
                    result.append(data_node)
            return [x.ip_address for x in result[:self.REPLICATION_FACTOR - 1]]

        result = []
        maximum_rack_node = max([len(racks[x]) for x in racks])
        i = 0
        while len(result) < self.REPLICATION_FACTOR - 1 and i < maximum_rack_node:
            for rack_number in racks:
                if len(result) == self.REPLICATION_FACTOR - 1:
                    return [x.ip_address for x in result]

                try:
                    if self.current_data_node.rack_number != rack_number and \
                            chunk_size < racks[rack_number][i].available_byte_size:
                        result.append(racks[rack_number][i])
                except IndexError:
                    pass
            i += 1

        add_on = []
        if len(result) < self.REPLICATION_FACTOR - 1:
            try:
                current_rack_nodes = racks[self.current_data_node.rack_number]
            except KeyError:
                return [x.ip_address for x in result]

            for data_node in current_rack_nodes:
                if data_node.available_byte_size > chunk_size:
                    add_on.append(data_node)

        return [x.ip_address for x in result + add_on[:self.REPLICATION_FACTOR - 1 - len(result)]]
