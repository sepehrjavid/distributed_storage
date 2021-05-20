from multiprocessing import Process

from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from meta_data.models.chunk import Chunk
import os
import uuid

from singleton.singleton import Singleton
from storage.exceptions import InvalidFilePath, ChunkNotFound, DataNodeNotSaved


class Storage(metaclass=Singleton):
    CHUNK_SIZE = 64 * (10 ** 6)
    REPLICATION_FACTOR = 3

    def __init__(self, storage_path, current_data_node, controller, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_path = storage_path
        self.db = MetaDatabase()
        self.controller = controller
        if isinstance(current_data_node, DataNode) and DataNode.fetch_by_id(current_data_node.id, self.db) is not None:
            self.current_data_node = current_data_node
        else:
            raise DataNodeNotSaved

    def is_valid_path(self, path):
        return self.storage_path in path and os.path.isfile(path)

    def choose_data_node_to_save(self, file_size, db):
        all_nodes = DataNode.fetch_all(db=db)

        if sum([x.available_byte_size for x in all_nodes]) < file_size:
            return None

        assigned_nodes = []
        i = 0
        bytes_assigned = 0
        while bytes_assigned < file_size:
            data_node = all_nodes[i % len(all_nodes)]
            if (file_size - bytes_assigned) >= self.CHUNK_SIZE and data_node.available_byte_size >= self.CHUNK_SIZE:
                assigned_nodes.append((self.CHUNK_SIZE, data_node))
                bytes_assigned += self.CHUNK_SIZE
                data_node.available_byte_size -= self.CHUNK_SIZE
            elif (file_size - bytes_assigned) < self.CHUNK_SIZE and data_node.available_byte_size >= (
                    file_size - bytes_assigned):
                assigned_nodes.append((file_size - bytes_assigned, data_node))
                bytes_assigned += (file_size - bytes_assigned)
                data_node.available_byte_size -= (file_size - bytes_assigned)
            else:
                assigned_nodes.append((data_node.available_byte_size, data_node))
                bytes_assigned += data_node.available_byte_size
                data_node.available_byte_size = 0
            i += 1

        return assigned_nodes

    def get_new_file_path(self, extension=None):
        if extension is None:
            filepath = self.storage_path + str(uuid.uuid4())
        else:
            filepath = self.storage_path + str(uuid.uuid4()) + "." + extension
        while os.path.isfile(filepath):
            if extension is None:
                filepath = self.storage_path + str(uuid.uuid4())
            else:
                filepath = self.storage_path + str(uuid.uuid4()) + "." + extension

        return filepath

    def remove_chunk(self, title, permission):
        chunk = Chunk.fetch_by_title_and_permission(title, permission)
        if chunk is None:
            raise ChunkNotFound

        if self.is_valid_path(chunk.local_path):
            os.remove(chunk.local_path)
        chunk.delete()

    def update_byte_size(self, byte_size):
        self.current_data_node.available_byte_size += byte_size
        self.current_data_node.save()
        # TODO inform the rest of the nodes

    def add_chunk(self, **kwargs):
        if kwargs.get("local_path") is not None and self.is_valid_path(kwargs.get("local_path")):
            chunk = Chunk(**kwargs, data_node=self.current_data_node)
            chunk.save()
            # TODO inform the rest of the nodes
        else:
            raise InvalidFilePath

    def retract_saved_chunk(self, path):
        if self.is_valid_path(path):
            os.remove(path)

    def get_replication_data_nodes(self, chunk_size):
        return None
        all_data_nodes = DataNode.fetch_all()
        other_data_nodes = list(filter(lambda x: x.id != self.current_data_node.id, all_data_nodes))
        racks = {}

        for data_node in other_data_nodes:
            if data_node.rack_number not in racks:
                racks[data_node.rack_number] = []
            racks[data_node.rack_number].append(data_node)

        if len(racks) == 1:
            return racks[self.current_data_node.rack_number][:self.REPLICATION_FACTOR - 1]

        result = []
        i = 0
        while len(result) < self.REPLICATION_FACTOR - 1:
            for rack_number in racks:
                if len(result) == self.REPLICATION_FACTOR - 1:
                    return result

                if self.current_data_node.rack_number != rack_number:
                    result.append(racks[rack_number][i])

            i += 1

        return result
