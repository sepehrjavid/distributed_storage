from meta_data.models import DataNode, ChunkMetadata
import os
import uuid

from singleton.singleton import Singleton
from storage.exceptions import InvalidFilePath, ChunkNotFound, DataNodeNotSaved


class Storage(metaclass=Singleton):
    CHUNK_SIZE = 64 * (10 ** 6)
    REPLICATION_FACTOR = 4

    def __init__(self, storage_path, current_data_node):
        self.storage_path = storage_path
        if isinstance(current_data_node, DataNode) and DataNode.fetch_by_id(current_data_node.id) is not None:
            self.current_data_node = current_data_node
        else:
            raise DataNodeNotSaved

    def is_valid_path(self, path):
        return self.storage_path in path and os.path.isfile(path)

    def choose_data_node_to_save(self, file_size):
        required_node_numbers = file_size // self.CHUNK_SIZE
        if file_size % self.CHUNK_SIZE != 0:
            required_node_numbers += 1

        all_nodes = DataNode.fetch_all()

        if len(all_nodes) >= required_node_numbers:
            return all_nodes[:required_node_numbers]

        ratio = required_node_numbers // len(all_nodes)
        if required_node_numbers % len(all_nodes) != 0:
            ratio += 1

        all_nodes = ratio * all_nodes
        return all_nodes[:required_node_numbers]

    def get_new_file_path(self):
        filepath = self.storage_path + str(uuid.uuid4())
        while os.path.isfile(filepath):
            filepath = self.storage_path + str(uuid.uuid4())

        return filepath

    def remove_chunk(self, title, permission):
        chunk = ChunkMetadata.fetch_by_title_and_permission(title, permission)
        if chunk is None:
            raise ChunkNotFound

        if self.is_valid_path(chunk.local_path):
            os.remove(chunk.local_path)
        chunk.delete()

    def add_chunk(self, **kwargs):
        if kwargs.get("local_path") is not None and self.is_valid_path(kwargs.get("local_path")):
            chunk = ChunkMetadata(**kwargs, data_node=self.current_data_node)
            chunk.save()
            self.current_data_node.available_byte_size -= chunk.chunk_size
            self.current_data_node.save()
        else:
            raise InvalidFilePath

    def retract_saved_chunk(self, path):
        if self.is_valid_path(path):
            os.remove(path)

    def get_replication_data_nodes(self):
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
