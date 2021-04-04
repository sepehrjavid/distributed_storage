from meta_data.models import DataNode, ChunkMetadata
import os
import uuid

from singleton.singleton import Singleton
from storage.exceptions import InvalidFileChunk


class Storage(metaclass=Singleton):
    CHUNK_SIZE = 64 * (10 ** 6)
    REPLICATION_FACTOR = 3

    def __init__(self, storage_path):
        self.storage_path = storage_path

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
            raise InvalidFileChunk

        if self.storage_path in chunk.local_path and os.path.isfile(chunk.local_path):
            os.remove()
        chunk.delete()
