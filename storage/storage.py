from meta_data.models import DataNode


class Storage:
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
        pass
