import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.broadcast_server import BroadcastServer
from session.sessions import FileSession
from storage.storage import Storage


def client():
    session = FileSession("192.168.1.11", 54455)
    session = FileSession("192.168.1.11", 54455)
    print("hey")
    # session.receive_file("/Users/sepehrjavid/Desktop/p.mkv")


if __name__ == "__main__":
    p = DataNode.fetch_by_id(8)
    s = Storage("/Users/sepehrjavid/Desktop/", p)
    print(s.get_new_file_path())
    print(s.get_replication_data_nodes())
    print(s.add_chunk(sequence=1, title="l", local_path="/Users/sepehrjavid/Desktop/q.txt", chunk_size=90, permission="skm"))
    # print(ChunkMetadata.fetch_by_title_and_permission("slm", "sajdn").data_node)
    # thread = Thread(target=client, args=[])
    # thread.start()
    # thread.join()
