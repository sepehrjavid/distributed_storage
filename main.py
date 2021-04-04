import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.broadcast_server import BroadcastServer
from session.sessions import FileSession


def client():
    session = FileSession("192.168.1.11", 54455)
    session = FileSession("192.168.1.11", 54455)
    print("hey")
    # session.receive_file("/Users/sepehrjavid/Desktop/p.mkv")


if __name__ == "__main__":
    print(ChunkMetadata.fetch_by_title_and_permission("slm", "sajdn").data_node)
    # thread = Thread(target=client, args=[])
    # thread.start()
    # thread.join()
