from threading import Thread
from time import time

from meta_data.models import DataNode, ChunkMetadata
from servers.broadcast_server import BroadcastServer
from session.sessions import FileSession
from storage.storage import Storage


def client():
    session = FileSession(source_ip_address="192.168.1.13")
    session.receive_file("/Users/sepehrjavid/Desktop/p.mp4")


if __name__ == "__main__":
    storage = Storage("/Users/sepehrjavid/Desktop", DataNode.fetch_by_id(1))
    server = BroadcastServer(storage=storage, ip_address="192.168.1.255")
    server.run()
