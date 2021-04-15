from threading import Thread
from time import time

from meta_data.models import DataNode, ChunkMetadata
from session.sessions import FileSession
from storage.storage import Storage


def client():
    session = FileSession(source_ip_address="192.168.1.13")
    session.receive_file("/Users/sepehrjavid/Desktop/p.mp4")


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    session = FileSession(destination_ip_address="192.168.1.13")
    start = time()
    thread.start()
    session.transfer_file("/Users/sepehrjavid/Desktop/q.mp4")
    thread.join()
    print(time() - start)
