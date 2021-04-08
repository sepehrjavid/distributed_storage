import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.broadcast_server import BroadcastServer
from session.sessions import FileSession, SimpleSession
from storage.storage import Storage


def client():
    session = FileSession(ip_address="192.168.1.11", port_number=54455)
    session.receive_file()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    session = FileSession(ip_address="192.168.1.11", port_number=54455)
    start = time()
    thread.start()
    session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv", "sep")
    thread.join()
    print(time() - start)
