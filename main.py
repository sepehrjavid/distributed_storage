import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.broadcast_server import BroadcastServer
from session.sessions import FileSession, SimpleSession
from storage.storage import Storage


def client():
    receiver = FileSession(server_ip_address="192.168.1.13")
    receiver.receive_file()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    transmitter = FileSession(server_ip_address="192.168.1.13")
    start = time()
    thread.start()
    sleep(0.5)
    transmitter.transfer_file("/Users/sepehrjavid/Desktop/q.txt", "192.168.1.13")
    thread.join()
    print(time() - start)
