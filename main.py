import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.storage_client_server import StorageClientServer
from session.sessions import FileSession


def client():
    session = FileSession("192.168.1.11", 54455)
    session = FileSession("192.168.1.11", 54455)
    print("hey")
    # session.receive_file("/Users/sepehrjavid/Desktop/p.mkv")


if __name__ == "__main__":
    # server = StorageServer("192.168.1.12")
    # server.run()
    thread = Thread(target=client, args=[])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("192.168.1.11", 54455))
    server.listen(5)
    thread.start()
    client_socket, addr = server.accept()
    server.close()
    start = time()
    session = FileSession(input_socket=client_socket, is_server=True)
    print("slm")
    # session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    session.close()
    thread.join()
    end = time()
    print(end - start)
