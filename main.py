import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from session.sessions import FileSession


def client():
    session = FileSession("192.168.1.11", 54456)
    session.receive_file("/Users/sepehrjavid/Desktop/p.txt")
    session.end_session()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("192.168.1.11", 54456))
    server.listen(5)
    thread.start()
    client_socket, addr = server.accept()
    start = time()
    session = FileSession(input_socket=client_socket, is_server=True)
    session.transfer_file("/Users/sepehrjavid/Desktop/q.txt")
    session.end_session()
    thread.join()
    end = time()
    print(end - start)
