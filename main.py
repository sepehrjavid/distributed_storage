import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from servers.storage_server import StorageServer
from session.sessions import FileSession


def client():
    # server = StorageServer("192.168.1.12")
    # server.start()
    # session.receive_file("/Users/sepehrjavid/Desktop/p.mkv")
    pass


if __name__ == "__main__":
    server = StorageServer("192.168.1.12")
    server.run()
    # thread = Thread(target=client, args=[])
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind(("192.168.1.11", 54456))
    # server.listen(5)
    # thread.start()
    # session = FileSession("192.168.1.12", 54222)
    # session.end_session()
    # client_socket, addr = server.accept()
    # start = time()
    # session = FileSession(input_socket=client_socket, is_server=True)
    # session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    # session.end_session()
    # thread.join()
    # end = time()
    # print(end - start)
