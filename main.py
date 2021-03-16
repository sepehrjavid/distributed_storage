import socket
from threading import Thread
from time import time

from session.sessions import FileSession


def client():
    session = FileSession("192.168.1.11", 54455)
    session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    session.end_session()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("192.168.1.11", 54455))
    server.listen(5)
    thread.start()
    start = time()
    client_socket, addr = server.accept()
    session = FileSession(input_socket=client_socket, is_server=True)
    # print(len(session.encryption_class.encrypt("11111".encode())))
    session.receive_file("/Users/sepehrjavid/Desktop/sep.mkv")
    thread.join()
    end = time()
    print(end - start)
