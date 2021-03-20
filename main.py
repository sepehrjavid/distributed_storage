import socket
from threading import Thread
from time import time, sleep

from session.sessions import FileSession


def client():
    pass
    session = FileSession("192.168.1.14", 54455)
    session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    # print(int.from_bytes(sep, byteorder=sys.byteorder, signed=False))
    session.end_session()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("192.168.1.14", 54455))
    server.listen(5)
    thread.start()
    client_socket, addr = server.accept()
    start = time()
    session = FileSession(input_socket=client_socket, is_server=True)
    session.receive_file("/Users/sepehrjavid/Desktop/sep.mkv")
    thread.join()
    end = time()
    print(end - start)
