import socket
from threading import Thread
from time import time

from encryption.encryptors import RSAEncryption
from session.sessions import SimpleSession


def client():
    session = SimpleSession("192.168.1.11", 54456)
    data = 16384 * "g"
    session.transfer_data(data)
    session.end_session()


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("192.168.1.11", 54456))
    server.listen(5)
    thread.start()
    start = time()
    client_socket, addr = server.accept()
    session = SimpleSession(input_socket=client_socket, is_server=True)
    print(session.receive_data())
    thread.join()
    end = time()
    print(end - start)

