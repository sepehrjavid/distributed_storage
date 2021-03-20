import socket
from threading import Thread
from time import time, sleep

from broadcast.servers import SimpleBroadcastServer
from broadcast.transmitters import SimpleTransmitter
from session.sessions import FileSession


def client():
    # session = FileSession("192.168.1.11", 54455)
    # session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    # session.end_session()
    server = SimpleBroadcastServer("192.168.1.255", 54455)
    server.start(lambda x, y: print(y))


if __name__ == "__main__":
    thread = Thread(target=client, args=[])
    thread.daemon = True
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind(("192.168.1.11", 54455))
    # server.listen(5)
    broad_caster = SimpleTransmitter("192.168.1.255", 54455)
    thread.start()
    broad_caster.transmit("slm o dorud")
    sleep(1)
    # start = time()
    # thread.join()
    # client_socket, addr = server.accept()
    # session = FileSession(input_socket=client_socket, is_server=True)
    # session.receive_file("/Users/sepehrjavid/Desktop/sep.mkv")
    # end = time()
    # print(end - start)
