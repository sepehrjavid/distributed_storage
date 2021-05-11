from socket import socket, AF_INET, SOCK_DGRAM

from broadcast.transmitters import SimpleTransmitter


class SimpleBroadcastServer:
    def __init__(self, broadcast_address, port_number):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind((broadcast_address, port_number))

    def _start(self):
        while True:
            data, source_address = self.socket.recvfrom(SimpleTransmitter.MAXIMUM_DATA_SIZE)
            self.on_receive(source_address, data)

    def on_receive(self, source_address, data) -> None:
        pass
