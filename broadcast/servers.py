from socket import socket, AF_INET, SOCK_DGRAM

from broadcast.transmitters import SimpleTransmitter


class SimpleBroadcastServer:
    def __init__(self, ip_address, port_number, on_receive):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind((ip_address, port_number))
        self.on_receive = on_receive

    def start(self):
        while True:
            data, source_address = self.socket.recvfrom(SimpleTransmitter.MAXIMUM_DATA_SIZE)
            self.on_receive(source_address, data)
