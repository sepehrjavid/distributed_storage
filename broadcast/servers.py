from socket import socket, AF_INET, SOCK_DGRAM

from broadcast.transmitters import SimpleTransmitter


class SimpleBroadcastServer:
    def __init__(self, ip_address, port_number):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind((ip_address, port_number))

    def start(self, on_receive):
        while True:
            data, source_address = self.socket.recvfrom(SimpleTransmitter.MAXIMUM_DATA_SIZE)
            on_receive(source_address, data)
