from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, SO_BROADCAST, socket


class SimpleTransmitter:
    MAXIMUM_DATA_SIZE = 4096

    def __init__(self, ip_address, port_number):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        self.ip_address = ip_address
        self.port_number = port_number

    def transmit(self, data, encode=True):
        if encode:
            encoded = data.encode()
        else:
            encoded = data

        self.socket.sendto(encoded, (self.ip_address, self.port_number))
