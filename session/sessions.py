import socket

from encryption.encryptors import RSAEncryption


class SimpleSession:
    MDU = 16384
    MTU = 21944

    def __init__(self, ip_address=None, port_number=None, input_socket=None, is_server=False):
        if input_socket:
            self.socket = input_socket
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((ip_address, port_number))

        if is_server:
            self.encryption_class = RSAEncryption.create_server_encryption(self.socket)
        else:
            self.encryption_class = RSAEncryption.create_client_encryption(self.socket)

    def transfer_data(self, data):
        if type(data) == bytes:
            encoded_data = data
        elif type(data) == str:
            encoded_data = data.encode()
        else:
            raise Exception("unsupported data type")

        self.socket.send(self.encryption_class.encrypt(encoded_data))

    def receive_data(self, decode=True):
        if decode:
            return self.encryption_class.decrypt(self.socket.recv(self.MTU)).decode()
        return self.encryption_class.decrypt(self.socket.recv(self.MTU))

    def end_session(self):
        self.socket.close()
