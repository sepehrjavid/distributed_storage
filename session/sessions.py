import os
import socket

from encryption.encryptors import RSAEncryption
from session.exceptions import PeerTimeOutException


class EncryptedSession:
    MTU = 4096
    DATA_LENGTH_BYTE_NUMBER = 2
    MDU = MTU - DATA_LENGTH_BYTE_NUMBER
    DATA_LENGTH_BYTE_ORDER = "big"

    def __init__(self, is_server=False, **kwargs):
        self.is_server = is_server
        self.ip_address = kwargs.get("ip_address")
        self.port_number = kwargs.get("port_number")

        if kwargs.get("input_socket"):
            self.socket = kwargs.get("input_socket")
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.socket.connect((self.ip_address, self.port_number))
            except ConnectionRefusedError:
                raise PeerTimeOutException

        if kwargs.get("encryption_class"):
            self.encryption_class = kwargs.get("encryption_class")
        else:
            if is_server:
                self.encryption_class = RSAEncryption.create_server_encryption(self.socket)
            else:
                self.encryption_class = RSAEncryption.create_client_encryption(self.socket)

    def transfer_data(self, data, encode=True):
        if encode:
            data = data.encode()

        encrypted_data = self.encryption_class.encrypt(data)

        data_length = int(len(encrypted_data)).to_bytes(byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                                        length=self.DATA_LENGTH_BYTE_NUMBER,
                                                        signed=False)
        self.socket.sendall(data_length + encrypted_data)

    def receive_data(self, decode=True):
        data_length = self.socket.recv(self.DATA_LENGTH_BYTE_NUMBER)

        if len(data_length) == 0:
            return None

        data_length = int.from_bytes(data_length,
                                     byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                     signed=False)
        bytes_read = 0
        encrypted_data = b''
        while bytes_read < data_length:
            temp_encrypted_data = self.socket.recv(data_length - bytes_read)
            encrypted_data += temp_encrypted_data
            bytes_read += len(temp_encrypted_data)

        received_data = self.encryption_class.decrypt(encrypted_data)

        if decode:
            return received_data.decode()
        return received_data

    def close(self):
        self.socket.close()


class SimpleSession:
    MTU = 4096
    DATA_LENGTH_BYTE_NUMBER = 2
    MDU = MTU - DATA_LENGTH_BYTE_NUMBER
    DATA_LENGTH_BYTE_ORDER = "big"

    def __init__(self, **kwargs):
        self.ip_address = kwargs.get("ip_address")
        self.port_number = kwargs.get("port_number")

        if kwargs.get("input_socket"):
            self.socket = kwargs.get("input_socket")
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.socket.connect((self.ip_address, self.port_number))
            except ConnectionRefusedError:
                raise PeerTimeOutException

    def transfer_data(self, data, encode=True):
        if encode:
            data = data.encode()

        data_length = int(len(data)).to_bytes(byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                              length=self.DATA_LENGTH_BYTE_NUMBER,
                                              signed=False)
        self.socket.sendall(data_length + data)

    def receive_data(self, decode=True):
        data_length = self.socket.recv(self.DATA_LENGTH_BYTE_NUMBER)

        if len(data_length) == 0:
            return None

        data_length = int.from_bytes(data_length,
                                     byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                     signed=False)
        bytes_read = 0
        data = b''
        while bytes_read < data_length:
            temp_data = self.socket.recv(data_length - bytes_read)
            data += temp_data
            bytes_read += len(temp_data)

        if decode:
            return data.decode()
        return data

    def close(self):
        self.socket.close()

    def convert_to_encrypted_session(self, is_server=False):
        return EncryptedSession(is_server=is_server, input_socket=self.socket, ip_address=self.ip_address,
                                port_number=self.port_number)


class FileSession:
    def __init__(self, **kwargs):
        self.source_ip_address = kwargs.get("source_ip_address")
        self.destination_ip_address = kwargs.get("destination_ip_address")

    def transfer_file(self, source_file_path, session=None):
        session.transfer_data(str(os.path.getsize(source_file_path)))

        with open(source_file_path, "rb") as file:
            while True:
                data = file.read(EncryptedSession.MDU)

                if len(data) == 0:
                    break

                session.transfer_data(data, encode=False)

    def receive_file(self, dest_path, session, replication_list=None):
        file_size = int(session.receive_data())
        received = 0

        with open(dest_path, "wb") as file:
            data = session.receive_data(decode=False)
            while received < file_size:
                file.write(data)
                received += len(data)
                data = session.receive_data(decode=False)
