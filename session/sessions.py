import socket

from encryption.encryptors import RSAEncryption
from session.exceptions import InvalidSessionType, PeerTimeOutException


class SimpleSession:
    MTU = 4096
    DATA_LENGTH_BYTE_NUMBER = 2
    MDU = MTU - DATA_LENGTH_BYTE_NUMBER
    DATA_LENGTH_BYTE_ORDER = "big"

    def __init__(self, is_server=False, **kwargs):
        if kwargs.get("input_socket"):
            self.socket = kwargs.get("input_socket")
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.socket.connect((kwargs.get("ip_address"), kwargs.get("port_number")))
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
        self.socket.send(data_length + encrypted_data)

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


class FileSession:
    FILE_TRANSMISSION_PORT = 54224

    def __init__(self, **kwargs):
        self.source_ip_address = kwargs.get("source_ip_address")
        self.destination_ip_address = kwargs.get("destination_ip_address")

    def transfer_file(self, source_file_path, session=None):
        if session is None:
            session = SimpleSession(ip_address=self.destination_ip_address, port_number=self.FILE_TRANSMISSION_PORT)
        elif session.is_server:
            raise InvalidSessionType("is_server must be False")

        with open(source_file_path, "rb") as file:
            while True:
                data = file.read(SimpleSession.MDU)

                if len(data) == 0:
                    break

                session.transfer_data(data, encode=False)

        session.close()

    def receive_file(self, dest_path, session=None, replication_list=None):
        if session is None:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.source_ip_address, self.FILE_TRANSMISSION_PORT))
            server_socket.listen(5)
            client_socket, addr = server_socket.accept()
            session = SimpleSession(input_socket=client_socket, is_server=True)
            server_socket.close()
        elif not session.is_server:
            raise InvalidSessionType("Is server must be True")

        with open(dest_path, "wb") as file:
            data = session.receive_data(decode=False)
            while data is not None:
                file.write(data)
                data = session.receive_data(decode=False)

        session.close()
