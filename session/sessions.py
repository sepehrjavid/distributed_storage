import os
import pickle
import socket
import threading
from pickle import UnpicklingError
from threading import Thread
from storage.storage import Storage

from encryption.encryptors import RSAEncryption
from session.exceptions import PeerTimeOutException

'''
The receiver in any sort of session is the server!
'''


class SimpleSession:
    MDU = 16386
    DATA_LENGTH_BYTE_NUMBER = 2
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

        self.transfer_lock = threading.Lock()
        self.receive_lock = threading.Lock()

    def transfer_data(self, data, encode=True):
        if encode:
            data = data.encode()

        encrypted_data = self.encryption_class.encrypt(data)

        data_length = int(len(encrypted_data)).to_bytes(byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                                        length=self.DATA_LENGTH_BYTE_NUMBER,
                                                        signed=False)
        with self.transfer_lock:
            self.socket.send(data_length + encrypted_data)

    def receive_data(self, decode=True, time_out=None):
        with self.receive_lock:
            if time_out is not None:
                self.socket.settimeout(time_out)
            try:
                data_length = self.socket.recv(self.DATA_LENGTH_BYTE_NUMBER)
            except socket.timeout:
                self.socket.settimeout(None)
                return
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
    THREAD_COUNT = 6
    SEQUENCE_LENGTH = 2
    MTU = SimpleSession.MDU - SEQUENCE_LENGTH
    BYTES_TO_SEND_BYTE_SIZE = 3

    def __init__(self, file_path=None, **kwargs):
        self.ip_address = kwargs.get("ip_address")
        self.port_number = kwargs.get("port_number")
        self.file_path = file_path

    def __file_transfer_thread(self, file_path, start_file_byte, byte_to_send, session=None):
        sequence = (start_file_byte / self.MTU) + 1

        if session is None:
            session = SimpleSession(ip_address=self.ip_address, port_number=self.port_number)
        session.transfer_data(int(byte_to_send).to_bytes(byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                                         length=self.BYTES_TO_SEND_BYTE_SIZE,
                                                         signed=False))
        bytes_sent = 0
        with open(file_path, "rb") as file:
            file.seek(start_file_byte)

            while bytes_sent < byte_to_send:
                data = file.read(self.MTU)
                result = int(sequence).to_bytes(byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                                length=SimpleSession.DATA_LENGTH_BYTE_NUMBER,
                                                signed=False) + data
                session.transfer_data(result, encode=False)
                bytes_sent += len(data)

    def transfer_file(self, source_file_path, user_permission):
        threads = []
        bytes_assigned = 0

        file_size = os.path.getsize(source_file_path)
        meta_data = {"size": file_size, "permission": user_permission}

        initial_session = SimpleSession(ip_address=self.ip_address, port_number=self.port_number)
        initial_session.transfer_data(pickle.dumps(meta_data), encode=False)

        if file_size / self.MTU == 0:
            threads.append(
                Thread(target=self.__file_transfer_thread, args=[source_file_path, 0, file_size, initial_session]))
            bytes_assigned += file_size
            file_size -= file_size
        else:
            threads.append(
                Thread(target=self.__file_transfer_thread, args=[source_file_path, 0, self.MTU, initial_session]))
            bytes_assigned += self.MTU
            file_size -= self.MTU

        threads[-1].start()

        for i in range(self.THREAD_COUNT - 1):
            if file_size / self.MTU == 0:
                threads.append(
                    Thread(target=self.__file_transfer_thread, args=[source_file_path, bytes_assigned, file_size]))
                bytes_assigned += file_size
                file_size -= file_size
            else:
                threads.append(
                    Thread(target=self.__file_transfer_thread, args=[source_file_path, bytes_assigned, self.MTU]))
                bytes_assigned += self.MTU
                file_size -= self.MTU
            threads[-1].start()

        for thread in threads:
            thread.join()

    def __file_receive_thread(self):
        while True:
            data = self.receive_data(decode=False)

            try:
                eof = pickle.loads(data)
                if eof is None:
                    break
            except UnpicklingError:

                data = (int.from_bytes(data[:self.SEQUENCE_LENGTH],
                                       byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                       signed=False),
                        data[self.SEQUENCE_LENGTH:])

                with self.received_chunks_lock:
                    self.received_chunks.append(data)

    def receive_file(self, storage: Storage, replicate=True):
        meta_data = pickle.loads(self.receive_data(decode=False))

        file_size = meta_data["size"]
        permission = meta_data["permission"]

        storage.update_byte_size(-file_size)
        destination_filename = storage.get_new_file_path()

        replication_nodes = []
        if replicate:
            replication_nodes = storage.get_replication_data_nodes()

        receive_threads = []
        self.received_chunks = []

        for i in range(self.THREAD_COUNT):
            receive_threads.append(Thread(target=self.__file_receive_thread, args=[]))
            receive_threads[-1].start()

        for thread in receive_threads:
            thread.join()

        self.received_chunks.sort(key=lambda x: x[0])
        file = open(destination_filename, 'wb')

        for data in self.received_chunks:
            file.write(data[1])

        file.close()
