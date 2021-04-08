import os
import pickle
import socket
from threading import Thread, Lock
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

        self.transfer_lock = Lock()
        self.receive_lock = Lock()

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
    THREAD_COUNT = 1
    SEQUENCE_LENGTH = 2
    MTU = SimpleSession.MDU - SEQUENCE_LENGTH
    BYTES_TO_SEND_BYTE_SIZE = 3

    def __init__(self, ip_address, port_number):
        self.ip_address = ip_address
        self.port_number = port_number
        self.received_chunks = []
        self.received_chunks_lock = Lock()

    def __file_transfer_thread(self, file_path, start_file_byte, byte_to_send, session=None):
        sequence = (start_file_byte / self.MTU) + 1

        if session is None:
            session = SimpleSession(ip_address=self.ip_address, port_number=self.port_number)
        session.transfer_data(int(byte_to_send).to_bytes(byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                                         length=self.BYTES_TO_SEND_BYTE_SIZE,
                                                         signed=False), encode=False)
        with open(file_path, "rb") as file:
            file.seek(start_file_byte)

            while byte_to_send > 0:
                if byte_to_send < self.MTU:
                    data = file.read(byte_to_send)
                else:
                    data = file.read(self.MTU)

                result = int(sequence).to_bytes(byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                                length=SimpleSession.DATA_LENGTH_BYTE_NUMBER,
                                                signed=False) + data
                session.transfer_data(result, encode=False)
                byte_to_send -= len(data)

        session.close()

    def transfer_file(self, source_file_path, user_permission):
        threads = []
        assigned_bytes = 0

        file_size = os.path.getsize(source_file_path)
        meta_data = {"size": file_size, "permission": user_permission}

        threads_bytes_to_send = self.THREAD_COUNT * [file_size // self.THREAD_COUNT]
        if file_size % self.THREAD_COUNT != 0:
            threads_bytes_to_send[-1] += file_size % self.THREAD_COUNT

        initial_session = SimpleSession(ip_address=self.ip_address, port_number=self.port_number)
        initial_session.transfer_data(pickle.dumps(meta_data), encode=False)

        threads.append(
            Thread(target=self.__file_transfer_thread,
                   args=[source_file_path, 0, threads_bytes_to_send[0], initial_session]))
        assigned_bytes += threads_bytes_to_send[0]

        threads[-1].start()

        for i in range(1, self.THREAD_COUNT):
            threads.append(
                Thread(target=self.__file_transfer_thread,
                       args=[source_file_path, assigned_bytes, threads_bytes_to_send[i]]))
            threads[-1].start()
            assigned_bytes += threads_bytes_to_send[i]

        for thread in threads:
            thread.join()

    def __file_receive_thread(self, client_socket, session=None):
        if session is None:
            session = SimpleSession(is_server=True, input_socket=client_socket)
        byte_to_receive = int.from_bytes(session.receive_data(decode=False),
                                         byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                         signed=False)

        bytes_received = 0
        while bytes_received < byte_to_receive:
            data = session.receive_data(decode=False)

            bytes_received += len(data[self.SEQUENCE_LENGTH:])

            data = (int.from_bytes(data[:self.SEQUENCE_LENGTH],
                                   byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                   signed=False),
                    data[self.SEQUENCE_LENGTH:])

            with self.received_chunks_lock:
                self.received_chunks.append(data)

    def receive_file(self, storage: Storage = None):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip_address, self.port_number))
        server_socket.listen(5)
        client_socket, addr = server_socket.accept()

        initial_session = SimpleSession(input_socket=client_socket, is_server=True)
        meta_data = pickle.loads(initial_session.receive_data(decode=False))

        # destination_filename = storage.get_new_file_path()
        destination_filename = "/Users/sepehrjavid/Desktop/f.mkv"

        receive_threads = []
        self.received_chunks = []

        receive_threads.append(
            Thread(target=self.__file_receive_thread, args=[initial_session.socket, initial_session]))
        receive_threads[-1].start()

        for i in range(self.THREAD_COUNT - 1):
            client_socket, addr = server_socket.accept()
            receive_threads.append(Thread(target=self.__file_receive_thread, args=[client_socket]))
            receive_threads[-1].start()

        server_socket.close()

        for thread in receive_threads:
            thread.join()

        self.received_chunks.sort(key=lambda x: x[0])

        with open(destination_filename, 'wb') as file:
            for data in self.received_chunks:
                file.write(data[1])
