import os
import pickle
import socket
import threading
from threading import Thread

from encryption.encryptors import RSAEncryption


class SimpleSession:
    MDU = 16384
    MTU = 21924

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

    def transfer_data(self, data, encode=True):
        if encode:
            data = data.encode()

        encrypted_data = self.encryption_class.encrypt(data)
        encrypted_length = self.encryption_class.encrypt(str(len(encrypted_data)).zfill(5).encode())
        print(len(encrypted_length))

        self.socket.send(encrypted_length)
        self.socket.send(encrypted_data)

    def receive_data(self, decode=True):
        encrypted_length = self.socket.recv(100)
        data_length = int(self.encryption_class.decrypt(encrypted_length))

        if decode:
            return self.encryption_class.decrypt(self.socket.recv(data_length)).decode()
        return self.encryption_class.decrypt(self.socket.recv(data_length))

    def end_session(self):
        self.socket.close()


class FileSession(SimpleSession):
    THREAD_COUNT = 5
    SEQUENCE_LENGTH = 4

    def __init__(self, ip_address=None, port_number=None, input_socket=None, is_server=False):
        super().__init__(ip_address, port_number, input_socket, is_server)
        self.transfer_chunks = []
        self.receive_chunks = []
        self.read_sequence = 0
        self.file_lock = threading.Lock()
        self.transfer_chunks_lock = threading.Lock()
        self.transfer_lock = threading.Lock()

    def __file_reading_thread(self, file):
        while True:
            with self.file_lock:
                data = file.read(self.MDU - self.SEQUENCE_LENGTH)
                temp = self.read_sequence
                self.read_sequence += 1

            if not data:
                break

            result = str(temp).zfill(self.SEQUENCE_LENGTH).encode() + data

            with self.transfer_chunks_lock:
                self.transfer_chunks.append(result)

        with self.transfer_chunks_lock:
            self.transfer_chunks.append(None)

    def __file_transfer_thread(self):
        while True:
            with self.transfer_chunks_lock:
                if len(self.transfer_chunks) == 0:
                    continue
                data = self.transfer_chunks.pop(0)

            if data is None:
                with self.transfer_lock:
                    self.transfer_data(pickle.dumps(None), encode=False)
                break

            with self.transfer_lock:
                self.transfer_data(data, encode=False)

    def transfer_file(self, source_file_path):
        file_reader_threads = []
        socket_sender_threads = []
        file_size = os.path.getsize(source_file_path)
        file = open(source_file_path, "rb")
        self.read_sequence = 0

        for i in range(self.THREAD_COUNT):
            file_reader_threads.append(Thread(target=self.__file_reading_thread, args=[file]))
            file_reader_threads[-1].start()

        for i in range(self.THREAD_COUNT):
            socket_sender_threads.append(Thread(target=self.__file_transfer_thread, args=[]))
            socket_sender_threads[-1].start()

        for thread in file_reader_threads:
            thread.join()

        file.close()

        for thread in socket_sender_threads:
            thread.join()

    def receive_file(self):
        for i in range(9):
            data = self.receive_data(decode=False)
            print(data)
