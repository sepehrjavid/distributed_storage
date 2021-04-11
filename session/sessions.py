import pickle
import socket
import threading
from pickle import UnpicklingError
from threading import Thread

from storage.storage import Storage

from encryption.encryptors import RSAEncryption

'''
The transmitter in any sort of session is the server!
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
            self.socket.connect((kwargs.get("ip_address"), kwargs.get("port_number")))

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

    def convert_to_file_session(self):
        return FileSession(input_socket=self.socket, encryption_class=self.encryption_class)


class FileSession:
    THREAD_COUNT = 5
    SEQUENCE_LENGTH = 2
    MTU = SimpleSession.MDU - SEQUENCE_LENGTH
    FILE_TRANSFER_PORT = 54224

    def __init__(self, server_ip_address):
        self.to_transfer_chunks = []
        self.received_chunks = []
        self.received_chunks_lock = threading.Lock()
        self.server_ip_address = server_ip_address

    def __file_reading_thread(self, file_path):
        read_sequence = 0

        with open(file_path, "rb") as file:
            while True:
                data = file.read(self.MTU)

                if not data:
                    break

                result = int(read_sequence).to_bytes(byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                                     length=SimpleSession.DATA_LENGTH_BYTE_NUMBER,
                                                     signed=False) + data

                self.to_transfer_chunks[read_sequence % self.THREAD_COUNT].append(result)
                read_sequence += 1

        for lst in self.to_transfer_chunks:
            lst.append(None)

    def __file_transfer_thread(self, thread_id, ip_address):
        session = SimpleSession(ip_address=ip_address, port_number=self.FILE_TRANSFER_PORT, is_server=True)

        while True:
            if len(self.to_transfer_chunks[thread_id]) == 0:
                continue
            data = self.to_transfer_chunks[thread_id].pop(0)

            if data is None:
                session.transfer_data(pickle.dumps(None), encode=False)
                break

            session.transfer_data(data, encode=False)

        session.close()

    def transfer_file(self, source_file_path, ip_address):
        sender_threads = []
        self.to_transfer_chunks = [[] for _ in range(self.THREAD_COUNT)]

        file_reader_thread = Thread(target=self.__file_reading_thread, args=[source_file_path])
        file_reader_thread.start()

        for i in range(self.THREAD_COUNT):
            sender_threads.append(Thread(target=self.__file_transfer_thread, args=[i, ip_address]))
            sender_threads[-1].start()

        file_reader_thread.join()

        for thread in sender_threads:
            thread.join()

    def __file_receive_thread(self, client_socket):
        session = SimpleSession(input_socket=client_socket)

        while True:
            data = session.receive_data(decode=False)

            try:
                eof = pickle.loads(data)
                if eof is None:
                    break
            except UnpicklingError:

                data = (int.from_bytes(data[:self.SEQUENCE_LENGTH],
                                       byteorder=SimpleSession.DATA_LENGTH_BYTE_ORDER,
                                       signed=False),
                        data[self.SEQUENCE_LENGTH:])

                with self.received_chunks_lock:
                    self.received_chunks.append(data)

        session.close()

    def receive_file(self):
        # storage.update_byte_size(-chunk_size)
        receive_threads = []
        self.received_chunks = []

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.server_ip_address, self.FILE_TRANSFER_PORT))
        server_socket.listen(5)

        for _ in range(self.THREAD_COUNT):
            client_socket, addr = server_socket.accept()
            receive_threads.append(Thread(target=self.__file_receive_thread, args=[client_socket]))
            receive_threads[-1].start()

        for thread in receive_threads:
            thread.join()

        destination_filename = "/Users/sepehrjavid/Desktop/qpashm.mkv"
        self.received_chunks.sort(key=lambda x: x[0])
        file = open(destination_filename, 'wb')

        for data in self.received_chunks:
            file.write(data[1])

        file.close()
