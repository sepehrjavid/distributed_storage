import os
import pickle
import socket
import threading
from pickle import UnpicklingError
from threading import Thread

from meta_data.models import DataNode
from servers.data_node_server import DataNodeServer
from servers.valid_messages import REPLICATE, ACCEPT, OUT_OF_SPACE
from storage.storage import Storage

from encryption.encryptors import RSAEncryption
from session.exceptions import PeerTimeOutException


class SimpleSession:
    MDU = 16000
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

    def convert_to_file_session(self):
        return FileSession(input_socket=self.socket, encryption_class=self.encryption_class)


class FileSession(SimpleSession):
    THREAD_COUNT = 6
    SEQUENCE_LENGTH = 2
    MTU = SimpleSession.MDU - SEQUENCE_LENGTH

    def __init__(self, is_server=False, **kwargs):
        super().__init__(is_server, **kwargs)
        self.to_transfer_chunks = []
        self.received_chunks = []
        self.read_sequence = 0
        self.file_lock = threading.Lock()
        self.to_transfer_chunks_lock = threading.Lock()
        self.received_chunks_lock = threading.Lock()

    def __file_reading_thread(self, file):
        while True:
            with self.file_lock:
                data = file.read(self.MTU)
                temp = self.read_sequence
                self.read_sequence += 1

            if not data:
                break

            result = int(temp).to_bytes(byteorder=self.DATA_LENGTH_BYTE_ORDER,
                                        length=self.DATA_LENGTH_BYTE_NUMBER,
                                        signed=False) + data

            with self.to_transfer_chunks_lock:
                self.to_transfer_chunks.append(result)

        with self.to_transfer_chunks_lock:
            self.to_transfer_chunks.append(None)

    def __file_transfer_thread(self):
        while True:
            with self.to_transfer_chunks_lock:
                if len(self.to_transfer_chunks) == 0:
                    continue
                data = self.to_transfer_chunks.pop(0)

            if data is None:
                self.transfer_data(pickle.dumps(None), encode=False)
                break

            self.transfer_data(data, encode=False)

    def transfer_file(self, source_file_path):
        file_reader_threads = []
        socket_sender_threads = []

        # file_size = os.path.getsize(source_file_path)
        # meta_data = {"size": file_size, "permission": user_permission}
        # self.transfer_data(pickle.dumps(meta_data), encode=False)

        file = open(source_file_path, "rb")
        self.read_sequence = 0
        self.to_transfer_chunks = []

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

    def __replicate_thread(self, active_threads, node: DataNode, **chunk_data):
        session = SimpleSession(ip_address=node.ip_address, port_number=DataNodeServer.DATA_NODE_PORT_NUMBER)
        msg = REPLICATE.format(chunk_size=chunk_data.get("chunk_size"), perm_hash=chunk_data.get("perm_hash"),
                               title=chunk_data.get("title"), sequence=chunk_data.get("sequence"))
        session.transfer_data(msg)
        response = session.receive_data()
        if response == OUT_OF_SPACE:
            pass

        chunks_replicated = []
        termination_message_seen = 0

        while True:
            with self.received_chunks_lock:
                sample = self.received_chunks[-1]

            if sample in chun

    def receive_file(self, storage: Storage, title, chunk_size, permission_hash, chunk_sequence, replicate=False):
        # meta_data = pickle.loads(self.receive_data(decode=False))

        # file_size = meta_data["size"]
        # permission = meta_data["permission"]

        storage.update_byte_size(-chunk_size)
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
