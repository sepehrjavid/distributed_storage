from meta_data.database import MetaDatabase
from meta_data.models.chunk import Chunk
from meta_data.models.directory import Directory
from meta_data.models.file import File
from meta_data.models.permission import Permission
from valid_messages import (CREATE_CHUNK, INVALID_METADATA, MESSAGE_SEPARATOR, OUT_OF_SPACE,
                            ACCEPT, REJECT, NEW_CHUNK, NO_PERMISSION, DUPLICATE_CHUNK_FOR_FILE, GET_CHUNK,
                            CHUNK_NOT_FOUND)
from session.sessions import EncryptedSession, FileSession
from singleton.singleton import Singleton
from threading import Thread, Lock
from time import time, sleep
import socket
import parse

from storage.exceptions import NotEnoughSpace


class DataNodeServer(metaclass=Singleton):
    DATA_NODE_PORT_NUMBER = 54223
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 5 * 60
    CONTROLLER_INTERVAL = 10

    def __init__(self, ip_address, storage):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((ip_address, self.DATA_NODE_PORT_NUMBER))
        self.active_clients = []
        self.active_clients_lock = Lock()
        self.controller_thread = None
        self.storage = storage

    def __remove_expired_clients(self):
        with self.active_clients_lock:
            for client in self.active_clients:
                if not client["thread"].is_alive() or time() - client["start_time"] >= self.MAXIMUM_CLIENT_HANDLE_TIME:
                    self.active_clients.remove(client)

    def __active_client_controller_thread(self):
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def run(self):
        print("Data node server started")
        self.controller_thread = Thread(target=self.__active_client_controller_thread, args=[])
        self.controller_thread.start()
        self.server_socket.listen(5)
        while True:
            client_socket, addr = self.server_socket.accept()
            with self.active_clients_lock:
                if len(self.active_clients) >= self.MAXIMUM_CLIENT_ALLOWED:
                    return

            client_data = {
                "ip_address": addr[0],
                "start_time": time(),
                "socket": client_socket
            }

            client_data["thread"] = ClientThread(client_data, self.storage)
            client_data["thread"].start()

            with self.active_clients_lock:
                self.active_clients.append(client_data)


class ClientThread(Thread):
    def __init__(self, client_data, storage, *args, **kwargs):
        super(ClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data
        self.storage = storage
        self.session = None
        self.db = None

    def run(self):
        self.db = MetaDatabase()
        self.session = EncryptedSession(input_socket=self.client_data.get("socket"), is_server=True)
        message = self.session.receive_data()
        command = message.split(MESSAGE_SEPARATOR)[0]

        if command == CREATE_CHUNK.split(MESSAGE_SEPARATOR)[0]:
            self.create_chunk(message)
        elif command == GET_CHUNK.split(MESSAGE_SEPARATOR)[0]:
            self.get_chunk(message)

    def get_chunk(self, message):
        meta_data = dict(parse.parse(GET_CHUNK, message).named)
        username = meta_data.get("username")
        logical_path = meta_data.get("path")
        sequence = meta_data.get("sequence")
        lst = logical_path.split("/")
        dir_path = "/".join(lst[:-1])

        if "." in lst[-1]:
            file_name = lst[-1].split(".")[0]
            extension = lst[-1].split(".")[1]
        else:
            file_name = lst[-1].split(".")[0]
            extension = None

        directory = Directory.find_path_directory(
            main_dir=Directory.fetch_user_main_directory(username=username, db=self.db), path=logical_path)

        file = list(filter(lambda x: x.title == file_name and x.extension == extension, directory.files()))[0]
        if file.get_user_permission(username) not in [Permission.READ_WRITE, Permission.READ_ONLY]:
            self.session.transfer_data(NO_PERMISSION)
            self.session.close()
            return

        requested_chunk = Chunk.fetch_by_file_id_data_node_id_sequence(file_id=file.id,
                                                                       data_node_id=self.storage.current_data_node.id,
                                                                       sequence=sequence, db=self.db)
        if requested_chunk is None:
            self.session.transfer_data(CHUNK_NOT_FOUND)
            self.session.close()
            return

        self.session.transfer_data(ACCEPT)
        self.session.transfer_data(requested_chunk.chunk_size)
        file_session = FileSession()
        file_session.transfer_file(requested_chunk.local_path, session=self.session)
        self.session.close()

    def create_chunk(self, message):
        try:
            meta_data = dict(parse.parse(CREATE_CHUNK, message).named)
        except ValueError:
            self.session.transfer_data(INVALID_METADATA)
            self.session.close()
            return

        try:
            self.storage.update_byte_size(-int(meta_data.get("chunk_size")), self.db)
        except NotEnoughSpace:
            self.session.transfer_data(OUT_OF_SPACE)
            self.session.close()
            return

        username = meta_data.get("username")

        requested_dir = Directory.find_path_directory(
            main_dir=Directory.fetch_user_main_directory(username=username, db=self.db),
            path=meta_data.get("path"))

        if requested_dir.get_user_permission(username) not in [Permission.WRITE_ONLY, Permission.READ_WRITE]:
            self.session.transfer_data(NO_PERMISSION)
            self.session.close()
            return

        file = File.fetch_by_dir_title_extension(dir_id=requested_dir.id, title=meta_data.get("title"),
                                                 extension=meta_data.get("extension"), db=self.db)
        if file is None:
            self.session.transfer_data(REJECT)
            self.session.close()

        if Chunk.fetch_by_file_id_data_node_id_sequence(file_id=file.id, data_node_id=self.storage.current_data_node.id,
                                                        sequence=meta_data.get("sequence"), db=self.db) is not None:
            self.session.transfer_data(DUPLICATE_CHUNK_FOR_FILE)
            self.session.close()

        self.session.transfer_data(ACCEPT)

        destination_file_path = self.storage.get_new_file_path()

        file_session = FileSession()
        file_session.receive_file(destination_file_path, session=self.session)

        chunk = Chunk(db=self.db, sequence=meta_data.get("sequence"), local_path=destination_file_path,
                      chunk_size=meta_data.get("chunk_size"), data_node_id=self.storage.current_data_node.id,
                      file_id=file.id)
        chunk.save()
        self.session.close()

        self.storage.controller.inform_modification(
            NEW_CHUNK.format(ip_address=self.storage.current_data_node.ip_address,
                             sequence=meta_data.get("sequence"),
                             chunk_size=meta_data.get("chunk_size"),
                             path=meta_data.get("path"),
                             title=meta_data.get("title"),
                             extension=meta_data.get("extension"),
                             username=username,
                             destination_file_path=destination_file_path
                             ))
