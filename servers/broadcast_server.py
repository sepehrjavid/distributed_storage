import pickle
from threading import Thread, Lock
from time import sleep
from time import time

import parse

from broadcast.servers import SimpleBroadcastServer
from meta_data.database import MetaDatabase
from meta_data.models.directory import Directory
from meta_data.models.file import File
from meta_data.models.permission import Permission
from meta_data.models.user import User
from valid_messages import (CREATE_FILE, MESSAGE_SEPARATOR, OUT_OF_SPACE, ACCEPT, DUPLICATE_FILE_FOR_USER,
                            NEW_FILE, NO_PERMISSION, INVALID_PATH, LOGIN, CREDENTIALS, USER_NOT_FOUND, AUTH_FAILED,
                            CREATE_ACCOUNT, DUPLICATE_ACCOUNT, NEW_USER, GET_FILE, FILE_DOES_NOT_EXIST, CORRUPTED_FILE)
from session.exceptions import PeerTimeOutException
from session.sessions import EncryptedSession
from singleton.singleton import Singleton


class BroadcastServer(SimpleBroadcastServer, metaclass=Singleton):
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 4 * 60
    CONTROLLER_INTERVAL = 5
    BROADCAST_SERVER_PORT_NUMBER = 54222
    CLIENT_PORT_NUMBER = BROADCAST_SERVER_PORT_NUMBER

    def __init__(self, broadcast_address, storage):
        super().__init__(broadcast_address, self.BROADCAST_SERVER_PORT_NUMBER)
        self.active_clients = []
        self.active_clients_lock = Lock()
        self.controller_thread = None
        self.storage = storage

    def on_receive(self, source_address, data):
        with self.active_clients_lock:
            if len(self.active_clients) >= self.MAXIMUM_CLIENT_ALLOWED:
                return
            for client in self.active_clients:
                if client["ip_address"] == source_address:
                    return

        client_data = {
            "ip_address": source_address[0],
            "start_time": time(),
            "command": data.decode()
        }

        client_data["thread"] = ClientThread(client_data, self.storage)
        client_data["thread"].start()

        with self.active_clients_lock:
            self.active_clients.append(client_data)

    def __remove_expired_clients(self):
        with self.active_clients_lock:
            for client in self.active_clients:
                if not client["thread"].is_alive() or time() - client["start_time"] >= self.MAXIMUM_CLIENT_HANDLE_TIME:
                    self.active_clients.remove(client)

    def __active_client_controller_thread(self):
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def start(self):
        print("broadcast server started")
        self.controller_thread = Thread(target=self.__active_client_controller_thread, args=[])
        self.controller_thread.start()
        self._start()


class ClientThread(Thread):
    def __init__(self, client_data, storage, *args, **kwargs):
        super(ClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data
        self.storage = storage
        self.db_connection = None
        self.session = None

    def run(self):
        self.db_connection = MetaDatabase()
        try:
            self.session = EncryptedSession(ip_address=self.client_data.get("ip_address"),
                                            port_number=BroadcastServer.CLIENT_PORT_NUMBER)
        except PeerTimeOutException:
            return

        message = self.client_data.get("command")
        command = message.split(MESSAGE_SEPARATOR)[0]

        if command == CREATE_FILE.split(MESSAGE_SEPARATOR)[0]:
            self.create_file(message)
        elif message == LOGIN:
            self.login()
        elif message == CREATE_ACCOUNT:
            self.create_account()
        elif command == GET_FILE.split(MESSAGE_SEPARATOR)[0]:
            self.get_file(message)

    def get_file(self, message):
        meta_data = dict(parse.parse(GET_FILE, message).named)
        username = meta_data.get("username")
        logical_path = meta_data.get("path")
        lst = logical_path.split("/")
        dir_path = "/".join(lst[:-1])

        if "." in lst[-1]:
            file_name = lst[-1].split(".")[0]
            extension = lst[-1].split(".")[1]
        else:
            file_name = lst[-1].split(".")[0]
            extension = None

        directory = Directory.find_path_directory(
            main_dir=Directory.fetch_user_main_directory(username=username, db=self.db_connection), path=logical_path)

        if directory is None:
            self.session.transfer_data(INVALID_PATH)
            self.session.close()
            return

        files = directory.files
        possible_file = list(filter(lambda x: x.title == file_name and x.extension == extension, files))
        if len(possible_file) == 0:
            self.session.transfer_data(FILE_DOES_NOT_EXIST)
            self.session.close()
            return

        file = possible_file[0]

        if file.get_user_permission(username) not in [Permission.READ_WRITE, Permission.READ_ONLY]:
            self.session.transfer_data(NO_PERMISSION)
            self.session.close()
            return

        chunks = file.chunks
        result = []
        for chunk in chunks:
            if chunks.sequence not in [x[0] for x in result]:
                result.append((chunks.sequence, chunks.data_node.ip_address))

        if len(result) != file.sequence_num:
            self.session.transfer_data(CORRUPTED_FILE)
            self.session.close()
            return

        self.session.transfer_data(pickle.dumps(result), encode=False)
        self.session.close()

    def create_account(self):
        credentials = self.session.receive_data()
        meta_data = dict(parse.parse(CREDENTIALS, credentials).named)
        username = meta_data.get("username")
        password = meta_data.get("password")

        user = User.fetch_by_username(username=username, db=self.db_connection)

        if user is None:
            user = User(db=self.db_connection, username=username, password=password)
            user.save()
            main_directory = Directory(db=self.db_connection, title=Directory.MAIN_DIR_NAME, parent_directory_id=None)
            main_directory.save()
            permission = Permission(db=self.db_connection, directory_id=main_directory.id, user_id=user.id,
                                    perm=Permission.READ_WRITE)
            permission.save()
            self.storage.controller.inform_modification(NEW_USER.format(username=username, password=password))
            self.session.transfer_data(ACCEPT)
        else:
            self.session.transfer_data(DUPLICATE_ACCOUNT)

        self.session.close()

    def login(self):
        credentials = self.session.receive_data()
        meta_data = dict(parse.parse(CREDENTIALS, credentials).named)
        username = meta_data.get("username")
        password = meta_data.get("password")

        user = User.fetch_by_username(username=username, db=self.db_connection)
        if user is None:
            self.session.transfer_data(USER_NOT_FOUND)
            self.session.close()
            return

        if user.password == password:
            self.session.transfer_data(ACCEPT)
        else:
            self.session.transfer_data(AUTH_FAILED)
        self.session.close()

    def create_file(self, command):
        meta_data = dict(parse.parse(CREATE_FILE, command).named)
        username = meta_data.get("username")

        requested_dir = Directory.find_path_directory(
            main_dir=Directory.fetch_user_main_directory(username=username, db=self.db_connection),
            path=meta_data.get("path"))

        if requested_dir is None:
            self.session.transfer_data(INVALID_PATH)
            self.session.close()
            return

        if requested_dir.get_user_permission(username) not in [Permission.WRITE_ONLY, Permission.READ_WRITE]:
            self.session.transfer_data(NO_PERMISSION)
            self.session.close()
            return

        if File.fetch_by_dir_title_extension(dir_id=requested_dir.id, title=meta_data.get("title"),
                                             extension=meta_data.get("extension"), db=self.db_connection) is not None:
            self.session.transfer_data(DUPLICATE_FILE_FOR_USER)
            self.session.close()
            return

        data_nodes = self.storage.choose_data_node_to_save(file_size=int(meta_data.get("total_size")),
                                                           db=self.db_connection)

        if data_nodes is None:
            self.session.transfer_data(OUT_OF_SPACE)
        else:
            user = User.fetch_by_username(username=username, db=self.db_connection)
            file = File(db=self.db_connection, title=meta_data.get("title"), is_complete=False,
                        extension=meta_data.get("extension"), directory_id=requested_dir.id,
                        sequence_num=len(data_nodes))
            file.save()
            Permission(db=self.db_connection, perm=Permission.READ_WRITE, file_id=file.id, user_id=user.id).save()
            self.storage.controller.inform_modification(NEW_FILE.format(title=meta_data.get("title"),
                                                                        extension=meta_data.get("extension"),
                                                                        path=meta_data.get("path"),
                                                                        sequence_num=len(data_nodes),
                                                                        username=meta_data.get("username")
                                                                        ))

            self.session.transfer_data(ACCEPT)
            self.session.transfer_data(pickle.dumps(data_nodes), encode=False)
        self.session.close()
