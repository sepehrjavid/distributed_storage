from servers.valid_messages import CREATE_CHUNK, DELETE_CHUNK, INVALID_METADATA
from session.sessions import SimpleSession, FileSession
from singleton.singleton import Singleton
from threading import Thread, Lock
from time import time, sleep
import socket


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
                if time() - client["start_time"] >= self.MAXIMUM_CLIENT_HANDLE_TIME:
                    # TODO kill client thread
                    self.active_clients.remove(client)

    def __active_client_controller_thread(self):
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def run(self):
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

    def run(self):
        session = SimpleSession(input_socket=self.client_data.get("socket"), is_server=True)
        command = session.receive_data()
        if command == CREATE_CHUNK:
            meta_data = session.receive_data()
            if self.storage.is_valid_metadata(meta_data):
                destination_file_path = self.storage.get_new_file_path()
                file_session = FileSession()
                file_session.receive_file(destination_file_path,
                                          replication_list=self.storage.get_replication_data_nodes())
            else:
                session.transfer_data(INVALID_METADATA)

        elif command == DELETE_CHUNK:
            pass
