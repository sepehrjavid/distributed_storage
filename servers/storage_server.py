import socket
from threading import Thread, Lock
from time import sleep
from time import time

from singleton.singleton import Singleton


class StorageServer(metaclass=Singleton):
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 5 * 60
    CONTROLLER_INTERVAL = 10
    STORAGE_SERVER_PORT_NUMBER = 54222

    def __init__(self, ip_address):
        self.server_socket = server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((ip_address, self.STORAGE_SERVER_PORT_NUMBER))
        self.active_clients = []
        self.active_clients_lock = Lock()
        self.controller_thread = None

    def __remove_expired_clients(self):
        with self.active_clients_lock:
            for client in self.active_clients:
                if time() - client["start_time"] >= self.MAXIMUM_CLIENT_HANDLE_TIME:
                    # TODO stop client thread
                    self.active_clients.remove(client)

    def __active_client_controller(self):
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def __handle_client(self, client_data):
        print(client_data)

    def start(self):
        self.controller_thread = Thread(target=self.__active_client_controller, args=[])
        self.server_socket.listen(5)
        while True:
            client_socket, addr = self.server_socket.accept()
            client_data = {
                "ip_address": addr[0],
                "port": addr[1],
                "socket": client_socket,
                "start_time": time()
            }

            client_data["thread"] = StorageClientThread(client_data)
            client_data["thread"].start()

            with self.active_clients_lock:
                self.active_clients.append(client_data)


class StorageClientThread(Thread):
    def __init__(self, client_data, *args, **kwargs):
        super(StorageClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data

    def run(self):
        # TODO create request
        # TODO receive chunk
        # TODO delete request
        # TODO delete chunk
        pass
