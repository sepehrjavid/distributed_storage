from threading import Thread, Lock
from time import sleep
from time import time

from broadcast.servers import SimpleBroadcastServer
from servers.valid_messages import ACCEPT_CLIENT_TO_SERVE
from session.sessions import SimpleSession
from singleton.singleton import Singleton


class StorageServer(SimpleBroadcastServer, metaclass=Singleton):
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 5 * 60
    CONTROLLER_INTERVAL = 10
    STORAGE_SERVER_PORT_NUMBER = 54222
    CLIENT_PORT_NUMBER = 54223

    def __init__(self, ip_address):
        super().__init__(ip_address, self.STORAGE_SERVER_PORT_NUMBER)
        self.active_clients = []
        self.active_clients_lock = Lock()
        self.controller_thread = None

    def on_receive(self, source_address, data):
        with self.active_clients_lock:
            if len(self.active_clients) >= self.MAXIMUM_CLIENT_ALLOWED:
                return

        client_data = {
            "ip_address": source_address[0],
            "start_time": time()
        }

        client_data["thread"] = StorageClientThread(client_data)
        client_data["thread"].start()

        with self.active_clients_lock:
            self.active_clients.append(client_data)

    def __remove_expired_clients(self):
        with self.active_clients_lock:
            for client in self.active_clients:
                if time() - client["start_time"] >= self.MAXIMUM_CLIENT_HANDLE_TIME:
                    # TODO stop client thread
                    self.active_clients.remove(client)

    def __active_client_controller_thread(self):
        print("controller go")
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def run(self):
        print("server go")
        self.controller_thread = Thread(target=self.__active_client_controller_thread, args=[])
        self.controller_thread.start()
        self._start()


class StorageClientThread(Thread):
    def __init__(self, client_data, *args, **kwargs):
        super(StorageClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data

    def run(self):
        print("let's go")
        session = SimpleSession(self.client_data.get("ip_address"), StorageServer.CLIENT_PORT_NUMBER)
        session.transfer_data(ACCEPT_CLIENT_TO_SERVE)
        command = session.receive_data(time_out=2)
        if command is None:
            print("Connection failed")
            return
        print(command)
        # TODO create request
        # TODO receive chunk
        # TODO delete request
        # TODO delete chunk
