import pickle
from threading import Thread, Lock
from time import sleep
from time import time

from broadcast.servers import SimpleBroadcastServer
from servers.valid_messages import CREATE_FILE, DELETE_FILE, CREATE_CHUNK
from session.exceptions import PeerTimeOutException
from session.sessions import SimpleSession
from singleton.singleton import Singleton


class StorageClientServer(SimpleBroadcastServer, metaclass=Singleton):
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 5 * 60
    CONTROLLER_INTERVAL = 10
    STORAGE_SERVER_PORT_NUMBER = 54222
    CLIENT_PORT_NUMBER = 54223

    def __init__(self, ip_address, storage):
        super().__init__(ip_address, self.STORAGE_SERVER_PORT_NUMBER)
        self.active_clients = []
        self.active_clients_lock = Lock()
        self.controller_thread = None
        self.storage = storage

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
                    # TODO kill client thread
                    self.active_clients.remove(client)

    def __active_client_controller_thread(self):
        while True:
            sleep(self.CONTROLLER_INTERVAL)
            self.__remove_expired_clients()

    def run(self):
        self.controller_thread = Thread(target=self.__active_client_controller_thread, args=[])
        self.controller_thread.start()
        self._start()


class StorageClientThread(Thread):
    def __init__(self, client_data, storage, *args, **kwargs):
        super(StorageClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data
        self.storage = storage

    def run(self):
        try:
            session = SimpleSession(self.client_data.get("ip_address"), StorageClientServer.CLIENT_PORT_NUMBER)
        except PeerTimeOutException:
            return

        command = session.receive_data()

        if command == CREATE_FILE:
            self.create_file(session)
        elif command == DELETE_FILE:
            self.delete_file(session)

    def create_file(self, session):
        file_spec = session.receive_data()
        data_nodes = self.storage.choose_data_node_to_save()
        session.transfer_data(pickle.dumps(data_nodes))
        session.close()

    def delete_file(self, session):
        pass
