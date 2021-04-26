import pickle
from threading import Thread, Lock
from time import sleep
from time import time

import parse

from broadcast.servers import SimpleBroadcastServer
from servers.valid_messages import CREATE_FILE, DELETE_FILE, MESSAGE_SEPARATOR, OUT_OF_SPACE, ACCEPT
from session.exceptions import PeerTimeOutException
from session.sessions import SimpleSession
from singleton.singleton import Singleton


class BroadcastServer(SimpleBroadcastServer, metaclass=Singleton):
    MAXIMUM_CLIENT_ALLOWED = 30
    MAXIMUM_CLIENT_HANDLE_TIME = 3 * 60
    CONTROLLER_INTERVAL = 10
    BROADCAST_SERVER_PORT_NUMBER = 54222
    CLIENT_PORT_NUMBER = BROADCAST_SERVER_PORT_NUMBER

    def __init__(self, ip_address, storage):
        super().__init__(ip_address, self.BROADCAST_SERVER_PORT_NUMBER)
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

    def run(self):
        self.controller_thread = Thread(target=self.__active_client_controller_thread, args=[])
        self.controller_thread.start()
        self._start()


class ClientThread(Thread):
    def __init__(self, client_data, storage, *args, **kwargs):
        super(ClientThread, self).__init__(*args, **kwargs)
        self.client_data = client_data
        self.storage = storage

    def run(self):
        try:
            session = SimpleSession(ip_address=self.client_data.get("ip_address"),
                                    port_number=BroadcastServer.CLIENT_PORT_NUMBER)
        except PeerTimeOutException:
            return

        command = self.client_data.get("command")

        if command.split(MESSAGE_SEPARATOR)[0] == CREATE_FILE.split(MESSAGE_SEPARATOR)[0]:
            self.create_file(session, command)
        elif command.split(MESSAGE_SEPARATOR)[0] == CREATE_FILE.split(MESSAGE_SEPARATOR)[0]:
            pass
        elif command == DELETE_FILE:
            self.delete_file(session)

    def create_file(self, session, command):
        file_size = int(dict(parse.parse(CREATE_FILE, command).named)["total_size"])
        data_nodes = self.storage.choose_data_node_to_save(file_size)
        if data_nodes is None:
            print("out of space")
            session.transfer_data(OUT_OF_SPACE)
        else:
            session.transfer_data(ACCEPT)
            session.transfer_data(pickle.dumps(data_nodes), encode=False)
        session.close()

    def delete_file(self, session):
        pass

    def retrieve_file(self, session, command):
        pass
