import ipaddress
from multiprocessing import Process
from multiprocessing.connection import Connection
from threading import Thread
from time import sleep

from controllers.peer_controller import PeerController
from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from servers.broadcast_server import BroadcastServer
from servers.data_node_server import DataNodeServer
from singleton.singleton import Singleton
from storage.storage import Storage
from valid_messages import START_CLIENT_SERVER, DELETE_CHUNK, MESSAGE_SEPARATOR


class ClientController(Process, metaclass=Singleton):
    def __init__(self, peer_controller_pipe: Connection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = None
        self.update_config_file()
        print("DFS configuration loaded successfully")
        self.ip_address = self.config.get("ip_address")
        self.network_id = self.config.get("network_id")
        self.rack_number = self.config.get("rack_number")
        self.available_byte_size = self.config.get("available_byte_size")
        self.storage_base_path = self.config.get("path")
        self.broadcast_address = str(ipaddress.ip_network(self.network_id).broadcast_address)

        self.peer_controller_pipe = peer_controller_pipe
        self.db_connection = None
        self.storage = None
        self.data_node_server = None
        self.broadcast_server = None
        self.data_node_server_thread = None

    def update_config_file(self):
        with open(PeerController.CONFIG_FILE_PATH, "r") as config_file:
            config = config_file.read().replace('\n', '').replace('\t', '').replace(' ', '')

        if config[-2] == ",":
            config = config[:-2] + config[-1]

        self.config = PeerController.parse_config(config)

    def run(self):
        self.db_connection = MetaDatabase()
        while True:
            if self.peer_controller_pipe.poll():
                msg = self.peer_controller_pipe.recv()
                if msg == START_CLIENT_SERVER:
                    break
            sleep(1)

        self.storage = Storage(storage_path=self.storage_base_path,
                               current_data_node=DataNode.fetch_by_ip(ip_address=self.ip_address,
                                                                      db=self.db_connection), controller=self)
        self.data_node_server = DataNodeServer(ip_address=self.ip_address, storage=self.storage)
        self.broadcast_server = BroadcastServer(broadcast_address=self.broadcast_address, storage=self.storage)
        self.data_node_server_thread = Thread(target=self.data_node_server.run, args=[])
        self.data_node_server_thread.start()
        print("data node server started")
        self.broadcast_server.start()

    def inform_modification(self, message):
        self.peer_controller_pipe.send(message)

    def peer_controller_message_handler(self):
        db = MetaDatabase()
        while True:
            if self.peer_controller_pipe.poll():
                msg = self.peer_controller_pipe.recv()
                command = msg.split(MESSAGE_SEPARATOR)[0]
                if command == DELETE_CHUNK.split(MESSAGE_SEPARATOR)[0]:
                    self.storage.remove_chunk_file(path=DELETE_CHUNK.split(MESSAGE_SEPARATOR)[1], db=db)
            sleep(5)
