from time import time

from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from servers.peer_server import PeerServer
from session.sessions import SimpleSession
from singleton.singleton import Singleton


class PeerController(metaclass=Singleton):
    def __init__(self, ip_address, db=None):
        if db is None:
            self.db = MetaDatabase()
        else:
            self.db = db

        self.peers = []

    def get_destinations(self):
        return [x.ip_address for x in DataNode.fetch_all(self.db)]

    def inform_new_directory(self, **kwargs):
        print(self.get_destinations)

    def add_peer(self, ip_address, available_byte_size, rack_number):
        peer_session = SimpleSession(ip_address=ip_address, port_number=PeerServer.PORT_NUMBER, is_server=True)

        data_node = DataNode(db=self.db, ip_address=ip_address, available_byte_size=available_byte_size,
                             rack_number=rack_number)
        data_node.save()

        self.peers.append({"ip": ip_address, "session": peer_session})

    def update_peer(self, ip_address, available_byte_size):
        data_node = DataNode.fetch_by_ip(ip_address, self.db)
        data_node.last_seen = time()
        data_node.available_byte_size = available_byte_size
        data_node.save()
