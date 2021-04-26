from broadcast.servers import SimpleBroadcastServer
from meta_data.peer_controller import PeerController
from singleton.singleton import Singleton


class PeerServer:
    PORT_NUMBER = 50501

    pass


class PeerBroadcastServer(SimpleBroadcastServer, metaclass=Singleton):
    PORT_NUMBER = 50502

    def __init__(self, ip_address):
        super().__init__(ip_address, self.PORT_NUMBER)
        self.peer_controller = PeerController()

    def on_receive(self, source_address, data) -> None:
        pass
