from broadcast.servers import SimpleBroadcastServer
from servers.valid_messages import JOIN_NETWORK
from singleton.singleton import Singleton


class PeerBroadcastServer(SimpleBroadcastServer, metaclass=Singleton):
    PORT_NUMBER = 50501

    def __init__(self, broadcast_address, peer_controller):
        super().__init__(broadcast_address, self.PORT_NUMBER)
        self.peer_controller = peer_controller

    def on_receive(self, source_address, data):
        if data.decode() == JOIN_NETWORK and source_address[0] != self.peer_controller.ip_address:
            print("got join message yay!")
            self.peer_controller.lock_queue()
            print("slm")
            self.peer_controller.add_peer(source_address[0])

    def start(self):
        print("Broadcast server started")
        self._start()
