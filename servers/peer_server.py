from broadcast.servers import SimpleBroadcastServer
from servers.valid_messages import JOIN_NETWORK, UNBLOCK_QUEUEING, BLOCK_QUEUEING
from singleton.singleton import Singleton


class PeerBroadcastServer(SimpleBroadcastServer, metaclass=Singleton):
    PORT_NUMBER = 50501

    def __init__(self, broadcast_address, peer_controller):
        super().__init__(broadcast_address, self.PORT_NUMBER)
        self.peer_controller = peer_controller

    def on_receive(self, source_address, data):
        if source_address[0] == self.peer_controller.ip_address:
            return

        if data.decode() == JOIN_NETWORK:
            print("got join message yay!")
            self.peer_controller.add_peer(source_address[0])
        elif data == UNBLOCK_QUEUEING:
            self.peer_controller.release_queue_lock()
        elif data == BLOCK_QUEUEING:
            self.peer_controller.lock_queue()

    def start(self):
        print("Broadcast server started")
        self._start()
