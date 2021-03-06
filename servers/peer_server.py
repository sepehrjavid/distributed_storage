import parse

from broadcast.servers import SimpleBroadcastServer
from meta_data.models.data_node import DataNode
from valid_messages import (JOIN_NETWORK, UNBLOCK_QUEUEING, BLOCK_QUEUEING, PEER_FAILURE, MESSAGE_SEPARATOR,
                            RESPOND_PEER_FAILURE, NAME_NODE_DOWN)
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
        elif data.decode() == UNBLOCK_QUEUEING:
            self.peer_controller.release_queue_lock()
            print("unblocked")
        elif data.decode() == BLOCK_QUEUEING:
            self.peer_controller.lock_queue()
            print("blocked")
        elif data.decode().split(MESSAGE_SEPARATOR)[0] == NAME_NODE_DOWN.split(MESSAGE_SEPARATOR)[0]:
            print("name node down")
            meta_data = dict(parse.parse(NAME_NODE_DOWN, data.decode()).named)
            name_node = DataNode.fetch_by_ip(ip_address=meta_data.get("name_node_address"),
                                             db=self.peer_controller.db_connection)
            if name_node is not None:
                name_node.delete()
            self.peer_controller.update_name_node_ip_address(db=self.peer_controller.db_connection)
        elif data.decode().split(MESSAGE_SEPARATOR)[0] == PEER_FAILURE.split(MESSAGE_SEPARATOR)[0]:
            self.peer_controller.respond_to_peer_failure(data.decode())
            print("data node failure detected")
        elif data.decode().split(MESSAGE_SEPARATOR)[0] == RESPOND_PEER_FAILURE.split(MESSAGE_SEPARATOR)[0]:
            self.peer_controller.handle_peer_failure_response(data.decode())
            print("response to data node failure received")

    def start(self):
        print("Broadcast server started")
        self._start()
