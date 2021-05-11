from multiprocessing import Queue

from controllers.peer_controller import PeerController
from servers.data_node_server import DataNodeServer

if __name__ == "__main__":
    peer_controller = PeerController(Queue())
    peer_controller.start()
