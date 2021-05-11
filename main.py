from multiprocessing import Queue

from controllers.peer_controller import PeerController

if __name__ == "__main__":
    peer_controller = PeerController(Queue())
    peer_controller.start()
