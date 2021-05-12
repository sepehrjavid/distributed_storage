from multiprocessing import Queue

from controllers.peer_controller import PeerController
from storage.client_controller import ClientController

if __name__ == "__main__":
    storage_to_peer_controller = Queue()
    peer_controller_process = PeerController(activity_queue=storage_to_peer_controller)
    client_controller_process = ClientController()
    peer_controller_process.start()
    client_controller_process.start()
    peer_controller_process.join()
    client_controller_process.join()
