from multiprocessing import Pipe

from controllers.peer_controller import PeerController
from controllers.client_controller import ClientController

if __name__ == "__main__":
    peer_controller_side, client_controller_side = Pipe()
    peer_controller_process = PeerController(client_controller_pipe=peer_controller_side)
    client_controller_process = ClientController(peer_controller_pipe=client_controller_side)
    peer_controller_process.start()
    client_controller_process.start()
    peer_controller_process.join()
    client_controller_process.join()
