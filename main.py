import ipaddress
import socket
from time import sleep

import parse

from broadcast.transmitters import SimpleTransmitter
from meta_data.peer_controller import PeerController
from servers.data_node_server import DataNodeServer
from servers.peer_server import PeerBroadcastServer
from servers.valid_messages import JOIN_NETWORK, INTRODUCE_PEER, CONFIRM_HANDSHAKE
from session.sessions import SimpleSession


class Main:
    CONFIG_FILE_PATH = "dfs.conf"

    def __init__(self, ip_address, rack_number, available_byte_size):
        self.ip_address = ip_address
        self.rack_number = rack_number
        self.available_byte_size = available_byte_size
        self.broadcast_address = ipaddress.ip_network(self.ip_address).broadcast_address
        self.peer_transmitter = SimpleTransmitter(broadcast_address=self.broadcast_address,
                                                  port_number=PeerBroadcastServer.PORT_NUMBER)

    def join_network(self):
        self.peer_transmitter.transmit(JOIN_NETWORK)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip_address, PeerController.PORT_NUMBER))
        server_socket.listen(5)
        peer_socket, addr = server_socket.accept()

        peer_session = SimpleSession(input_socket=peer_socket, ip_address=addr[0], is_server=True)
        suggested_peer = peer_session.receive_data()
        suggested_peer_address = dict(parse.parse(INTRODUCE_PEER, suggested_peer).named)["ip_address"]

        suggested_peer_socket, addr = server_socket.accept()
        while addr[0] != suggested_peer_address:
            suggested_peer_socket.close()
            suggested_peer_socket, addr = server_socket.accept()

        suggested_peer_session = SimpleSession(input_socket=suggested_peer_socket, ip_address=suggested_peer_address,
                                               is_server=True)

        confirmation_message = CONFIRM_HANDSHAKE.format(available_byte_size=self.available_byte_size,
                                                        rack_number=self.rack_number)
        peer_session.transfer_data(confirmation_message)
        suggested_peer_session.transfer_data(confirmation_message)
        return PeerController(self.ip_address, [peer_session, suggested_peer_session])

    def run(self):
        self.join_network()
        while True:
            sleep(1)


def data_node_server(ip_address, storage):
    server = DataNodeServer(ip_address=ip_address, storage=storage)
    server.run()


if __name__ == "__main__":
    main = Main(ip_address=input("ip address: "), available_byte_size=int(input("available byte size: ")),
                rack_number=int(input("rack number: ")))
    main.run()
