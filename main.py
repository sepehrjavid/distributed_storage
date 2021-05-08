import ipaddress
import socket
from time import sleep

import parse

from broadcast.transmitters import SimpleTransmitter
from meta_data.peer_controller import PeerController
from servers.data_node_server import DataNodeServer
from servers.peer_server import PeerBroadcastServer
from servers.valid_messages import JOIN_NETWORK, INTRODUCE_PEER, CONFIRM_HANDSHAKE, NULL
from session.sessions import SimpleSession


class Main:
    CONFIG_FILE_PATH = "dfs.conf"
    SOCKET_ACCEPT_TIMEOUT = 3
    JOIN_TRY_LIMIT = 3

    def __init__(self, ip_network, ip_address, rack_number, available_byte_size):
        self.ip_address = ip_address
        self.rack_number = rack_number
        self.available_byte_size = available_byte_size
        self.ip_network = ip_network
        self.broadcast_address = str(ipaddress.ip_network(self.ip_network).broadcast_address)
        self.broadcast_server = None
        self.peer_controller = None
        self.peer_transmitter = SimpleTransmitter(broadcast_address=self.broadcast_address,
                                                  port_number=PeerBroadcastServer.PORT_NUMBER)

    def join_network(self):
        confirmation_message = CONFIRM_HANDSHAKE.format(available_byte_size=self.available_byte_size,
                                                        rack_number=self.rack_number)

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip_address, PeerController.PORT_NUMBER))
        server_socket.listen(5)
        server_socket.settimeout(self.SOCKET_ACCEPT_TIMEOUT)
        try_number = 0

        while True:
            self.peer_transmitter.transmit(JOIN_NETWORK)
            try:
                peer_socket, addr = server_socket.accept()
                break
            except socket.timeout:
                try_number += 1
                if try_number >= self.JOIN_TRY_LIMIT:
                    print("timout")
                    return PeerController(ip_address=self.ip_address, peers_sessions=[])

        peer_session = SimpleSession(input_socket=peer_socket, ip_address=addr[0], is_server=True)
        print("got a peer yay!")
        suggested_peer = peer_session.receive_data()
        suggested_peer_address = dict(parse.parse(INTRODUCE_PEER, suggested_peer).named)["ip_address"]

        print(suggested_peer_address)
        if suggested_peer_address == NULL:
            peer_session.transfer_data(confirmation_message)
            return PeerController(ip_address=self.ip_address, peers_sessions=[peer_session])

        suggested_peer_socket, addr = server_socket.accept()
        while addr[0] != suggested_peer_address:
            suggested_peer_socket.close()
            suggested_peer_socket, addr = server_socket.accept()

        suggested_peer_session = SimpleSession(input_socket=suggested_peer_socket, ip_address=suggested_peer_address,
                                               is_server=True)

        peer_session.transfer_data(confirmation_message)
        suggested_peer_session.transfer_data(confirmation_message)
        return PeerController(self.ip_address, [peer_session, suggested_peer_session])

    def run(self):
        self.peer_controller = self.join_network()
        if len(self.peer_controller.peers) != 0:
            print(self.peer_controller.peers[0].session.ip_address)
        self.broadcast_server = PeerBroadcastServer(ip_address=self.broadcast_address,
                                                    peer_controller=self.peer_controller)
        self.broadcast_server.start()


def data_node_server(ip_address, storage):
    server = DataNodeServer(ip_address=ip_address, storage=storage)
    server.run()


if __name__ == "__main__":
    main = Main(ip_address=input("ip address: "), ip_network="192.168.1.0/24",
                available_byte_size=int(input("available byte size: ")),
                rack_number=int(input("rack number: ")))

    # main = Main(ip_address="192.168.1.11", ip_network="192.168.1.0/24", available_byte_size=200, rack_number=1)
    main.run()
