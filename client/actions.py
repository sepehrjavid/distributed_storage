import pickle
import socket

from broadcast.transmitters import SimpleTransmitter
from client.exceptions import InvalidClientActionConfigFile
import ipaddress
import os

from servers.broadcast_server import BroadcastServer
from servers.valid_messages import CREATE_FILE
from session.sessions import SimpleSession


class ClientActions:
    CONFIG_FILE_PATH = "/Users/sepehrjavid/Desktop/distributed_storage/client/dfs.conf"
    DATA_NODE_NETWORK_ADDRESS = "data_node_network"
    MANDATORY_FIELDS = [DATA_NODE_NETWORK_ADDRESS]
    SOCKET_ACCEPT_TIMEOUT = 2

    def __init__(self, username, ip_address):
        self.username = username
        self.configuration = None
        self.update_config_file()
        self.ip_address = ip_address

    def update_config_file(self):
        with open(self.CONFIG_FILE_PATH, "r") as config_file:
            config = config_file.read().replace('\n', '').replace('\t', '').replace(' ', '')

        if config[-2] == ",":
            config = config[:-2] + config[-1]

        self.configuration = self.parse_config(config)

    @staticmethod
    def parse_config(config):
        config_data = config[1:-1]

        properties = config_data.split(',')
        result_config = {}
        for prop in properties:
            result_config[prop.split(':')[0]] = prop.split(':')[1]

        ClientActions.validate_config(result_config)
        return result_config

    @staticmethod
    def validate_config(config):
        keys = config.keys()
        for field in ClientActions.MANDATORY_FIELDS:
            if field not in keys:
                raise InvalidClientActionConfigFile(field)

    def send_file(self, file_path):
        broadcast_address = ipaddress.ip_network(self.configuration[self.DATA_NODE_NETWORK_ADDRESS]).broadcast_address
        transmitter = SimpleTransmitter(str(broadcast_address), BroadcastServer.BROADCAST_SERVER_PORT_NUMBER)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip_address, BroadcastServer.CLIENT_PORT_NUMBER))
        server_socket.listen(5)
        server_socket.settimeout(self.SOCKET_ACCEPT_TIMEOUT)

        transmitter.transmit(CREATE_FILE.format(total_size=os.path.getsize(file_path)))
        while True:
            try:
                client_socket, addr = server_socket.accept()
                break
            except socket.timeout:
                transmitter.transmit(CREATE_FILE.format(total_size=os.path.getsize(file_path)))

        session = SimpleSession(input_socket=client_socket, is_server=True)
        data_nodes = pickle.loads(session.receive_data(decode=False))
        session.close()
