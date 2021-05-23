import ntpath
import pickle
import socket
from threading import Thread

from broadcast.transmitters import SimpleTransmitter
from client.exceptions import InvalidClientActionConfigFile
import ipaddress
import os

from servers.broadcast_server import BroadcastServer
from servers.data_node_server import DataNodeServer
from valid_messages import CREATE_FILE, CREATE_CHUNK, ACCEPT, OUT_OF_SPACE, LOGIN, CREDENTIALS, CREATE_ACCOUNT
from session.sessions import EncryptedSession, FileSession


class ClientActions:
    CONFIG_FILE_PATH = "/Users/sepehrjavid/Desktop/distributed_storage/client/dfs.conf"
    DATA_NODE_NETWORK_ADDRESS = "data_node_network"
    MANDATORY_FIELDS = [DATA_NODE_NETWORK_ADDRESS]
    SOCKET_ACCEPT_TIMEOUT = 2

    def __init__(self, ip_address):
        self.username = None
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

    def __send_chunk(self, ip_address, file_path, sequence, chunk_size, logical_path, filename, extension):
        session = EncryptedSession(ip_address=ip_address, port_number=DataNodeServer.DATA_NODE_PORT_NUMBER)
        session.transfer_data(CREATE_CHUNK.format(path=logical_path, title=filename, sequence=sequence,
                                                  chunk_size=chunk_size, username=self.username, extension=extension))
        response = session.receive_data()
        if response == ACCEPT:
            file_session = FileSession()
            file_session.transfer_file(file_path, session=session)
        else:
            print(response)
            session.close()

    def ask_for_service(self, message):
        broadcast_address = ipaddress.ip_network(self.configuration[self.DATA_NODE_NETWORK_ADDRESS]).broadcast_address
        transmitter = SimpleTransmitter(str(broadcast_address), BroadcastServer.BROADCAST_SERVER_PORT_NUMBER)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip_address, BroadcastServer.CLIENT_PORT_NUMBER))
        server_socket.listen(5)
        server_socket.settimeout(self.SOCKET_ACCEPT_TIMEOUT)

        transmitter.transmit(message)
        while True:
            try:
                client_socket, addr = server_socket.accept()
                break
            except socket.timeout:
                transmitter.transmit(message)

        return client_socket

    def create_account(self):
        username = input("Username: ")
        password = input("Password: ")

        client_socket = self.ask_for_service(CREATE_ACCOUNT)
        session = EncryptedSession(input_socket=client_socket, is_server=True)
        session.transfer_data(CREDENTIALS.format(username=username, password=password))
        response = session.receive_data()

        if response == ACCEPT:
            self.username = username
            return True
        return False

    def authenticate(self):
        username = input("Username: ")
        password = input("Password: ")

        client_socket = self.ask_for_service(LOGIN)
        session = EncryptedSession(input_socket=client_socket, is_server=True)
        session.transfer_data(CREDENTIALS.format(username=username, password=password))
        response = session.receive_data()

        if response == ACCEPT:
            self.username = username
            return True
        return False

    def send_file(self, file_path, logical_path):
        file_detail = ntpath.basename(file_path).split(".")

        filename = file_detail[0]
        if len(file_detail) == 1:
            extension = None
        else:
            extension = file_detail[1]

        client_socket = self.ask_for_service(message=CREATE_FILE.format(total_size=os.path.getsize(file_path),
                                                                        path=logical_path,
                                                                        username=self.username,
                                                                        title=filename,
                                                                        extension=extension
                                                                        ))

        session = EncryptedSession(input_socket=client_socket, is_server=True)
        response = session.receive_data()
        if response == OUT_OF_SPACE:
            print("The file system is out of space")
            return
        elif response == ACCEPT:
            chunk_instructions = pickle.loads(session.receive_data(decode=False))
        else:
            print("Something went wrong!")
            return

        session.close()
        print(chunk_instructions)

        """
        chunk instructions' structure is as followed:
        chunk_instructions = [(size, ip_address), (size, ip_address)]
        """

        chunk_threads = []
        print("Sending file...")

        for i in range(len(chunk_instructions)):
            chunk_threads.append(Thread(target=self.__send_chunk,
                                        args=[chunk_instructions[i][1], file_path, i + 1, chunk_instructions[i][0],
                                              logical_path, filename, extension]))
            chunk_threads[-1].start()

        for thread in chunk_threads:
            thread.join()
