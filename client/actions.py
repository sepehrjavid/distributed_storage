import ntpath
import pickle
import socket
from threading import Thread, Condition

from broadcast.transmitters import SimpleTransmitter
from client.exceptions import InvalidClientActionConfigFile
import ipaddress
import os

from servers.broadcast_server import BroadcastServer
from servers.data_node_server import DataNodeServer
from valid_messages import CREATE_FILE, CREATE_CHUNK, ACCEPT, OUT_OF_SPACE, LOGIN, CREDENTIALS, CREATE_ACCOUNT, \
    GET_FILE, INVALID_PATH, FILE_DOES_NOT_EXIST, NO_PERMISSION, CORRUPTED_FILE, GET_CHUNK, CREATE_DIR, \
    DUPLICATE_DIR_NAME, DELETE_FILE, ADD_DIR_PERM, INVALID_USERNAME
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
        self.received_seq = 0
        self.write_chunk_condition = Condition()

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

    def __send_chunk(self, ip_address, file_path, sequence, chunk_size, logical_path, filename, extension, offset):
        session = EncryptedSession(ip_address=ip_address, port_number=DataNodeServer.DATA_NODE_PORT_NUMBER)
        session.transfer_data(CREATE_CHUNK.format(path=logical_path, title=filename, sequence=sequence,
                                                  chunk_size=chunk_size, username=self.username, extension=extension))
        response = session.receive_data()
        if response == ACCEPT:
            file_session = FileSession()
            file_session.transfer_file(file_path, session=session, offset=offset, size=chunk_size)
        else:
            print(response)
            session.close()

    def ask_for_service(self, message) -> EncryptedSession:
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

        return EncryptedSession(input_socket=client_socket, is_server=True)

    def create_account(self):
        username = input("Username: ")
        password = input("Password: ")

        session = self.ask_for_service(CREATE_ACCOUNT)
        session.transfer_data(CREDENTIALS.format(username=username, password=password))
        response = session.receive_data()

        if response == ACCEPT:
            self.username = username
            return True
        return False

    def authenticate(self):
        username = input("Username: ")
        password = input("Password: ")

        session = self.ask_for_service(LOGIN)
        session.transfer_data(CREDENTIALS.format(username=username, password=password))
        response = session.receive_data()

        if response == ACCEPT:
            self.username = username
            return True
        return False

    def send_file(self):
        file_path = input("Enter the desired file path:\n")
        logical_path = input("Enter the desired logical path:\n")

        file_detail = ntpath.basename(file_path).split(".")

        filename = file_detail[0]
        if len(file_detail) == 1:
            extension = None
        else:
            extension = file_detail[1]

        session = self.ask_for_service(message=CREATE_FILE.format(total_size=os.path.getsize(file_path),
                                                                  path=logical_path,
                                                                  username=self.username,
                                                                  title=filename,
                                                                  extension=extension
                                                                  ))

        response = session.receive_data()
        if response == OUT_OF_SPACE:
            print("The file system is out of space")
            return
        elif response == ACCEPT:
            chunk_instructions = pickle.loads(session.receive_data(decode=False))
        else:
            print(response)
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

        offset = 0
        for i in range(len(chunk_instructions)):
            chunk_threads.append(Thread(target=self.__send_chunk,
                                        args=[chunk_instructions[i][1], file_path, i + 1, chunk_instructions[i][0],
                                              logical_path, filename, extension, offset]))
            offset += chunk_instructions[i][0]
            chunk_threads[-1].start()

        for thread in chunk_threads:
            thread.join()

    def __receive_chunk(self, ip_address, sequence, file, logical_path):
        session = EncryptedSession(ip_address=ip_address, port_number=DataNodeServer.DATA_NODE_PORT_NUMBER)
        session.transfer_data(GET_CHUNK.format(username=self.username, sequence=sequence, path=logical_path))
        response = session.receive_data()

        if response != ACCEPT:
            print(f"{response} occurred when retrieving from {ip_address}")
            return

        file_session = FileSession()
        data = file_session.receive_chunk(session)
        session.close()

        with self.write_chunk_condition:
            while self.received_seq != sequence:
                self.write_chunk_condition.wait()
            file.write(data)
            self.received_seq += 1
            self.write_chunk_condition.notifyAll()

    def retrieve_file(self):
        logical_file_path = input("File path: ")
        save_to_path = input("Save to path: ")
        filename = logical_file_path.split("/")[-1]

        session = self.ask_for_service(GET_FILE.format(path=logical_file_path, username=self.username))

        response = session.receive_data(decode=False)
        session.close()

        if response == INVALID_PATH.encode() or response == FILE_DOES_NOT_EXIST.encode():
            print("Invalid File Path")
            return
        elif response == NO_PERMISSION.encode():
            print("Permission Denied")
            return
        elif response == CORRUPTED_FILE.encode():
            print("The Requested File is Corrupted")
            return

        chunk_list = pickle.loads(response)
        chunk_list.sort(key=lambda x: x[0])
        print(chunk_list)

        """
                chunk list's structure is as followed:
                chunk_list = [(sequence, ip_address), (sequence, ip_address)]
        """

        self.received_seq = 1
        threads = []
        file = open(save_to_path + filename, "wb")
        for chunk in chunk_list:
            threads.append(Thread(target=self.__receive_chunk, args=[chunk[1], chunk[0], file, logical_file_path]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        file.close()

    def create_new_dir(self):
        path = input("Enter path: ")
        dir_name = input("Enter directory name: ")

        session = self.ask_for_service(CREATE_DIR.format(path=path + "/" + dir_name, username=self.username))

        response = session.receive_data()
        session.close()

        if response == ACCEPT:
            print("Success!")
        elif response == INVALID_PATH:
            print("Invalid Directory Path")
        elif response == NO_PERMISSION:
            print("Permission Denied")
        elif response == DUPLICATE_DIR_NAME:
            print("Directory name already exists under this path")

    def remove_file(self):
        logical_path = input("Enter the path: ")

        session = self.ask_for_service(DELETE_FILE.format(path=logical_path, username=self.username))

        response = session.receive_data()
        session.close()

        if response == ACCEPT:
            print("Success!")
        elif response == INVALID_PATH or response == FILE_DOES_NOT_EXIST:
            print("Invalid File Path")
        elif response == NO_PERMISSION:
            print("Permission Denied")

    def grant_directory_permission(self):
        path = input("Enter the directory path: ")
        user = input("Enter the username to grant permission to: ")

        session = self.ask_for_service(ADD_DIR_PERM.format(owner_username=self.username, path=path, perm_username=user))

        response = session.receive_data()

        if response == ACCEPT:
            print("Success!")
        elif response == INVALID_PATH:
            print("Invalid File Path")
        elif response == NO_PERMISSION:
            print("Permission Denied")
        elif response == INVALID_USERNAME:
            print("Invalid Username")
