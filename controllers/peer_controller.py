import ipaddress
import socket
from multiprocessing import Process
from multiprocessing.connection import Connection

from threading import Thread, Event
from time import monotonic, sleep

import parse

from broadcast.transmitters import SimpleTransmitter
from controllers.exceptions import InvalidDataNodeConfigFile, InvalidValueForConfigFiled
from controllers.peer_recv_thread import PeerRecvThread
from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from servers.peer_server import PeerBroadcastServer
from valid_messages import (INTRODUCE_PEER, CONFIRM_HANDSHAKE, MESSAGE_SEPARATOR, NULL, RESPOND_TO_BROADCAST,
                            REJECT, JOIN_NETWORK, ACCEPT, RESPOND_TO_INTRODUCTION, BLOCK_QUEUEING,
                            UNBLOCK_QUEUEING, ABORT_JOIN, SEND_DB, START_CLIENT_SERVER, PEER_FAILURE,
                            RESPOND_PEER_FAILURE, NAME_NODE_STATUS)
from session.exceptions import PeerTimeOutException
from session.sessions import SimpleSession, FileSession
from singleton.singleton import Singleton


class PeerController(Process, metaclass=Singleton):
    PORT_NUMBER = 50502
    CONFIG_FILE_PATH = "dfs.conf"
    SOCKET_ACCEPT_TIMEOUT = 3
    JOIN_TRY_LIMIT = 3
    MANDATORY_FIELDS = ["ip_address", "network_id", "rack_number", "available_byte_size", "path", "priority"]

    def __init__(self, client_controller_pipe: Connection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = None
        self.update_config_file()
        print("DFS configuration loaded successfully")
        self.ip_address = self.config.get("ip_address")
        self.network_id = self.config.get("network_id")
        self.rack_number = int(self.config.get("rack_number"))
        self.priority = int(self.config.get("priority"))
        if self.priority < 0:
            raise InvalidValueForConfigFiled("priority")
        self.available_byte_size = self.config.get("available_byte_size")
        self.broadcast_address = str(ipaddress.ip_network(self.network_id).broadcast_address)
        self.peer_transmitter = SimpleTransmitter(broadcast_address=self.broadcast_address,
                                                  port_number=PeerBroadcastServer.PORT_NUMBER)

        self.client_controller_pipe = client_controller_pipe
        self.peers = []
        self.broadcast_server = PeerBroadcastServer(broadcast_address=self.broadcast_address, peer_controller=self)
        self.name_node_ip_address = None

    def update_config_file(self):
        with open(self.CONFIG_FILE_PATH, "r") as config_file:
            config = config_file.read().replace('\n', '').replace('\t', '').replace(' ', '')

        if config[-2] == ",":
            config = config[:-2] + config[-1]

        self.config = self.parse_config(config)

    @property
    def is_name_node(self):
        return self.name_node_ip_address == self.ip_address

    def update_name_node_ip_address(self, db: MetaDatabase):
        self.name_node_ip_address = self.find_name_node(db=db)
        self.update_client_controller_is_name_node()
        print(f"Name node updated to {self.name_node_ip_address}")

    def update_client_controller_is_name_node(self):
        self.client_controller_pipe.send(NAME_NODE_STATUS.format(status=self.is_name_node))

    @staticmethod
    def find_name_node(db: MetaDatabase):
        all_nodes = DataNode.fetch_all(db=db)
        highest_priority = min([x.priority for x in all_nodes])
        candidates = [x for x in all_nodes if x.priority == highest_priority]
        candidates.sort(key=lambda x: x.ip_address)
        return candidates[0].ip_address

    @staticmethod
    def parse_config(config):
        config_data = config[1:-1]

        properties = config_data.split(',')
        result_config = {}
        for prop in properties:
            result_config[prop.split(':')[0]] = prop.split(':')[1]

        PeerController.is_config_valid(result_config)
        return result_config

    @staticmethod
    def is_config_valid(config: dict):
        keys = config.keys()
        for field in PeerController.MANDATORY_FIELDS:
            if field not in keys:
                raise InvalidDataNodeConfigFile(field)

    def lock_queue(self):
        self.activity_lock.clear()

    def release_queue_lock(self):
        self.activity_lock.set()

    def handle_storage_process_messages(self, activity_lock: Event):
        i = -1
        while True:
            i += 1
            activity_lock.wait()
            print(f"number: {i}")
            if self.client_controller_pipe.poll():
                message = self.client_controller_pipe.recv()
                self.inform_next_node(message)
            sleep(1)

    def add_peer(self, ip_address):
        print(f"ready to add peer {ip_address}")
        try:
            new_peer_session = SimpleSession(ip_address=ip_address, port_number=self.PORT_NUMBER)
            new_peer_session.transfer_data(RESPOND_TO_BROADCAST)
            response = new_peer_session.receive_data()
            if response == REJECT:
                new_peer_session.close()
                print("peer did not accept my help :(")
                return
            new_peer_session = new_peer_session.convert_to_encrypted_session()
        except PeerTimeOutException:
            print("peer did not accept my help :(")
            return

        self.peer_transmitter.transmit(BLOCK_QUEUEING)
        self.lock_queue()

        if len(self.peers) == 0:
            new_peer_session.transfer_data(INTRODUCE_PEER.format(ip_address=NULL))
        else:
            new_peer_session.transfer_data(INTRODUCE_PEER.format(ip_address=self.peers[0].session.ip_address))
            self.peers[0].session.transfer_data(INTRODUCE_PEER.format(ip_address=ip_address))

        print(f"waiting for confirmation from {new_peer_session.ip_address}")
        handshake_confirmation = new_peer_session.receive_data()
        if handshake_confirmation.split(MESSAGE_SEPARATOR)[0] != CONFIRM_HANDSHAKE.split(MESSAGE_SEPARATOR)[0]:
            new_peer_session.close()
            self.peer_transmitter.transmit(UNBLOCK_QUEUEING)
            self.release_queue_lock()
            return
        print(f"got handshake {handshake_confirmation}")

        thread = PeerRecvThread(session=new_peer_session, controller=self)
        thread.start()

        meta_data = dict(parse.parse(CONFIRM_HANDSHAKE, handshake_confirmation).named)
        data_node = DataNode(db=self.db_connection, ip_address=ip_address,
                             available_byte_size=meta_data.get("available_byte_size"),
                             rack_number=meta_data.get("rack_number"), priority=meta_data.get("priority"),
                             last_seen=monotonic())
        data_node.save()
        self.update_name_node_ip_address(db=self.db_connection)

        if len(self.peers) > 1:
            lost_peer = self.peers.pop(0)
            lost_peer.join()

        self.peers.append(thread)
        self.transfer_db(thread.session)
        print([x.session.ip_address for x in self.peers])

    def inform_next_node(self, message, previous_signature: str = ""):
        seen_data_nodes = previous_signature.split(MESSAGE_SEPARATOR)
        for peer in self.peers:
            if not peer.failed and peer.session.ip_address not in seen_data_nodes:
                peer.session.transfer_data(message)

    def join_network(self) -> list:
        confirmation_message = CONFIRM_HANDSHAKE.format(available_byte_size=self.available_byte_size,
                                                        priority=self.priority,
                                                        rack_number=self.rack_number)

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.ip_address, PeerController.PORT_NUMBER))
        server_socket.listen(2)
        server_socket.settimeout(self.SOCKET_ACCEPT_TIMEOUT)
        try_number = 0

        while True:
            self.peer_transmitter.transmit(JOIN_NETWORK)
            try:
                peer_socket, addr = server_socket.accept()
                session = SimpleSession(input_socket=peer_socket, ip_address=addr[0])
                command = session.receive_data()
                if command == RESPOND_TO_BROADCAST:
                    session.transfer_data(ACCEPT)
                    break
                else:
                    session.transfer_data(REJECT)
                    session.close()
                    raise socket.timeout
            except socket.timeout:
                try_number += 1
                if try_number >= self.JOIN_TRY_LIMIT:
                    print("timout")
                    return []

        peer_session = session.convert_to_encrypted_session(is_server=True)
        print(f"got peer {peer_session.ip_address}!")
        suggested_peer = peer_session.receive_data()
        suggested_peer_address = dict(parse.parse(INTRODUCE_PEER, suggested_peer).named)["ip_address"]

        print(f"got suggested {suggested_peer_address}")
        if suggested_peer_address == NULL:
            peer_session.transfer_data(confirmation_message)
            return [peer_session]

        suggested_peer_socket, addr = server_socket.accept()
        session = SimpleSession(input_socket=suggested_peer_socket, ip_address=addr[0])
        command = session.receive_data()
        while session.ip_address != suggested_peer_address or command != RESPOND_TO_INTRODUCTION:
            session.transfer_data(REJECT)
            session.close()
            try:
                suggested_peer_socket, addr = server_socket.accept()
            except socket.timeout:
                peer_session.transfer_data(ABORT_JOIN)
                peer_session.close()
                return []
            session = SimpleSession(input_socket=suggested_peer_socket, ip_address=addr[0])
            command = session.receive_data()
        session.transfer_data(ACCEPT)

        suggested_peer_session = session.convert_to_encrypted_session(is_server=True)
        print(f"connected to {suggested_peer_session.ip_address}")

        peer_session.transfer_data(confirmation_message)
        suggested_peer_session.transfer_data(confirmation_message)
        server_socket.close()
        print("confirmed both peers")
        return [peer_session, suggested_peer_session]

    def respond_to_peer_failure(self, message):
        meta_data = dict(parse.parse(PEER_FAILURE, message).named)
        failed_address = meta_data.get("failed_address")
        reporter_address = meta_data.get("reporter_address")

        for peer in self.peers:
            if peer.failed and peer.controller_inbox == failed_address:
                self.peer_transmitter.transmit(RESPOND_PEER_FAILURE.format(
                    failed_address=failed_address,
                    reporter_address=reporter_address,
                    self_ip_address=self.ip_address
                ))

    def handle_peer_failure_response(self, message):
        meta_data = dict(parse.parse(RESPOND_PEER_FAILURE, message).named)
        failed_address = meta_data.get("failed_address")
        reporter_address = meta_data.get("reporter_address")

        if self.ip_address == reporter_address:
            for peer in self.peers:
                if peer.failed and peer.controller_inbox == failed_address:
                    peer.thread_inbox = meta_data.get("self_ip_address")
                    peer.failure_help_found.set()
                    break

    # noinspection PyAttributeOutsideInit
    def run(self):
        self.db_connection = MetaDatabase()
        self.activity_lock = Event()
        self.activity_lock.set()
        self.storage_communicator_thread = Thread(target=self.handle_storage_process_messages,
                                                  args=[self.activity_lock])
        self.storage_communicator_thread.daemon = True

        peers_sessions = self.join_network()
        for peer in peers_sessions:
            self.peers.append(PeerRecvThread(session=peer, controller=self))
            self.peers[-1].start()
        print([x.session.ip_address for x in self.peers])

        if len(self.peers) == 0:
            MetaDatabase.initialize_tables()
            DataNode(db=self.db_connection, ip_address=self.ip_address, rack_number=self.rack_number,
                     available_byte_size=self.available_byte_size, last_seen=monotonic(),
                     priority=self.priority).save()
            self.client_controller_pipe.send(START_CLIENT_SERVER)
            self.update_name_node_ip_address(db=self.db_connection)

        self.storage_communicator_thread.start()
        print("Storage communicator started")
        self.broadcast_server.start()

    @staticmethod
    def transfer_db(session):
        session.transfer_data(SEND_DB)
        file_session = FileSession()
        file_session.transfer_file(MetaDatabase.DATABASE_PATH, session)
        print("transfer database")
