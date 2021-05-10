from threading import Thread
from time import time

import parse

from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from servers.valid_messages import (INTRODUCE_PEER, CONFIRM_HANDSHAKE, MESSAGE_SEPARATOR, STOP_FRIENDSHIP, NULL,
                                    RESPOND_TO_BROADCAST, REJECT, RESPOND_TO_INTRODUCTION, ACCEPT, REQUEST_DB, SEND_DB)
from session.exceptions import PeerTimeOutException
from session.sessions import EncryptedSession, SimpleSession, FileSession
from singleton.singleton import Singleton


class PeerController(metaclass=Singleton):
    PORT_NUMBER = 50502

    def __init__(self, ip_address, peers_sessions: list):
        self.ip_address = ip_address
        self.db_connection = MetaDatabase()

        self.peers = []
        for peer in peers_sessions:
            self.peers.append(PeerRecvThread(session=peer, controller=self))
            self.peers[-1].start()

        if len(self.peers) == 0:
            MetaDatabase.initialize_tables()
        else:
            self.retrieve_database()

    def retrieve_database(self):
        print(time())
        self.peers[0].session.transfer_data(REQUEST_DB)

    def lock_queue(self):
        pass

    def release_queue_lock(self):
        pass

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

        if len(self.peers) == 0:
            new_peer_session.transfer_data(INTRODUCE_PEER.format(ip_address=NULL))
        else:
            new_peer_session.transfer_data(INTRODUCE_PEER.format(ip_address=self.peers[0].session.ip_address))
            self.peers[0].session.transfer_data(INTRODUCE_PEER.format(ip_address=ip_address))

        print(f"waiting for confirmation from {new_peer_session.ip_address}")
        handshake_confirmation = new_peer_session.receive_data()
        if handshake_confirmation.split(MESSAGE_SEPARATOR)[0] != CONFIRM_HANDSHAKE.split(MESSAGE_SEPARATOR)[0]:
            new_peer_session.close()
            return
        print(f"got handshake {handshake_confirmation}")

        thread = PeerRecvThread(session=new_peer_session, controller=self)
        thread.start()
        print(time())

        meta_data = dict(parse.parse(CONFIRM_HANDSHAKE, handshake_confirmation).named)
        data_node = DataNode(db=self.db_connection, ip_address=ip_address,
                             available_byte_size=meta_data["available_byte_size"],
                             rack_number=meta_data["rack_number"], last_seen=time())
        data_node.save()

        if len(self.peers) > 1:
            self.inform_new_data_node(data_node)
            lost_peer = self.peers.pop(0)

        self.peers.append(thread)
        print(self.peers)

    def inform_new_data_node(self, data_node: DataNode):
        pass

    def update_peer(self, ip_address, available_byte_size):
        data_node = DataNode.fetch_by_ip(ip_address, self.db_connection)
        data_node.last_seen = time()
        data_node.available_byte_size = available_byte_size
        data_node.save()


class PeerRecvThread(Thread):
    def __init__(self, session: EncryptedSession, controller: PeerController, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session
        self.controller = controller
        self.continues = True
        self.db = None

    def run(self):
        self.db = MetaDatabase()
        while self.continues:
            message = self.session.receive_data()
            if message is None:
                self.perform_recovery_actions()
            else:
                self.handle_message(message)

    def handle_message(self, message):
        if message.split(MESSAGE_SEPARATOR)[0] == INTRODUCE_PEER.split(MESSAGE_SEPARATOR)[0]:
            self.add_peer(message)
        elif message == REQUEST_DB:
            self.transfer_db()
        elif message == SEND_DB:
            self.receive_db()
        elif message == STOP_FRIENDSHIP:
            self.session.close()
            self.continues = False

    def transfer_db(self):
        self.session.transfer_data(SEND_DB)
        file_session = FileSession()
        file_session.transfer_file(MetaDatabase.DATABASE_PATH, self.session)

    def receive_db(self):
        file_session = FileSession()
        file_session.receive_file(MetaDatabase.DATABASE_PATH, self.session)
        self.controller.release_queue_lock()

    def add_peer(self, message):
        ip_address = dict(parse.parse(INTRODUCE_PEER, message).named)["ip_address"]
        try:
            new_session = SimpleSession(ip_address=ip_address, port_number=PeerController.PORT_NUMBER)
            new_session.transfer_data(RESPOND_TO_INTRODUCTION)
            command = new_session.receive_data()
            if command == ACCEPT:
                new_session = new_session.convert_to_encrypted_session()
            else:
                return
        except PeerTimeOutException:
            return

        handshake_confirmation = new_session.receive_data()
        print(f"Thread got handshake {handshake_confirmation}")
        meta_data = dict(parse.parse(CONFIRM_HANDSHAKE, handshake_confirmation).named)
        data_node = DataNode(db=self.db, ip_address=ip_address,
                             available_byte_size=meta_data["available_byte_size"],
                             rack_number=meta_data["rack_number"], last_seen=time())
        data_node.save()

        if len(self.controller.peers) == 1:
            thread = PeerRecvThread(session=new_session, controller=self.controller)
            thread.start()
            self.controller.peers.append(thread)
        else:
            self.session.transfer_data(STOP_FRIENDSHIP)
            self.session = new_session
            self.controller.inform_new_data_node(data_node)

        print("Thread ", self.controller.peers)

    def perform_recovery_actions(self):
        self.session.close()
        self.db.close()
        self.continues = False
        self.controller.peers.remove(self)
        print(self.controller.peers)
