from threading import Thread
from time import monotonic

import parse

from meta_data.database import MetaDatabase
from meta_data.models.data_node import DataNode
from servers.valid_messages import (CONFIRM_HANDSHAKE, STOP_FRIENDSHIP, RESPOND_TO_INTRODUCTION, ACCEPT, INTRODUCE_PEER,
                                    MESSAGE_SEPARATOR, SEND_DB, UPDATE_DATA_NODE, UNBLOCK_QUEUEING)
from session.exceptions import PeerTimeOutException
from session.sessions import SimpleSession, FileSession, EncryptedSession


class PeerRecvThread(Thread):
    def __init__(self, session: EncryptedSession, controller, *args, **kwargs):
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
        elif message == SEND_DB:
            self.receive_db()
        elif message.split(MESSAGE_SEPARATOR)[0] == UPDATE_DATA_NODE.split(MESSAGE_SEPARATOR)[0]:
            self.update_data_node(message)
        elif message == STOP_FRIENDSHIP:
            self.session.close()
            self.continues = False

    def receive_db(self):
        file_session = FileSession()
        file_session.receive_file(MetaDatabase.DATABASE_PATH, self.session)
        self.controller.peer_transmitter.transmit(UNBLOCK_QUEUEING)
        self.controller.release_queue_lock()

    def update_data_node(self, message):
        meta_data = dict(parse.parse(INTRODUCE_PEER, message).named)
        data_node = DataNode.fetch_by_ip(meta_data.get("ip_address"), self.db)
        if data_node is not None:
            data_node.rack_number = meta_data.get("rack_number")
            data_node.available_byte_size = meta_data.get("available_byte_size")
            data_node.save()
        else:
            self.controller.inform_next_node(message)
            data_node = DataNode(db=self.db, ip_address=meta_data["ip_address"],
                                 rack_number=meta_data.get("rack_number"),
                                 available_byte_size=meta_data.get("available_byte_size"), last_seen=monotonic())
            data_node.save()

    def add_peer(self, message):
        ip_address = dict(parse.parse(INTRODUCE_PEER, message).named)["ip_address"]
        try:
            new_session = SimpleSession(ip_address=ip_address, port_number=self.controller.PORT_NUMBER)
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
                             rack_number=meta_data["rack_number"], last_seen=monotonic())
        data_node.save()

        if len(self.controller.peers) == 1:
            thread = PeerRecvThread(session=new_session, controller=self.controller)
            thread.start()
            self.controller.peers.append(thread)
        else:
            self.session.transfer_data(STOP_FRIENDSHIP)
            self.session = new_session
            self.controller.inform_next_node(
                UPDATE_DATA_NODE.encode(ip_address=data_node.ip_address, rack_number=data_node.rack_number,
                                        available_byte_size=data_node.available_byte_size))

        print("Thread ", self.controller.peers)

    def perform_recovery_actions(self):
        self.session.close()
        self.db.close()
        self.continues = False
        self.controller.peers.remove(self)
