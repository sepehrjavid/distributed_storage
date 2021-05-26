from threading import Thread
from time import monotonic

import parse

from meta_data.database import MetaDatabase
from meta_data.models.chunk import Chunk
from meta_data.models.data_node import DataNode
from meta_data.models.directory import Directory
from meta_data.models.file import File
from meta_data.models.permission import Permission
from meta_data.models.user import User
from valid_messages import (CONFIRM_HANDSHAKE, STOP_FRIENDSHIP, RESPOND_TO_INTRODUCTION, ACCEPT, INTRODUCE_PEER,
                            MESSAGE_SEPARATOR, SEND_DB, UPDATE_DATA_NODE, UNBLOCK_QUEUEING, START_CLIENT_SERVER,
                            NEW_USER, NEW_FILE, NEW_CHUNK)
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
        print(message)
        command = message.split(MESSAGE_SEPARATOR)[0]
        if command == INTRODUCE_PEER.split(MESSAGE_SEPARATOR)[0]:
            self.add_peer(message)
        elif message == SEND_DB:
            self.receive_db()
        elif command == UPDATE_DATA_NODE.split(MESSAGE_SEPARATOR)[0]:
            self.update_data_node(message)
        elif command == NEW_USER.split(MESSAGE_SEPARATOR)[0]:
            self.create_account(message)
        elif command == NEW_FILE.split(MESSAGE_SEPARATOR)[0]:
            self.create_file(message)
        elif command == NEW_CHUNK.split(MESSAGE_SEPARATOR)[0]:
            self.create_chunk(message)
        elif message == STOP_FRIENDSHIP:
            self.session.close()
            self.continues = False

    def create_chunk(self, message):
        meta_data = dict(parse.parse(NEW_CHUNK, message).named)
        path_owner = meta_data.get("path").split("/")[0]
        path = "/".join(meta_data.get("path").split("/")[1:])
        signature = meta_data.get("signature")
        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(NEW_CHUNK.format(
                ip_address=meta_data.get("ip_address"),
                sequence=meta_data.get("sequence"),
                chunk_size=meta_data.get("chunk_size"),
                path=meta_data.get("path"),
                title=meta_data.get("title"),
                extension=meta_data.get("extension"),
                destination_file_path=meta_data.get("destination_file_path"),
                signature=f"{signature}-{self.controller.ip_address}"
            ))

            requested_dir = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=path)
            file = File.fetch_by_dir_title_extension(dir_id=requested_dir.id, title=meta_data.get("title"),
                                                     extension=meta_data.get("extension"), db=self.db)
            data_node = DataNode.fetch_by_ip(meta_data.get("ip_address"), db=self.db)
            Chunk(db=self.db, sequence=meta_data.get("sequence"), local_path=meta_data.get("destination_file_path"),
                  chunk_size=meta_data.get("chunk_size"), data_node_id=data_node.id,
                  file_id=file.id).save()

    def create_account(self, message):
        meta_data = dict(parse.parse(NEW_USER, message).named)
        username = meta_data.get("username")
        password = meta_data.get("password")
        signature = meta_data.get("signature")
        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(NEW_USER.format(
                username=username,
                password=password,
                signature=f"{signature}-{self.controller.ip_address}"
            ))

            user = User.fetch_by_username(username=username, db=self.db)
            if user is None:
                user = User(db=self.db, username=username, password=password)
                user.save()
                main_directory = Directory(db=self.db, title=Directory.MAIN_DIR_NAME, parent_directory_id=None)
                main_directory.save()
                permission = Permission(db=self.db, directory_id=main_directory.id, user_id=user.id,
                                        perm=Permission.READ_WRITE)
                permission.save()
            else:
                user.password = password
                user.save()

    def create_file(self, message):
        meta_data = dict(parse.parse(NEW_FILE, message).named)
        path_owner = meta_data.get("path").split("/")[0]
        path = "/".join(meta_data.get("path").split("/")[1:])
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(NEW_FILE.format(
                title=meta_data.get("title"),
                extension=meta_data.get("extension"),
                username=meta_data.get("username"),
                path=meta_data.get("path"),
                sequence_num=meta_data.get("sequence_num"),
                signature=f"{signature}-{self.controller.ip_address}"
            ))

            user = User.fetch_by_username(username=meta_data.get("username"), db=self.db)
            requested_dir = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=path)
            file = File.fetch_by_dir_title_extension(dir_id=requested_dir.id, extension=meta_data.get("extension"),
                                                     title=meta_data.get("title"), db=self.db)

            if file is None:
                file = File(db=self.db, title=meta_data.get("title"), extension=meta_data.get("extension"),
                            sequence_num=meta_data.get("sequence_num"), directory_id=requested_dir.id,
                            is_complete=False)
                file.save()
                Permission(db=self.db, perm=Permission.READ_WRITE, file_id=file.id, user_id=user.id).save()
            else:
                file.title = meta_data.get("title")
                file.save()

    def receive_db(self):
        file_session = FileSession()
        file_session.receive_file(MetaDatabase.DATABASE_PATH, self.session)
        self.controller.client_controller_pipe.send(START_CLIENT_SERVER)
        self.controller.peer_transmitter.transmit(UNBLOCK_QUEUEING)
        self.controller.release_queue_lock()

    def update_data_node(self, message):
        meta_data = dict(parse.parse(UPDATE_DATA_NODE, message).named)
        signature = meta_data.get("signature")
        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(UPDATE_DATA_NODE.format(
                ip_address=meta_data.get("ip_address"),
                available_byte_size=meta_data.get("available_byte_size"),
                rack_number=meta_data.get("rack_number"),
                signature=f"{signature}-{self.controller.ip_address}"
            ))

            data_node = DataNode.fetch_by_ip(meta_data.get("ip_address"), self.db)
            if data_node is None:
                data_node = DataNode(db=self.db, ip_address=meta_data["ip_address"],
                                     rack_number=meta_data.get("rack_number"),
                                     available_byte_size=meta_data.get("available_byte_size"), last_seen=monotonic())
            else:
                data_node.available_byte_size = meta_data.get("available_byte_size")
                data_node.rack_number = meta_data.get("rack_number")
                data_node.last_seen = monotonic()

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
                UPDATE_DATA_NODE.format(ip_address=data_node.ip_address, rack_number=data_node.rack_number,
                                        available_byte_size=data_node.available_byte_size,
                                        signature=self.controller.ip_address))

        print("Thread ", self.controller.peers)

    def perform_recovery_actions(self):
        self.session.close()
        self.db.close()
        self.continues = False
        self.controller.peers.remove(self)
