import socket
from threading import Thread, Lock, Event
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
                            NEW_USER, NEW_FILE, NEW_CHUNK, NEW_DIR, REMOVE_FILE, NEW_FILE_PERMISSION,
                            NEW_DIR_PERMISSION, DELETE_CHUNK, REMOVE_DATA_NODE, PEER_FAILURE, NAME_NODE_DOWN)
from session.exceptions import PeerTimeOutException
from session.sessions import SimpleSession, FileSession, EncryptedSession


class PeerRecvThread(Thread):
    DATABASE_LOCK = Lock()
    RECOVERY_DATA_NODE_CONNECTION_TIMEOUT = 8

    def __init__(self, session: EncryptedSession, controller, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session
        self.controller = controller
        self.continues = True
        self.db = None
        self.controller_inbox = None
        self.thread_inbox = None
        self.failure_help_found = Event()
        self.failed = False

    def run(self):
        self.db = MetaDatabase()
        while self.continues:
            try:
                message = self.session.receive_data()
            except ConnectionResetError:
                message = None

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
        elif command == NEW_DIR.split(MESSAGE_SEPARATOR)[0]:
            self.create_dir(message)
        elif command == REMOVE_FILE.split(MESSAGE_SEPARATOR)[0]:
            self.delete_file(message)
        elif command == NEW_DIR_PERMISSION.split(MESSAGE_SEPARATOR)[0]:
            self.add_directory_permission(message)
        elif command == NEW_FILE_PERMISSION.split(MESSAGE_SEPARATOR)[0]:
            self.add_file_permission(message)
        elif command == REMOVE_DATA_NODE.split(MESSAGE_SEPARATOR)[0]:
            self.remove_data_node(message)
        elif message == STOP_FRIENDSHIP:
            self.session.close()
            self.continues = False

    def remove_data_node(self, message):
        meta_data = dict(parse.parse(REMOVE_DATA_NODE, message).named)
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(REMOVE_DATA_NODE.format(
                ip_address=meta_data.get("ip_address"),
                signature=f"{signature}-{self.controller.ip_address}"

            ), previous_signature=signature)

            with self.DATABASE_LOCK:
                data_node = DataNode.fetch_by_ip(ip_address=meta_data.get("ip_address"), db=self.db)
                if data_node is not None:
                    data_node.delete()

    def add_file_permission(self, message):
        meta_data = dict(parse.parse(NEW_FILE_PERMISSION, message).named)
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(NEW_FILE_PERMISSION.format(
                username=meta_data.get("username"),
                path=meta_data.get("path"),
                perm=meta_data.get("perm"),
                signature=f"{signature}-{self.controller.ip_address}"
            ), previous_signature=signature)

            logical_path = meta_data.get("path")
            lst = logical_path.split("/")
            path_owner = lst[0]
            dir_path = "/".join(lst[1:-1])

            if "." in lst[-1]:
                file_name = lst[-1].split(".")[0]
                extension = lst[-1].split(".")[1]
            else:
                file_name = lst[-1].split(".")[0]
                extension = None

            directory = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=dir_path)

            file = File.fetch_by_dir_title_extension(dir_id=directory.id, title=file_name, extension=extension,
                                                     db=self.db)

            with self.DATABASE_LOCK:
                permission = Permission.fetch_by_username_file_id(username=meta_data.get("username"),
                                                                  file_id=file.id,
                                                                  db=self.db)
                if permission is None:
                    user = User.fetch_by_username(username=meta_data.get("username"), db=self.db)
                    Permission(db=self.db, file_id=file.id, user_id=user.id, perm=meta_data.get("perm")).save()
                else:
                    permission.perm = meta_data.get("perm")
                    permission.save()

    def add_directory_permission(self, message):
        meta_data = dict(parse.parse(NEW_DIR_PERMISSION, message).named)
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(NEW_DIR_PERMISSION.format(
                username=meta_data.get("username"),
                path=meta_data.get("path"),
                perm=meta_data.get("perm"),
                signature=f"{signature}-{self.controller.ip_address}"
            ), previous_signature=signature)

            lst = meta_data.get("path").split("/")
            path_owner = lst[0]
            dir_path = "/".join(lst[1:])

            directory = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=dir_path)

            with self.DATABASE_LOCK:
                permission = Permission.fetch_by_username_directory_id(username=meta_data.get("username"),
                                                                       directory_id=directory.id,
                                                                       db=self.db)
                if permission is None:
                    user = User.fetch_by_username(username=meta_data.get("username"), db=self.db)
                    Permission(db=self.db, directory_id=directory.id, user_id=user.id,
                               perm=meta_data.get("perm")).save()
                else:
                    permission.perm = meta_data.get("perm")
                    permission.save()

    def delete_file(self, message):
        meta_data = dict(parse.parse(REMOVE_FILE, message).named)
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(REMOVE_FILE.format(
                path=meta_data.get("path"),
                signature=f"{signature}-{self.controller.ip_address}"
            ), previous_signature=signature)

            logical_path = meta_data.get("path")
            lst = logical_path.split("/")
            path_owner = lst[0]
            dir_path = "/".join(lst[1:-1])

            if "." in lst[-1]:
                file_name = lst[-1].split(".")[0]
                extension = lst[-1].split(".")[1]
            else:
                file_name = lst[-1].split(".")[0]
                extension = None

            directory = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=dir_path)

            with self.DATABASE_LOCK:
                possible_file = File.fetch_by_dir_title_extension(dir_id=directory.id, extension=extension,
                                                                  title=file_name, db=self.db)
                if possible_file is not None:
                    for chunk in possible_file.chunks:
                        self.controller.client_controller_pipe.send(DELETE_CHUNK.format(path=chunk.local_path))
                    possible_file.delete()

    def create_dir(self, message):
        meta_data = dict(parse.parse(NEW_DIR, message).named)
        signature = meta_data.get("signature")

        if self.controller.ip_address not in signature.split('-'):
            username = meta_data.get("username")
            self.controller.inform_next_node(NEW_DIR.format(
                path=meta_data.get("path"),
                username=username,
                signature=f"{signature}-{self.controller.ip_address}"

            ), previous_signature=signature)

            lst = meta_data.get("path").split("/")
            path_owner = lst[0]
            new_dir_name = lst[-1]
            dir_path = "/".join(lst[1:-1])

            directory = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=dir_path)

            with self.DATABASE_LOCK:
                if new_dir_name not in [x.title for x in directory.children]:
                    new_dir = Directory(db=self.db, title=new_dir_name, parent_directory_id=directory.id)
                    new_dir.save()
                    Permission(db=self.db, perm=Permission.OWNER, directory_id=new_dir.id,
                               user_id=User.fetch_by_username(username=username, db=self.db).id).save()

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
            ), previous_signature=signature)

            requested_dir = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=path)
            file = File.fetch_by_dir_title_extension(dir_id=requested_dir.id, title=meta_data.get("title"),
                                                     extension=meta_data.get("extension"), db=self.db)
            data_node = DataNode.fetch_by_ip(meta_data.get("ip_address"), db=self.db)

            with self.DATABASE_LOCK:
                possible_chunk = Chunk.fetch_by_file_id_data_node_id_sequence(file_id=file.id,
                                                                              data_node_id=data_node.id,
                                                                              sequence=meta_data.get("sequence"),
                                                                              db=self.db)
                if possible_chunk is None:
                    Chunk(db=self.db, sequence=meta_data.get("sequence"),
                          local_path=meta_data.get("destination_file_path"),
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
            ), previous_signature=signature)

            with self.DATABASE_LOCK:
                possible_user = User.fetch_by_username(username=username, db=self.db)
                if possible_user is None:
                    user = User(db=self.db, username=username, password=password)
                    user.save()

            if possible_user is None:
                main_directory = Directory(db=self.db, title=Directory.MAIN_DIR_NAME, parent_directory_id=None)
                main_directory.save()
                permission = Permission(db=self.db, directory_id=main_directory.id, user_id=user.id,
                                        perm=Permission.OWNER)
                permission.save()

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
            ), previous_signature=signature)

            user = User.fetch_by_username(username=meta_data.get("username"), db=self.db)
            requested_dir = Directory.find_path_directory(
                main_dir=Directory.fetch_user_main_directory(username=path_owner, db=self.db), path=path)

            with self.DATABASE_LOCK:
                file = File.fetch_by_dir_title_extension(dir_id=requested_dir.id, extension=meta_data.get("extension"),
                                                         title=meta_data.get("title"), db=self.db)

                if file is None:
                    file = File(db=self.db, title=meta_data.get("title"), extension=meta_data.get("extension"),
                                sequence_num=meta_data.get("sequence_num"), directory_id=requested_dir.id,
                                is_complete=False)
                    file.save()
                    Permission(db=self.db, perm=Permission.OWNER, file_id=file.id, user_id=user.id).save()

    def receive_db(self):
        file_session = FileSession()
        file_session.receive_file(MetaDatabase.DATABASE_PATH, self.session)
        self.controller.client_controller_pipe.send(START_CLIENT_SERVER)
        self.controller.inform_next_node(UPDATE_DATA_NODE.format(
            ip_address=self.controller.ip_address,
            rack_number=self.controller.rack_number,
            priority=self.controller.priority,
            available_byte_size=self.controller.available_byte_size,
            signature=self.controller.ip_address
        ))
        self.controller.peer_transmitter.transmit(UNBLOCK_QUEUEING)
        self.controller.release_queue_lock()
        self.controller.update_name_node_ip_address(db=self.db)

    def update_data_node(self, message):
        meta_data = dict(parse.parse(UPDATE_DATA_NODE, message).named)
        signature = meta_data.get("signature")
        if self.controller.ip_address not in signature.split('-'):
            self.controller.inform_next_node(UPDATE_DATA_NODE.format(
                ip_address=meta_data.get("ip_address"),
                available_byte_size=meta_data.get("available_byte_size"),
                rack_number=meta_data.get("rack_number"),
                priority=meta_data.get("priority"),
                signature=f"{signature}-{self.controller.ip_address}"
            ), previous_signature=signature)

            with self.DATABASE_LOCK:
                data_node = DataNode.fetch_by_ip(meta_data.get("ip_address"), self.db)
                if data_node is None:
                    DataNode(db=self.db, ip_address=meta_data.get("ip_address"),
                             rack_number=meta_data.get("rack_number"),
                             available_byte_size=meta_data.get("available_byte_size"),
                             last_seen=monotonic(), priority=meta_data.get("priority")).save()
                    self.controller.update_name_node_ip_address(db=self.db)
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
        with self.DATABASE_LOCK:
            data_node = DataNode.fetch_by_ip(ip_address=ip_address, db=self.db)
            if data_node is None:
                data_node = DataNode(db=self.db, ip_address=ip_address,
                                     available_byte_size=meta_data.get("available_byte_size"),
                                     rack_number=meta_data.get("rack_number"), priority=meta_data.get("priority"),
                                     last_seen=monotonic())
            else:
                data_node.available_byte_size = meta_data.get("available_byte_size")
                data_node.rack_number = meta_data.get("rack_number")
                data_node.priority = meta_data.get("priority")
                data_node.last_seen = monotonic()
            data_node.save()
        self.controller.update_name_node_ip_address(db=self.db)

        if len(self.controller.peers) == 1:
            thread = PeerRecvThread(session=new_session, controller=self.controller)
            thread.start()
            self.controller.peers.append(thread)
        else:
            self.session.transfer_data(STOP_FRIENDSHIP)
            self.session = new_session

        print("Thread ", [x.session.ip_address for x in self.controller.peers])

    def perform_recovery_actions(self):
        self.failed = True
        ip_address = self.session.ip_address
        self.controller_inbox = ip_address
        self.session.close()
        data_node = DataNode.fetch_by_ip(ip_address=ip_address, db=self.db)
        if data_node is not None:
            data_node.delete()

        if self.controller.name_node_ip_address == ip_address:
            print("Advertise Namenode down")
            self.controller.peer_transmitter.transmit(NAME_NODE_DOWN.format(name_node_address=ip_address))
            self.controller.update_name_node_ip_address(db=self.db)

        if len(self.controller.peers) == 1:
            self.continues = False
            self.db.close()
            self.controller.peers.remove(self)
        else:
            data_node_count = len(DataNode.fetch_all(db=self.db))
            if data_node_count > 2:
                self.controller.peers[1 - self.controller.peers.index(self)].session.transfer_data(
                    REMOVE_DATA_NODE.format(ip_address=ip_address,
                                            signature=self.controller.ip_address))

                self.controller.peer_transmitter.transmit(PEER_FAILURE.format(
                    failed_address=ip_address,
                    reporter_address=self.controller.ip_address
                ))

                self.failure_help_found.wait()
                new_ip_address = self.thread_inbox
                print(f"New peer address {new_ip_address}")

                if new_ip_address > self.controller.ip_address:
                    try_number = 0

                    while True:
                        try:
                            self.session = EncryptedSession(ip_address=new_ip_address,
                                                            port_number=self.controller.PORT_NUMBER)
                            self.failed = False
                            break
                        except PeerTimeOutException:
                            try_number += 1
                            if try_number == 3:
                                self.continues = False
                                self.db.close()
                                self.controller.peers.remove(self)
                                break

                else:
                    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_socket.bind((self.controller.ip_address, self.controller.PORT_NUMBER))
                    server_socket.listen(2)
                    server_socket.settimeout(self.RECOVERY_DATA_NODE_CONNECTION_TIMEOUT)

                    try:
                        client_socket, addr = server_socket.accept()
                        while addr[0] != new_ip_address:
                            client_socket, addr = server_socket.accept()
                        self.session = EncryptedSession(input_socket=client_socket, is_server=True, ip_address=addr[0])
                        server_socket.close()
                        self.failed = False
                    except socket.timeout:
                        self.continues = False
                        self.db.close()
                        self.controller.peers.remove(self)
            else:
                self.continues = False
                self.db.close()
                self.controller.peers.remove(self)

        self.controller_inbox = None
        self.thread_inbox = None
        self.failure_help_found.clear()
        print([x.session.ip_address for x in self.controller.peers])
