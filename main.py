from threading import Thread

from meta_data.database import MetaDatabase
from meta_data.models import DataNode
from servers.broadcast_server import BroadcastServer
from servers.data_node_server import DataNodeServer
from storage.storage import Storage


def data_node_server(ip_address, storage):
    server = DataNodeServer(ip_address=ip_address, storage=storage)
    server.run()


if __name__ == "__main__":
    MetaDatabase.initialize_tables()
    # storage = Storage("/Users/sepehrjavid/Desktop/", DataNode.fetch_by_id(1))
    # data_node_server_thread = Thread(target=data_node_server, args=["192.168.1.13", storage])
    # server = BroadcastServer(storage=storage, ip_address="192.168.1.255")
    # data_node_server_thread.start()
    # server.run()
    # data_node_server_thread.join()
