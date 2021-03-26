import socket
from threading import Thread
from time import time, sleep, gmtime, strftime

from meta_data.models import DataNode, ChunkMetadata
from session.sessions import FileSession


def client():
    session = FileSession("192.168.1.14", 54455)
    session.transfer_file("/Users/sepehrjavid/Desktop/q.mkv")
    # print(int.from_bytes(sep, byteorder=sys.byteorder, signed=False))
    session.end_session()


if __name__ == "__main__":
    res = DataNode.fetch_by_id(1)
    chunk = ChunkMetadata(data_node=res, local_path="sep", chunk_size=64000, sequence=0)
    chunk.save()
    print(chunk.id)

    # last = res[-1]

    # thread = Thread(target=client, args=[])
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind(("192.168.1.14", 54455))
    # server.listen(5)
    # thread.start()
    # client_socket, addr = server.accept()
    # start = time()
    # session = FileSession(input_socket=client_socket, is_server=True)
    # session.receive_file("/Users/sepehrjavid/Desktop/sep.mkv")
    # thread.join()
    # end = time()
    # print(end - start)
