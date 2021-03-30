from broadcast.servers import SimpleBroadcastServer
from servers.storage_server import StorageServer
from session.sessions import SimpleSession
from singleton.singleton import Singleton


class ClientAcceptanceServer(SimpleBroadcastServer, metaclass=Singleton):
    CLIENT_CONTROL_PORT_NUMBER = 54224
    CLIENT_BROADCAST_CONTROL_PORT_NUMBER = 54223

    def __init__(self, ip_address, on_receive):
        super().__init__(ip_address, self.CLIENT_BROADCAST_CONTROL_PORT_NUMBER, self.__on_receive)

    @staticmethod
    def __on_receive(source_address, data):
        storage_server = StorageServer()
        if len(storage_server.active_clients) >= StorageServer.MAXIMUM_CLIENT_ALLOWED:
            return

        client_socket = SimpleSession(ip_address=source_address,
                                      port_number=ClientAcceptanceServer.CLIENT_CONTROL_PORT_NUMBER)
        SimpleSession.transfer_data("accept")
