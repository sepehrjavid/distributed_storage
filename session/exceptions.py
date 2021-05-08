class PeerTimeOutException(Exception):
    MESSAGE = "The peer refused to connect"

    def __init__(self):
        super().__init__(PeerTimeOutException.MESSAGE)
