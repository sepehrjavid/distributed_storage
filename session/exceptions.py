class PeerTimeOutException(Exception):
    message = "The peer refused to connect"


class InvalidSessionType(Exception):
    def __init__(self, message):
        self.message = message
