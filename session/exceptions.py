class PeerTimeOutException(Exception):
    MESSAGE = "The peer refused to connect"

    def __init__(self):
        super().__init__(PeerTimeOutException.MESSAGE)


class InvalidSessionType(Exception):
    def __init__(self, message):
        self.message = message
