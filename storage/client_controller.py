from multiprocessing import Process

from singleton.singleton import Singleton


class ClientController(Process, metaclass=Singleton):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
