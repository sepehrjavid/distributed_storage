import socket

if __name__ == "__main__":
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect(("192.168.1.11", 54455))
