import hashlib
import pickle
import rsa

from cryptography.fernet import Fernet

from encryption.exceptions import KeyHashMismatchException


class RSAEncryption:
    def __init__(self, fernet):
        self.fernet = fernet

    @staticmethod
    def create_server_encryption(socket):
        public_key_pk, pub_key_sha256 = pickle.loads(socket.recv(1024))
        if hashlib.sha256(public_key_pk).hexdigest() != pub_key_sha256:
            raise KeyHashMismatchException()
        else:
            public_key = pickle.loads(public_key_pk)

        sym_key = Fernet.generate_key()
        encrypted_sym_key = rsa.encrypt(pickle.dumps(sym_key), public_key)
        encrypted_sym_key_sha256 = hashlib.sha256(encrypted_sym_key).hexdigest()
        socket.send(pickle.dumps((encrypted_sym_key, encrypted_sym_key_sha256)))

        fernet = Fernet(sym_key)
        return RSAEncryption(fernet)

    @staticmethod
    def create_client_encryption(socket):
        asymmetric_key = rsa.newkeys(2048)
        public_key = asymmetric_key[0]
        private_key = asymmetric_key[1]

        send_key = pickle.dumps(public_key)
        send_key_sha256 = hashlib.sha256(send_key).hexdigest()
        socket.send(pickle.dumps((send_key, send_key_sha256)))

        sym_key, sym_key_sha256 = pickle.loads(socket.recv(1024))
        if hashlib.sha256(sym_key).hexdigest() != sym_key_sha256:
            raise KeyHashMismatchException()
        else:
            asymmetric_key = pickle.loads(rsa.decrypt(sym_key, private_key))

        fernet = Fernet(sym_key)
        return RSAEncryption(fernet)

    def decrypt(self, encrypted_data):
        return self.fernet.decrypt(encrypted_data).decode()

    def encrypt(self, raw_data):
        return self.fernet.encrypt(raw_data.encode())
