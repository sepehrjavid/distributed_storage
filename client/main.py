from client.actions import ClientActions


class Main:
    LOGIN_COMMAND = "1"
    CREATE_ACCOUNT_COMMAND = "2"
    LOGIN_EXIT_COMMAND = "3"

    SEND_FILE = "1"
    DELETE_FILE = "2"
    RETRIEVE_FILE = "3"
    NEW_DIR = "4"
    EXIT_COMMAND = "5"

    def __init__(self, ip_address):
        self.ip_address = ip_address
        self.client_actions = ClientActions(ip_address)

    def run(self):
        while True:
            command = input("Choose action:\n1.Login\n2.Create account\n3.Exit\n")

            if command == self.LOGIN_EXIT_COMMAND:
                return
            elif command == self.CREATE_ACCOUNT_COMMAND:
                if self.client_actions.create_account():
                    break
                print("Username Already Exists!!")
            elif command == self.LOGIN_COMMAND:
                if self.client_actions.authenticate():
                    break
                print("Authentication Failed!!")

        while True:
            command = input("Choose action:\n1.send file\n2.delete file\n3.Retrieve file\n4.Exit\n")
            if command == self.SEND_FILE:
                self.client_actions.send_file()
            elif command == self.RETRIEVE_FILE:
                self.client_actions.retrieve_file()
            elif command == self.DELETE_FILE:
                pass
            elif command == self.NEW_DIR:
                pass
            elif command == self.EXIT_COMMAND:
                break


if __name__ == "__main__":
    program = Main("192.168.1.13")
    program.run()
