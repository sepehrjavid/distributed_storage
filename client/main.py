from client.actions import ClientActions


class Main:
    LOGIN_COMMAND = "1"
    CREATE_ACCOUNT_COMMAND = "2"
    LOGIN_EXIT_COMMAND = "3"

    SEND_FILE = "1"
    DELETE_FILE = "2"
    RETRIEVE_FILE = "3"
    NEW_DIR = "4"
    GRANT_DIR_PERM = "5"
    GRANT_FILE_PERM = "6"
    EXIT_COMMAND = "7"

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
            command = input("""Choose action:
            1.send file
            2.delete file
            3.Retrieve file
            4.New directory
            5.Grant directory permission
            6.Grant file permission
            7.Exit\n""")

            if command == self.SEND_FILE:
                self.client_actions.send_file()
            elif command == self.RETRIEVE_FILE:
                self.client_actions.retrieve_file()
            elif command == self.DELETE_FILE:
                self.client_actions.remove_file()
            elif command == self.NEW_DIR:
                self.client_actions.create_new_dir()
            elif command == self.GRANT_DIR_PERM:
                self.client_actions.grant_directory_permission()
            elif command == self.GRANT_FILE_PERM:
                self.client_actions.grant_file_permission()
            elif command == self.EXIT_COMMAND:
                break


if __name__ == "__main__":
    program = Main("192.168.1.11")
    program.run()
