from client.actions import ClientActions


class Main:
    SEND_FILE_COMMAND = "1"
    LOGIN_COMMAND = "1"
    CREATE_ACCOUNT_COMMAND = "2"
    DELETE_FILE_COMMAND = "2"
    EXIT_COMMAND = "3"

    def __init__(self, ip_address):
        self.ip_address = ip_address
        self.client_actions = ClientActions(ip_address)

    def run(self):
        while True:
            command = input("Choose action:\n1.Login\n2.Create account\n3.Exit\n")

            if command == self.EXIT_COMMAND:
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
            command = input("Choose action:\n1.send file\n2.delete file\n3.Exit\n")
            if command == self.SEND_FILE_COMMAND:
                file_path = input("Enter the desired filepath:\n")
                self.client_actions.send_file(file_path, "main")
            elif command == self.DELETE_FILE_COMMAND:
                pass
            elif command == self.EXIT_COMMAND:
                break


if __name__ == "__main__":
    program = Main("192.168.1.12")
    program.run()
