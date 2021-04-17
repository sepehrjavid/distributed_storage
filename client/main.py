from client.actions import ClientActions


class Main:
    SEND_FILE_COMMAND = "1"
    DELETE_FILE_COMMAND = "2"
    EXIT_COMMAND = "3"

    def __init__(self, username, ip_address):
        self.username = username
        self.ip_address = ip_address
        self.client_actions = ClientActions(username, ip_address)

    def run(self):
        while True:
            command = input("Choose action:\n1.send file\n2.delete file\n3.Exit\n")
            if command == self.SEND_FILE_COMMAND:
                file_path = input("Enter the desired filepath:\n")
                self.client_actions.send_file(file_path)
            elif command == self.DELETE_FILE_COMMAND:
                pass
            elif command == self.EXIT_COMMAND:
                break


if __name__ == "__main__":
    program = Main("Alexandra", "192.168.1.13")
    program.run()
