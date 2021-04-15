from client.actions import ClientActions


class Main:
    SEND_FILE_COMMAND = "1"
    DELETE_FILE_COMMAND = "2"
    EXIT_COMMAND = "3"

    def __init__(self, username):
        self.username = username
        self.client_actions = ClientActions(username)

    def run(self):
        while True:
            command = input("Choose action:\n1.send file\n2.delete file\n3.Exit")
            if command == self.SEND_FILE_COMMAND:
                pass
            elif command == self.DELETE_FILE_COMMAND:
                pass
            elif command == self.EXIT_COMMAND:
                break


if __name__ == "__main__":
    program = Main("Alexandra")
    # program.run()
