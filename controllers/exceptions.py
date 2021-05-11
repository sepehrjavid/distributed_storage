class InvalidDataNodeConfigFile(Exception):
    def __init__(self, field):
        super().__init__(f"Invalid Config File! {field} is missing")
