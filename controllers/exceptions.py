class InvalidDataNodeConfigFile(Exception):
    def __init__(self, field):
        super().__init__(f"Invalid Config File! {field} is missing")


class InvalidValueForConfigFiled(Exception):
    def __init__(self, field):
        super().__init__(f"Invalid Config File! Invalid value for {field}")
