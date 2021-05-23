"""
Client-Peer Messages
"""

MESSAGE_SEPARATOR = "-"

CREATE_FILE = "crf-{total_size}-{path}-{username}-{title}-{extension}"
DELETE_FILE = "rmf-{title}-{permission}"
CREATE_CHUNK = "crc-{sequence}-{chunk_size}-{username}-{path}-{title}-{extension}"
GET_FILE = "getf-{path}-{username}"
GET_CHUNK = "getc-{path}-{username}-{sequence}"

OUT_OF_SPACE = "no_space"
INVALID_METADATA = "meta_err"
DUPLICATE_FILE_FOR_USER = "dup_fil"
DUPLICATE_CHUNK_FOR_FILE = "dup-chnk"
NO_PERMISSION = "noperm"
INVALID_PATH = "invld_path"
FILE_DOES_NOT_EXIST = "ntexstfile"
CORRUPTED_FILE = "crpted"

LOGIN = "lgin"
CREDENTIALS = "cred-{username}-{password}"
USER_NOT_FOUND = "usr_not_fnd"
AUTH_FAILED = "auth_fld"
CREATE_ACCOUNT = "create_acc"
DUPLICATE_ACCOUNT = "dup_acc"

"""
Peer-Peer Messages
"""

JOIN_NETWORK = "join"
INTRODUCE_PEER = "intro-{ip_address}"
CONFIRM_HANDSHAKE = "confirm-{available_byte_size}-{rack_number}"
STOP_FRIENDSHIP = "stop_fr"
RESPOND_TO_BROADCAST = "broadcast"
RESPOND_TO_INTRODUCTION = "introduction"
SEND_DB = "snddb"
UNBLOCK_QUEUEING = "unblck"
BLOCK_QUEUEING = "blck"
ABORT_JOIN = "abrt"
NEW_FILE = "nwfl-{title}-{extension}-{username}-{path}-{sequence_num}"
NEW_USER = "nwusr-{username}-{password}"
UPDATE_AVAILABLE_SIZE = "size-{new_size}-{ip_address}"
NEW_CHUNK = "nwchnk-{ip_address}-{sequence}-{chunk_size}-{path}-{title}-{extension}-{username}-{destination_file_path}"

"""
General Messages
"""
ACCEPT = "ok"
NULL = "null"
REJECT = "reject"

"""
Metadata Events
"""

UPDATE_DATA_NODE = "nwnd-{ip_address}-{available_byte_size}-{rack_number}"

"""
Inter-Process Communication
"""

START_CLIENT_SERVER = "start"
