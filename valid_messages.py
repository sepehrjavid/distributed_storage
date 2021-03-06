"""
Client-Peer Messages
"""

MESSAGE_SEPARATOR = "-"

CREATE_FILE = "crf-{total_size}-{path}-{username}-{title}-{extension}"
DELETE_FILE = "rmf-{username}-{path}"
ADD_FILE_PERM = "adfperm-{path}-{owner_username}-{perm_username}-{perm}"
REMOVE_FILE_PERM = "rmfperm-{path}-{owner_username}-{perm_username}"
ADD_DIR_PERM = "addirperm-{path}-{owner_username}-{perm_username}-{perm}"
REMOVE_DIR_PERM = "rmdirperm-{path}-{owner_username}-{perm_username}"
CREATE_CHUNK = "crc-{sequence}-{chunk_size}-{username}-{path}-{title}-{extension}"
CREATE_DIR = "cd-{path}-{username}"
GET_FILE = "getf-{path}-{username}"
GET_CHUNK = "getc-{path}-{username}-{sequence}"

OUT_OF_SPACE = "no_space"
INVALID_METADATA = "meta_err"
DUPLICATE_FILE_FOR_USER = "dup_fil"
DUPLICATE_CHUNK_FOR_FILE = "dup_chnk"
DUPLICATE_DIR_NAME = "dup_dir"
NO_PERMISSION = "noperm"
INVALID_PATH = "invld_path"
FILE_DOES_NOT_EXIST = "ntexstfile"
CORRUPTED_FILE = "crpted"
CHUNK_NOT_FOUND = "chnk_notfnd"
INVALID_USERNAME = "invusr"
INVALID_PERMISSION_VALUE = "invpermval"

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
CONFIRM_HANDSHAKE = "confirm-{available_byte_size}-{rack_number}-{priority}"
STOP_FRIENDSHIP = "stop_fr"
RESPOND_TO_BROADCAST = "broadcast"
RESPOND_TO_INTRODUCTION = "introduction"
SEND_DB = "snddb"
UNBLOCK_QUEUEING = "unblck"
BLOCK_QUEUEING = "blck"
ABORT_JOIN = "abrt"
PEER_FAILURE = "failpeer-{failed_address}-{reporter_address}"
RESPOND_PEER_FAILURE = "rspndfailpeer-{failed_address}-{self_ip_address}-{reporter_address}"
REPLICATE_CHUNK = "rep-{create_chunk_message}"
NAME_NODE_DOWN = "namenodedown-{name_node_address}"

"""
General Messages
"""
ACCEPT = "ok"
NULL = "null"
REJECT = "reject"

"""
Metadata Events
"""

UPDATE_DATA_NODE = "nwnd-{ip_address}-{available_byte_size}-{rack_number}-{priority}-{signature}"
NEW_FILE = "nwfl-{title}-{extension}-{username}-{path}-{sequence_num}-{signature}"
NEW_USER = "nwusr-{username}-{password}-{signature}"
NEW_CHUNK = "nwchnk-{ip_address}-{sequence}-{chunk_size}-{path}-{title}-{extension}-{destination_file_path}-{signature}"
NEW_DIR = "nwdir-{path}-{username}-{signature}"
REMOVE_FILE = "remfile-{path}-{signature}"
NEW_FILE_PERMISSION = "nwfperm-{path}-{username}-{perm}-{signature}"
NEW_DIR_PERMISSION = "nwdperm-{path}-{username}-{perm}-{signature}"
REMOVE_DATA_NODE = "rmnd-{ip_address}-{signature}"

"""
Inter-Process Communication
"""

START_CLIENT_SERVER = "start"
DELETE_CHUNK = "delc-{path}"
NAME_NODE_STATUS = "name_node-{status}"
