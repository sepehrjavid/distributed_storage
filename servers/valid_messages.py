"""
Client Valid Messages
"""

MESSAGE_SEPARATOR = "-"
CREATE_FILE = "crf-{total_size}"
DELETE_FILE = "rmf-{title}-{permission}"
CREATE_CHUNK = "crc-{title}-{sequence}-{chunk_size}-{permission}"
DELETE_CHUNK = "rmc"
REPLICATE = "rpt-{title}-{sequence}-{chunk_size}-{permission}"
OUT_OF_SPACE = "no_space"
INVALID_METADATA = "meta_err"
DUPLICATE_FILE_FOR_USER = "dup"

"""
Peer Valid Messages
"""

JOIN_NETWORK = "join"
INTRODUCE_PEER = "intro-{ip_address}"
CONFIRM_HANDSHAKE = "confirm-{available_byte_size}-{rack_number}"
STOP_FRIENDSHIP = "stop-fr"
RESPOND_TO_BROADCAST = "broadcast"
RESPOND_TO_INTRODUCTION = "introduction"
SEND_DB = "snddb"
UNBLOCK_QUEUEING = "unblck"
BLOCK_QUEUEING = "blck"
ABORT_JOIN = "abrt"
# TODO clean messages


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
