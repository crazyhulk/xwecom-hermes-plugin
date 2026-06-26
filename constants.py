"""xwecom constants — aligned with official WeCom OpenClaw plugin."""

# WebSocket protocol commands
APP_CMD_SUBSCRIBE = "aibot_subscribe"
APP_CMD_CALLBACK = "aibot_msg_callback"
APP_CMD_LEGACY_CALLBACK = "aibot_callback"
APP_CMD_EVENT_CALLBACK = "aibot_event_callback"
APP_CMD_SEND = "aibot_send_msg"
APP_CMD_RESPONSE = "aibot_respond_msg"
APP_CMD_PING = "ping"
APP_CMD_UPLOAD_MEDIA_INIT = "aibot_upload_media_init"
APP_CMD_UPLOAD_MEDIA_CHUNK = "aibot_upload_media_chunk"
APP_CMD_UPLOAD_MEDIA_FINISH = "aibot_upload_media_finish"

# Stream constants
STREAM_EXPIRED_ERRCODE = 846608  # >6 min without update — stream is dead
STREAM_NOT_SUBSCRIBED_ERRCODE = 846609  # ws lost subscription
MAX_STREAM_CONTENT_LENGTH = 20480  # WeCom server-enforced byte limit per frame
MAX_INTERMEDIATE_FRAMES = 85  # Cap at 85 (100 queue limit - room for finalize)

# Block streaming parameters (aligned with official wecom-openclaw-plugin)
BLOCK_STREAM_MIN_CHARS = 120  # Don't emit a frame below this size
BLOCK_STREAM_MAX_CHARS = 360  # Force a break above this size
BLOCK_STREAM_IDLE_FLUSH = 0.25  # 250ms — flush partial buffer if no new tokens

# Sentence terminators for block chunking
SENTENCE_TERMINATORS = ".!?。！？"

# Media size limits (bytes)
IMAGE_MAX_BYTES = 10 * 1024 * 1024
VIDEO_MAX_BYTES = 10 * 1024 * 1024
VOICE_MAX_BYTES = 2 * 1024 * 1024
FILE_MAX_BYTES = 20 * 1024 * 1024
ABSOLUTE_MAX_BYTES = FILE_MAX_BYTES
UPLOAD_CHUNK_SIZE = 512 * 1024
MAX_UPLOAD_CHUNKS = 100
VOICE_SUPPORTED_MIMES = {"audio/amr"}

# Connection parameters
MAX_MESSAGE_LENGTH = 4000
CONNECT_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 15.0
HEARTBEAT_INTERVAL_SECONDS = 30.0
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]

# Dedup
DEDUP_MAX_SIZE = 1000
DEDUP_TTL_SECONDS = 300
