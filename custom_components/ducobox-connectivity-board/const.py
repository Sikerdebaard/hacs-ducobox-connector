from datetime import timedelta

DOMAIN = "ducobox_connectivity_board"
SCAN_INTERVAL = timedelta(seconds=60)

# Timeout constants
HTTP_TIMEOUT_BUFFER = timedelta(seconds=5)  # Buffer before next scan
EXECUTOR_TIMEOUT_BUFFER = timedelta(seconds=50)  # Extra time for retries
WRITE_TIMEOUT = 30.0  # Timeout for write operations (seconds)
