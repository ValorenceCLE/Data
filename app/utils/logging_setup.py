import logging
import sys
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import queue
import atexit

# Create a queue for logging to ensure non-blocking behavior
log_queue = queue.Queue()

# Initialize the QueueHandler (no formatter should be set here)
queue_handler = QueueHandler(log_queue)

# Initialize the logger
local_logger = logging.getLogger("local_logger")
local_logger.setLevel(logging.INFO)

# Prevent adding handlers multiple times
if not local_logger.handlers:
    # Add the QueueHandler to the logger
    local_logger.addHandler(queue_handler)
    
    # Disable propagation to prevent log records from being passed to ancestor loggers
    local_logger.propagate = False

    # Define the formatter once (only for the actual output handlers)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)'
    )

    # Set up console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Set up rotating file handler
    file_handler = RotatingFileHandler(
        "application.log",        # Log file name
        maxBytes=5 * 1024 * 1024, # 5 MB
        backupCount=5             # Keep up to 5 backup files
    )
    file_handler.setFormatter(formatter)

    # Initialize the QueueListener with the console and file handlers
    queue_listener = QueueListener(
        log_queue, console_handler, file_handler, respect_handler_level=True
    )
    queue_listener.start()

    # Ensure that the listener stops gracefully on program exit
    atexit.register(queue_listener.stop)
