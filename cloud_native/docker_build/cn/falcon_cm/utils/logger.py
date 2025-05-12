import logging
import logging.handlers

def logging_init(log_file, log_level):
    """Settings for logging, such as format, level, and handler."""
    logger = logging.getLogger("logger")
    logger.setLevel(log_level)
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when="D", interval=1, backupCount=7)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    
    
    
