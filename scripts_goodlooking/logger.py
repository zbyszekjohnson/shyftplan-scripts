import logging


def configure_logger():
    logging.basicConfig(filename='logs.log', level=logging.INFO)
    logger = logging.getLogger("my_logger")
    return logger
