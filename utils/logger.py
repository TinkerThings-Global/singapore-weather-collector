import logging


class Logger:
    def __init__(self, name=__name__, level=logging.INFO, filename=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(ch)

        # If a filename is provided, add a file handler
        if filename:
            fh = logging.FileHandler(filename)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)

    def debug(self, message):
        self.logger.debug(message)
