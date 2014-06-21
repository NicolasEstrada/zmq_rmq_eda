import logging
import logging.handlers
import multiprocessing
import os

import clint

DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_LOG_FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
DEFAULT_LOG_PATH = 'log'
DEFAULT_BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FULL_LOG_PATH = os.path.join(DEFAULT_BASE_PATH, DEFAULT_LOG_PATH)

LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def start_logger(
    log_filename,
    log_path=DEFAULT_FULL_LOG_PATH,
    logger_name=None,
    log_format=DEFAULT_LOG_FORMAT,
        use_root_logger=True):
    # If the environment variables ENV is setted
    # we set the logging level according to his
    # value
    use_console = False

    if 'WORKING_ENV' in os.environ:
        level = os.environ['WORKING_ENV']
        log_level = LOG_LEVELS[level]

        if level == 'DEBUG':
            use_console = True
    else:
        log_level = None

        # Search if the user passed the logging level in the CLI arguments
        for level in LOG_LEVELS:
            if level in clint.args.all:
                log_level = LOG_LEVELS[level]

                if level == 'DEBUG':
                    use_console = True

                break

        # If not, the dafault level is INFO
        if log_level is None:
            log_level = LOG_LEVELS[DEFAULT_LOG_LEVEL]
            level = DEFAULT_LOG_LEVEL

    if logger_name is None:
        if use_root_logger:
            logger_name = ''
        else:
            logger_name = log_filename

    # Gets a logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Adds the handler
    logger.addHandler(init_file_logger(log_filename, log_path, log_level, log_format))

    if use_console:
        logger.addHandler(init_console_logger(log_level, log_format))

    logger.info('Initializing logging handler with {0} level'.format(level))

    return logger


def start_multiprocessing_logger(log_format=DEFAULT_LOG_FORMAT, send_to_stderr=False):
    if 'WORKING_ENV' in os.environ:
        level = os.environ['WORKING_ENV']
        log_level = LOG_LEVELS[level]
    else:
        log_level = None

        # Search if the user passed the logging level in the CLI arguments
        for level in LOG_LEVELS:
            if level in clint.args.all:
                log_level = LOG_LEVELS[level]
                break

        # If not, the dafault level is INFO
        if log_level is None:
            log_level = LOG_LEVELS[DEFAULT_LOG_LEVEL]
            level = DEFAULT_LOG_LEVEL

    if send_to_stderr:
        logger = multiprocessing.log_to_stderr()
    else:
        logger = multiprocessing.get_logger()

    logger.setLevel(log_level)

    # Adds the handler
    logger.addHandler(init_console_logger(log_level, log_format))
    logger.info('Initializing multiprocessing logging handler with {0} level'.format(level))

    return logger


def init_console_logger(log_level, log_format):
    # Set up the console logging
    console_log = logging.StreamHandler()
    console_log.setLevel(log_level)
    console_log.setFormatter(logging.Formatter(log_format))

    return console_log


def init_file_logger(file_name, path, log_level, log_format):

    # Check if the directory tree exists
    if not os.path.isdir(path):
        # If not, creates the non-existent directories
        os.makedirs(path)

    full_path = os.path.join(path, "{0}.log".format(file_name))

    # Set the file logging
    file_log = logging.handlers.TimedRotatingFileHandler(
        full_path,
        when='midnight',
        utc=True)
    file_log.setLevel(log_level)

    # Sets formatter
    file_log.setFormatter(logging.Formatter(log_format))

    return file_log


def init_smtp_logger(server, from_address, to_addresses, subject):
    smtp_log = logging.handlers.SMTPHandler(
        mailhost=server,
        fromaddr=from_address,
        toaddrs=to_addresses,
        subject=subject)

    smtp_log.setLevel(LOG_LEVELS['ERROR'])
    smtp_log.setFormatter(logging.Formatter(
        "TIME: %(asctime)s\nLOGGER: %(name)-12s\nERROR LEVEL: %(levelname)-8s\nMESSAGE: %(message)s"))

    return smtp_log
