import errno
import gzip
import os
import re

import logging_manager

logger = logging_manager.start_logger('python_utils.file_utils', use_root_logger=False)


def better_open(path, mode='r'):

    if re.search('\.gz$', path):
        file_handler = gzip.GzipFile(filename=path, mode=mode)
    else:
        file_handler = open(path, mode)

    return file_handler


# def get_lines_from_handler(file_handler, start_line_offset=0, end_line_offset=None):
#     end_line_offset = float('inf') if end_line_offset is None else end_line_offset

#     for line_count, line in enumerate(file_handler):
#         if start_line_offset <= line_count <= end_line_offset:
#             yield line
#         elif line_count > end_line_offset:
#             break


def get_lines(path, start_line_offset=0, end_line_offset=None):
    end_line_offset = float('inf') if end_line_offset is None else end_line_offset

    with better_open(path, 'r') as file_handler:
        for line_count, line in enumerate(file_handler):
            if start_line_offset <= line_count <= end_line_offset:
                yield line
            elif line_count > end_line_offset:
                break


def count_lines(path):
    with better_open(path, 'r') as file_r:
        line_number = 0

        for line_number, line in enumerate(file_r, start=1):
            pass

    return line_number


def silent_remove(path):
    try:
        os.remove(path)
    except OSError as e:
        logger.exception("Couldn't delete file {0}".format(path))
        if e.errno != errno.ENOENT:
            raise


def delete_files(files, silent=False):
    for path in files:

        logger.debug("Deleting '{0}'".format(path))

        if silent:
            silent_remove(path)
        else:
            os.remove(path)
