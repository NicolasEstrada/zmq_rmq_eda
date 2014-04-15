import gzip
import logging
import os
import os.path
import re
import time

from tempfile import NamedTemporaryFile, SpooledTemporaryFile

import boto.s3.key

from boto.s3.connection import S3Connection

import file_utils
import logging_manager

from jsonhandler import json

logger = logging_manager.start_logger('python_utils.s3', use_root_logger=False)

# Disables Boto verbose logging
logging.getLogger('boto').setLevel(logging.CRITICAL)

MAX_TEMP_FILE_SIZE = 3221225472  # 3 GB
DEFAULT_CHUNK_SIZE = 104857600  # 100 MB
MIN_CHUNK = 5242880  # 5 MB
DEFAULT_REDUCED_REDUNDANCY = False


class S3BucketWrapper(object):

    def __init__(self, access_key, secret_key, bucketname, reduced_redundancy=DEFAULT_REDUCED_REDUNDANCY):
        self.connection = S3Connection(access_key, secret_key)
        self.reduced_redundancy = reduced_redundancy

        # Temporal fix to have a better name for this property
        self.boto_bucket = self.bucket = self.connection.get_bucket(bucketname)
        self.bucket_name = self.name = bucketname
        self.s3_bucket_url = self.s3_url = "s3://{0}".format(bucketname)

    def __str__(self):
        return "S3 connection to bucket {0} <{1}>".format(self.name, self.s3_url)

    def exists(self, key_name):
        """ Check if the specified key exists in the bucket

        Is a little redundant, but more explicit than checking for the
        result of the get_key() function.

        """

        return False if self.get_key(key_name) is None else True

    def get_key(self, key_name):
        """Helper function to get an existing key from S3

        Input:
            key_name, S3 key name to be fetched.

        Output:
            boto Key object if the key exists, None otherwise.

        """

        return self.boto_bucket.get_key(key_name)

    def get_or_create_key(self, key_name):
        """Helper function to get an existing key, or create a
        new one if it doesn't exists.

        Input:
            key_name, name of the key to get or create on S3

        Output:
            Boto Key object

        """

        k = boto.s3.key.Key(bucket=self.boto_bucket)
        k.key = key_name
        return k

    def download_file(self, key, path='', temporary=False, on_memory=False, retries=2, return_path=False):
        """Download a file from S3, retrying if the download fails.
        Optionally, the file could be downloaded as a temporary file,
        returning a file handler.

        Input:
            key, the S3 key object to download
            path, folder to store the file
            temporary, indicates if the file should be downloaded as a
                       temporary file. With this, the function returns
                       a file handler
            on_memory, if the 'temporary' parameter is True, and this
                       is True, the temporary file is created in memory,
                       using an instance of  tempfile.SpooledTemporaryFile
                       class
            retries, number of retries if the download fails

        Output:
            The path where the file was downloaded, or a file handler if temporary is True

        """

        k = self.get_key(key)

        # Creates the path if it not exists
        if not on_memory and path and not os.path.isdir(path):
            os.makedirs(path)

        fails = 0

        for retry in xrange(retries):
            try:
                logger.info('Downloading {0}/{1} to {2}'.format(self.s3_bucket_url, key, path))

                # When using regular files, it's not necessary to return a file handler
                tmp_file_handler = None

                if temporary:
                    log_msg = "Using a temporary file"

                    # This is a hack: gzip.GzipFile only accepts reading from files
                    # with 'r' or 'rb' mode. This is checked recovering the first
                    # letter from the mode, so, opening the file using the 'r+b' mode
                    # allows reading and writting the temporary file without problems
                    if re.search('\.gz$', key):
                        file_mode = 'r+b'
                    else:
                        # In other cases, use the default mode for NamedTemporaryFile
                        file_mode = 'w+b'

                    if on_memory:
                        log_msg += " [on memory]"
                        tmp_file_handler = SpooledTemporaryFile(mode=file_mode, max_size=MAX_TEMP_FILE_SIZE)
                    else:
                        log_msg += " [on disk]"
                        tmp_file_handler = NamedTemporaryFile(mode=file_mode, dir=path)

                    log_msg += " [MODE: {0}]"
                    logger.debug(log_msg.format(file_mode))

                    k.get_contents_to_file(tmp_file_handler)

                    # This is necessary, Boto doesn't do it
                    tmp_file_handler.seek(0)
                else:
                    logger.debug("Using a regular file")

                    _, filename = os.path.split(key)

                    if path:
                        final_path = os.path.join(path, filename)
                    else:
                        final_path = filename

                    k.get_contents_to_filename(final_path)

                logger.info('File {0} was succesfully downloaded'.format(key))

                if temporary:
                    return tmp_file_handler
                elif return_path:
                    return final_path

            except Exception:
                log_msg = 'Download failed for {0}/{1}...'.format(self.s3_bucket_url, key)

                fails += 1

                if fails >= retries:
                    log_msg += ' Not retrying (Failed retries: {0})'.format(retry)
                    logger.error(log_msg)
                    raise
                else:
                    log_msg += ' Retrying'
                    logger.info(log_msg)

    def upload_file(self, filename, key, retries=2, reduced_redundancy=None):
        """

        """

        if reduced_redundancy is None:
            reduced_redundancy = self.reduced_redundancy

        file_size = os.stat(filename).st_size
        logger.debug("File size: {0} bytes".format(file_size))

        ping = time.time()
        fails = 0

        log_msg = "Uploading {0} to {1}/{2}".format(filename, self.s3_bucket_url, key)

        for retry in xrange(retries):
            try:
                # If the file is too big...
                if file_size > (DEFAULT_CHUNK_SIZE * 2):
                    log_msg += " using multipart mode"
                    logger.info(log_msg)

                    # The multipart uploads begins here
                    multipart = self.boto_bucket.initiate_multipart_upload(
                        key,
                        reduced_redundancy=reduced_redundancy)

                    # Get the points where the file should be divided
                    checkpoints = self.file_offsets(filename)

                    # Open a file handler
                    with open(filename, 'r') as input_file:
                        # Cycle through the checkpoints
                        for part, offset in checkpoints:

                            logger.debug("Uploading chunk #{0} (offset: {1})".format(
                                part,
                                offset))

                            # Upload the part and add the number of bytes
                            # uploaded
                            multipart.upload_part_from_file(
                                input_file,
                                part,
                                size=offset)

                    # Finish the upload
                    mp_status = multipart.complete_upload()

                    # Show the upload status in the logs
                    logger.debug(str(mp_status))
                else:
                    log_msg += " using simple mode"
                    logger.info(log_msg)

                    # Creates a new Key in the bucket, using the output filename
                    new_key = self.boto_bucket.new_key(key)

                    with open(filename, 'r') as input_file:
                        new_key.set_contents_from_file(
                            input_file,
                            reduced_redundancy=reduced_redundancy)

                break

            except Exception as e:
                logger.exception(e)

                log_msg = 'Upload failed for {0} to {1}/{2}...'.format(filename, self.s3_bucket_url, key)
                fails += 1

                if fails >= retries:
                    log_msg += ' Not retrying (Failed retries: {0})'.format(retry)
                    logger.error(log_msg)
                    raise
                else:
                    log_msg += ' Retrying'
                    logger.info(log_msg)

        size_mb = ((float(file_size) / 1024) / 1024)
        elapsed = time.time() - ping

        logger.info("File {0} successfully uploaded! {1:6.1f} MiB in {2:.1f}s ({3} KiB/s)".format(
            filename,
            size_mb,
            elapsed,
            int(size_mb * 1000 / elapsed)))

    # def compress_and_upload(self, filename, key, reduced_redundancy=REDUNDANCY):
    #     with NamedTemporaryFile(delete=False) as compressed_file:
    #         gzip_handler = gzip.GzipFile(fileobj=compressed_file)

    #         with open(filename, 'r') as file_handler:
    #             gzip_handler.write(file_handler.read())

    #         gzip_handler.close()

    #         self.upload_file(compressed_file.name, key, reduced_redundancy)

    # def upload_from_filename(self, filename):
    #     with open(filename, 'r') as file_handler:
    #         self.upload_file(file_handler)

    def file_offsets(self, filename):
        """

        """

        file_size = os.stat(filename).st_size

        offset = 0
        part = 1

        # Get the checkpoints
        while offset < file_size:

            chunk_size = 0

            # How much bytes are left after sending one chunk
            remaining_bytes = file_size - offset - DEFAULT_CHUNK_SIZE

            # If after the upload there's more data to be uploaded...
            if remaining_bytes > 0:

                # But, if the amount of bytes is less than MIN_CHUNK size...
                if remaining_bytes < MIN_CHUNK:
                    # Upload all the remaining data
                    chunk_size = file_size - offset
                    offset += chunk_size

                # Otherwise, upload one complete chunk
                else:
                    chunk_size = DEFAULT_CHUNK_SIZE
                    offset += chunk_size

            # Otherwise, just upload the remaining data
            else:
                chunk_size = file_size - offset
                offset += chunk_size

            yield (part, chunk_size)

            part += 1

    def file_to_dicts(self, key):
        """Fetchs a key from S3 as a temporary file and the returns a
        generetor, which yields each line as a Python dictionary

        Input:
            key: string, name of S3 key, such as
                summaries/dashboard_user/20120801/1/filename
                The file must contain valid JSON objects on each line.

        Output:
            Generator that returns one dictionary object per next() call

        """

        _, filename = os.path.split(key)

        with self.download_file(key, temporary=True, on_memory=True) as file_handler:
            if re.search('\.gz$', key):
                file_content = gzip.GzipFile(fileobj=file_handler)
            else:
                file_content = file_handler

            for line in file_content:
                clean_line = line.strip()

                if clean_line:
                    try:
                        yield json.loads(clean_line)
                    except ValueError as e:
                        logger.exception('JSON decode error: {0}'.format(e))

    def get_file_lines(self, key, start_line_offset=0, end_line_offset=None):
        """

        """

        end_line_offset = float('inf') if end_line_offset is None else end_line_offset

        logger.debug("Fetching file from '{0}/{1}' from line {2} to {3}".format(
            self.s3_bucket_url,
            key,
            start_line_offset,
            end_line_offset))

        with self.download_file(key, temporary=True, on_memory=True) as file_handler:
            if re.search('\.gz$', key):
                new_handler = gzip.GzipFile(fileobj=file_handler)
            else:
                new_handler = file_handler

            return file_utils.get_lines_from_handler(
                new_handler,
                start_line_offset=start_line_offset,
                end_line_offset=end_line_offset)

    def find_keys(self, regexp, prefix='', return_real_key=False):
        """This function returns a list of key names that match the regexp.

        """

        logger.debug("Searching for pattern '{0}', Using prefix '{1}' on {2}".format(
            regexp,
            prefix,
            self.s3_bucket_url))

        pattern = re.compile(regexp)

        for key in self.boto_bucket.list(prefix=prefix):
            if pattern.search(key.name):
                if return_real_key:
                    yield key
                else:
                    yield key.name

    def delete_keys(self, keys=None, regexp=None, prefix=''):
        """Deletes a set of keys. If the keys parameter is specified,
        it's assumed that it is an iterable with the keys to be deleted.

        If the regexp parameter is specified, it corresponds to a valid
        REGEXP that will be used to search for the keys to be deleted.
        Optionally, a prefix could be specified to make the search for keys
        faster.

        """

        if regexp:
            search_pattern = 'regexp'
            keys = self.find_keys(regexp, prefix)
        # This conditions is here for debugging purposes (look at the log message below!)
        elif keys:
            search_pattern = 'key list'
        else:
            raise ValueError("None of the parameters are valid [keys: {0} / regexp: {1}]".format(keys, regexp))

        deleted_count = 0
        error_count = 0

        if keys:
            results = self.boto_bucket.delete_keys(keys)

            for error_count, result in enumerate(results.errors, start=1):
                logger.error("Couldn't delete key {0}: {1} ({2})".format(
                    result.key,
                    result.message,
                    result.code))

            for deleted_count, result in enumerate(results.deleted, start=1):
                logger.debug("Key {0} deleted (DeleteMarker: {1})".format(
                    result.key,
                    result.delete_marker))

            logger.info("{0} keys deleted, {1} with errors [Using {2}]".format(
                deleted_count,
                error_count,
                search_pattern))
        else:
            logger.info("No keys ")

        return (deleted_count, error_count)
