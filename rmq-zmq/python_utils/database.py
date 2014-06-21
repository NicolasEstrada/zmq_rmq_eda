"""This is a wrapper for MySQL. Super simple to use.
"""

import time

import MySQLdb
import MySQLdb.cursors

from _mysql_exceptions import IntegrityError

import logging_manager

__author__ = "Nicolas, Matias, Gonzalo"
__version__ = "0.2"

RETRY_IN = 10
HOST_TEMPLATE = 'hostname_with_{0}_fill'
DB_TEMPLATE = 'dbname_with_{0}_fill'

logger = logging_manager.start_logger('python_utils.database', use_root_logger=False)


# Database class
class Database(object):
    def __init__(self, host='localhost', port=3306, dbname='test', username='root', password=''):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.db_connection = None
        self.cursor = None
        self.connecting = False

    def __del__(self):
        self.disconnect()

    def connect(self):
        """Call this function to connect to MySQL.
        Not strictly necessary, as it happens by itself when you run a query.
        Input: None
        Output: Returns True when connected"""

        self.connecting = True

        log_msg = 'Connecting to MySQL {0}@{1}:{2}. Database: {3}'.format(
            self.username,
            self.host,
            self.port,
            self.dbname)
        logger.info(log_msg)

        while not self.db_connection:
            try:
                self.db_connection = MySQLdb.connect(
                    passwd=self.password,
                    db=self.dbname,
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    cursorclass=MySQLdb.cursors.DictCursor,
                    charset="utf8",
                    use_unicode="True")

                logger.info("Connection stablished")

                # Setting autocommit True for InnoDB transactions
                self.db_connection.autocommit(True)
                self.cursor = self.db_connection.cursor()
                self.connecting = False

            except MySQLdb.Error, e:
                logger.exception(("DB Error {0}: {1}".format(e.args[0], e.args[1])))
                # Waiting the db connection
                time.sleep(RETRY_IN)
                return False

            except Exception, e:
                logger.exception("Error: {0}".format(str(e)))
                time.sleep(RETRY_IN)
                return False

        return True

    def disconnect(self):
        """Disconnect from MySQL"""
        if self.is_connected():
            self.db_connection.close()
            # We have to create a new object to reconnect.
            self.db_connection = None
        elif self.db_connection:
            self.db_connection = None

    def is_connected(self):
        """Check if the connection is stablished"""
        return self.db_connection and self.db_connection.open

    def query(self, query, data=None):
        """Use this function to retrieve information from the MySQL databases.
        Input:  query, string with mysql query, use %s for input parameters
                data (optional), an array of strings (or objects that are
                    correctly formatted if you call them with str()) which
                    should be placed in the position of the %s in the query.
        Output: a list of dictionaries, where the keys are column names and
                    the values are the row values. Note that most formats
                    will be correctly converted,
                    such as integers and datetimes.
        """
        logger.debug("Performing the query")
        # If the connection is down, reconnect
        if not self.db_connection:
            logger.info("Reconnecting at query execution ...")
            if self.connecting:
                logger.info("Waiting db connection ...")
                return None
            else:
                logger.info("Connecting to db: {0}".format(self.dbname))
                self.connect()

        try:
            self.cursor.execute(query, data)
            return self.cursor.fetchall()

        except MySQLdb.Error, e:
            # Check if the connection is up and dismiss the query
            logger.exception(("DB Error {0}: {1} Last query executed {2}".format(
                e.args[0],
                e.args[1],
                self.cursor._last_executed)))

            self.disconnect()

            return None

    def big_query(self, query, data=None, batchsize=1000):
        """Use this function if you want to return more data than just
        a few lines. It returns a generator instead of just a huge bunch
        of data at once. Please don't add the ';' to the end of the query,
        because we'll be appending LIMITs to the end of the query.

        Input:  query, string with mysql query, use %s for input parameters
                data (optional), an array of strings (or objects that are
                    correctly formatted if you call them with str()) which
                    should be placed in the position of the %s in the query.
                batchsize (optional), the amount of rows to return per next().
                Defaults to 1000.
        Output: A generator that returns results of the 'query' function
                    (but then with limited size) on every next() call."""
        offset = 0
        while True:
            logger.debug(("Running query from {0} with limit {1}"
                          .format(offset, batchsize)))
            new_query = "{0} LIMIT {1}, {2};".format(query, offset, batchsize)
            result = self.query(new_query, data)

            if len(result) > 0:
                yield result
            else:
                break
            offset += batchsize

        return

    def insert_rows(self, table, cols, rows):
        """Insert rows in the specified columns into the table.

        Input:  table, string name of table.
                cols, list of strings with the cols to be filled.
                rows, list of tuples with the corresponding values for
                each column. Must be in the same order.

        """

        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
            table,
            ', '.join(cols),
            ', '.join(("%s" for i in xrange(len(cols)))))

        if isinstance(rows, list):
            if len(rows) > 1:
                self.cursor.executemany(query, rows)
            else:
                self.cursor.execute(query, rows[0])
        elif isinstance(rows, tuple):
            self.cursor.execute(query, rows)

    def get_cols(self, table):
        """This function get a list of columns names for the specified table

        Input:  table name (string)

        Output: columns names (list of strings)

        """

        query = """ SELECT `COLUMN_NAME`
        FROM `INFORMATION_SCHEMA`.`COLUMNS`
        WHERE `TABLE_SCHEMA`='{dbname}'
        AND `TABLE_NAME`='{table_name}';""".format(
            dbname=self.dbname,
            table_name=table)

        dict_col = self.query(query)
        columns = []

        for col in range(1, len(dict_col)):
            columns.append(dict_col[col]["COLUMN_NAME"])

        return columns

# Peewee helpers


def insert_into_mysql(model_class, raws_as_dict, silent=True):
    """Function to insert dictionaries into MySql.
    This function use peewee and the models defined on Rig to keep
    consistency and if we have to made some changes, just modify the code
    only in one part.
    Also, peewee don't put rules like default values in MySql schema, because
    if you changes the models in peewee then changes in the schema will be
    required. For that reason we can't use an custom database helper to insert
    the data, because we probally have some inconsistency issues.

    Input:  model_class must be a class that inherits from peewee.Model
            raws dict generator or list.
            silent (boolean), when this option is True, the insert will
            not fail on duplicate data. Otherwise, will raise an error.

    Output: Number of rows afected.
    """
    count = 0
    for raw in raws_as_dict:
        if silent:
            model_class.get_or_create(**raw)
            count += 1
        else:
            try:
                model_class.create(**raw)
                count += 1
            except IntegrityError:
                logger.debug(
                    "[{0}] Duplicate entry for {1}, timeframe: {2} ".format(
                    raw.get("date", ''),
                    raw.get("name", ''),
                    raw.get("timeframe", '')))

    return count


class ShardedDatabase(object):
    """Class that handles queries over all shards"""
    def __init__(self, host_template=HOST_TEMPLATE, port=3306, db_template=DB_TEMPLATE,
                 shardids=[], username='test', password=''):

        super(ShardedDatabase, self).__init__()
        self.dbs = {}
        self.shardids = shardids
        for shard in self.shardids:
            self.dbs[shard] = Database(
                host=host_template.format(shard),
                port=port,
                dbname=db_template.format(shard),
                username=username,
                password=password)

    def query(self, query, data=None):
        """Performs query over multiple shards"""
        logger.debug("Performing the query")
        for shard in self.shardids:
            yield self.dbs[shard].query(query, data)

    def query_shard_target(self, shardid, query, data=None):
        """Performs query depending on shardid"""
        return self.dbs[shardid].query(query, data)

    def big_query(self, query, data=None, batchsize=1000):
        """Performs a big query over multiple shards"""
        logger.debug("Performing the query")
        for shard in self.shardids:
            yield self.dbs[shard].big_query(query, data, batchsize)

