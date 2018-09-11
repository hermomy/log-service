# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 05-09-2018
"""

import mysql.connector as database
from itertools import islice, chain
from db import logger, ORIGIN_TABLE, ARCHIVE_TABLE
from time import sleep

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def archiveLogClick(data, cursor, column_data, batch_count):
    try:
        cols = ["`%s`" % column[0] for column in column_data]
        string_literals = ','.join(['%s'] * (len(cols)))
        columns = ",".join(cols)
        query = "INSERT INTO %s (%s) VALUES (%s)" % (ARCHIVE_TABLE, columns, string_literals)

        logger.info("Number of rows to migrate: {}".format(cursor.rowcount))
        logger.info(query)
        interval = 3
        try:
            seq = xrange(cursor.rowcount)
            for batched in batch(seq, int(batch_count)):
                for item in batched:
                    cursor.execute(query, data[item])
                    logger.info("LogClick ID inserted into {}: {}".format(ARCHIVE_TABLE, data[item][0]))
                logger.warning("Continuing on next batch in {} seconds".format(interval))
                sleep(interval)
        except IndexError:
            logger.warning("End of insert, continuing...")

        # for d in data:
        #     rowId = d[0]
        #     # cursor.execute(query, d)
        #     logger.info("LogClick ID inserted into {}: {}".format(ARCHIVE_TABLE, rowId))

    except database.ProgrammingError as error:
        logger.warning("Programming Error {}".format(error))
    except database.IntegrityError as error:
        logger.warning("Integrity Error: {}".format(error))

def purgeOrigin(cursor, last_date):
    query = "DELETE FROM %s WHERE created <= '%s'" % (ORIGIN_TABLE, last_date)
    try:
        cursor.execute(query)
        logger.info("{} rows with created date <= {} has been deleted".format(cursor.rowcount, last_date))
    except database.ProgrammingError as error:
        logger.warning("Error: {}".format(error))

def closeConnections(connection):
    for key, conn in connection.items():
        conn.commit()
        conn.close()
        logger.info("Operations for {} completed".format(key))



for x in batch(range(0, 10), 3):
    print x