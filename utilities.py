# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 05-09-2018
"""

import mysql.connector as database
from itertools import islice, chain
from db import connection, logger, ORIGIN_TABLE
from time import sleep

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def archiveLogClick(data, cursor, column_data, table_into, batch_count):
    try:
        total_rows = len(data)
        if total_rows > 0:
            cols = ["`%s`" % column[0] for column in column_data]
            string_literals = ','.join(['%s'] * (len(cols)))
            columns = ",".join(cols)
            query = "INSERT INTO %s (%s) VALUES (%s)" % (table_into, columns, string_literals)
            logger.info("Number of rows to migrate: {}".format(total_rows))
            interval = 3
            ids = []
            try:
                seq = xrange(total_rows)
                for batched in batch(seq, int(batch_count)):
                    for item in batched:
                        ids.append(data[item][0]) # Appending duplicate ids for deletion
                        try:
                            cursor.execute(query, data[item])
                            logger.info("LogClick ID inserted into {}: {}".format(table_into, data[item][0]))
                        except database.IntegrityError as error:
                            total_rows = total_rows - 1
                            logger.error("{}, continuing...".format(error))
                            pass
                cursor.execute("select database()")
                db_name = cursor.fetchone()[0]
                logger.info("{} records successfully inserted into {}.{}.".format(total_rows, db_name, table_into))
            except IndexError:
                logger.warning("End of insert, continuing...")
            return ids
        else:
            logger.info("Nothing to update.")
            return []
    except database.ProgrammingError as error:
        logger.error("Error {}".format(error))

def checkConnections(connection, status="close"):
    for key, conn in connection.items():
        conn.commit()
        conn.close() if status == "close" else conn.reconnect(attempts=3, delay=0)
        logger.info("Connections on {} {}".format(key, status))

def purgeOrigin(cursor, item_ids):
    # Delete rows with inserted ids in ARCHIVE
    logger.info("Deleting archived records...")

    seq = xrange(len(item_ids))
    try:
        for batched in batch(seq, 150):
            for d in batched:
                iid = item_ids[d]
                delquery = "DELETE FROM %s WHERE id = %s" % (ORIGIN_TABLE, iid)
                cursor.execute(delquery)
                logger.info("Deleted {}".format(iid))
        cursor.execute("select database()")
        db_name = cursor.fetchone()[0]
        logger.info("{} rows with has been deleted from {}.{}".format(len(item_ids), db_name, ORIGIN_TABLE))
    except database.OperationalError as error:
        logger.warning("Error deleting: {}".format(error))
    except database.ProgrammingError as error:
        logger.warning("Error: {}".format(error))