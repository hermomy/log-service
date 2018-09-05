# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 05-09-2018
"""

import mysql.connector as database
from db import logger, ORIGIN_TABLE, ARCHIVE_TABLE

def archiveLogClick(results, cursor, column_data):
    try:
        cols = ["`%s`" % column[0] for column in column_data]
        string_literals = ','.join(['%s'] * (len(cols)))
        columns = ",".join(cols)
        query = "INSERT INTO %s (%s) VALUES (%s)" % (ARCHIVE_TABLE, columns, string_literals)

        for d in results:
            rowId = d[0]
            cursor.execute(query, d)
            logger.info("LogClick ID inserted into {}: {}".format(ARCHIVE_TABLE, rowId))

    except database.ProgrammingError as error:
        logger.warning(error)

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
