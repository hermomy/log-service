# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 04-09-2018
"""

import sys
import datetime as DT
from db import logger, connection, ORIGIN, ORIGIN_TABLE, ARCHIVE, ARCHIVE_TABLE, TIMEDELTA
from utilities import archiveLogClick, purgeOrigin, closeConnections
import mysql.connector as database

today = DT.date.today()
batch = 5

if len(sys.argv) > 1:
    batch = sys.argv[1]

date_condition = today - DT.timedelta(days=int(TIMEDELTA))
logger.info("Data will be selected from {} onwards ({} days).".format(date_condition, TIMEDELTA))

try:
    ORIGIN.execute("select * FROM log_clicks WHERE created <= '{0!s}'".format(date_condition))
    ARCHIVE.execute("select * FROM log_clicks WHERE created <= '{0!s}'".format(date_condition))

    log_clicks_data = ORIGIN.fetchall()

    ORIGIN.execute("desc {}".format(ORIGIN_TABLE))
    columns = ORIGIN.fetchall()

    # archiveLogClick(log_clicks_data, ARCHIVE, columns, batch)
    # archiveLogClick(log_clicks_data, ORIGIN, columns, batch)

    ARCHIVE.execute("desc {}".format(ARCHIVE_TABLE))
    # Clear after archived
    if ARCHIVE.rowcount > 0:
        logger.warning('%s rows successfully added' % ARCHIVE.rowcount)
        purgeOrigin(ORIGIN, date_condition)
    else:
        logger.info('No rows has been affected')

    closeConnections(connection)
except (database.IntegrityError, database.ProgrammingError) as error:
    for key, conn in connection.items():
        conn.rollback()
        conn.close()
        logger.info("Rollback {}".format(key))