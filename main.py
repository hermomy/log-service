# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 04-09-2018
"""

import datetime as DT
from db import logger, connection, ORIGIN, ORIGIN_TABLE, ARCHIVE, TIMEDELTA
from utilities import archiveLogClick, purgeOrigin, closeConnections

today = DT.date.today()

date_condition = today - DT.timedelta(days=int(TIMEDELTA))
logger.info("Data will be selected from {} onwards ({} days).".format(date_condition, TIMEDELTA))
ORIGIN.execute("select * FROM log_clicks WHERE created <= '{0!s}'".format(date_condition))

log_clicks_data = ORIGIN.fetchall()

ORIGIN.execute("desc {}".format(ORIGIN_TABLE))
columns = ORIGIN.fetchall()
logger.info('Number of columns from origin: {}'.format(len(columns)))

archiveLogClick(log_clicks_data, ARCHIVE, columns)
archiveLogClick(log_clicks_data, ORIGIN, columns)

# Clear after archived
if ARCHIVE.rowcount > 0:
    logger.warning('%s rows successfully added' % ARCHIVE.rowcount)
    purgeOrigin(ORIGIN, date_condition)
else:
    logger.info('No rows has been affected')

closeConnections(connection)