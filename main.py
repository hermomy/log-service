# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
"""

import configparser
import datetime as DT
import mysql.connector as database
import coloredlogs, logging

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)

config = configparser.ConfigParser()
config.read('config.ini')
today = DT.date.today()

for c in config.sections():
    logger.debug('Database: {0}'.format(config[c]['DATABASE']))

    if config[c]['DATABASE'] == 'production':
        logging.basicConfig(filename='LogService.log', filemode='w', level=logging.DEBUG)
        break

connection = {
    'main': database.connect(user=config['MAIN']['USER'],
                     password=config['MAIN']['PASSWORD'],
                     database=config['MAIN']['DATABASE'],
                     port=config['MAIN']['PORT'],
                     host=config['MAIN']['HOST']
                     ),
    'log': database.connect(user=config['LOG']['USER'],
                     password=config['LOG']['PASSWORD'],
                     database=config['LOG']['DATABASE'],
                     port=config['LOG']['PORT'],
                     host=config['LOG']['HOST']
                     )
}

##############

MAIN_insert_log_clicks = 'insert into archive_log_clicks ({0!s}) select log_clicks.* FROM log_clicks WHERE log_clicks.created <= {1!r}'

ORIGIN = connection['main'].cursor(buffered=True)
ARCHIVE = connection['log'].cursor(buffered=True)
origin_table = 'log_clicks'
archive_table = 'archive_log_clicks'

##############

def archiveLogClick(results, cursor, column_data):
    try:
        cols = ["`%s`" % column[0] for column in column_data]
        string_literals = ','.join(['%s'] * (len(cols)))
        columns = ",".join(cols)
        query = "INSERT INTO %s (%s) VALUES (%s)" % (archive_table, columns, string_literals)

        for d in results:
            rowId = d[0]
            cursor.execute(query, d)
            logger.info("LogClick ID inserted into {}: {}".format(archive_table, rowId))

    except database.Error as error:
        logger.warning(error)

def purgeOrigin(cursor, last_date):
    query = "DELETE FROM %s WHERE created <= '%s'" % (origin_table, last_date)
    try:
        cursor.execute(query)
        logger.warning(cursor.statement)
        logger.info("Rows with created date <= {} has been deleted".format(last_date))
    except database.Error as error:
        logger.warning("Error: {}".format(error))

##############

date_condition = "2018-02-01" #today - DT.timedelta(days=365)
ORIGIN.execute("select * FROM log_clicks WHERE created <= {0!r}".format(date_condition))
logger.info(ORIGIN.rowcount)
log_clicks_data = ORIGIN.fetchall()

ORIGIN.execute("desc {}".format(origin_table))
columns = ORIGIN.fetchall()
logger.info('Columns: {}'.format(len(columns)))

archiveLogClick(log_clicks_data, ARCHIVE, columns)
archiveLogClick(log_clicks_data, ORIGIN, columns)

# Clear after archived
purgeOrigin(ORIGIN, date_condition)

logger.info(ARCHIVE.statement)


for key, conn in connection.items():
    conn.commit()
    conn.close()
    logger.info("Operatins for {} completed".format(key))

