# -*- coding: utf-8 -*-
"""
@author: Irfan Radzi
@organization: Hermo Creative Sdn Bhd
@created: 04-09-2018
"""

import configparser
import coloredlogs, logging
import datetime
import mysql.connector as database

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)

config = configparser.ConfigParser()
config.read('config.ini')

for c in config.sections():
    if config[c]['DATABASE'] == 'production':
        logging.basicConfig(filename='LogService.log', filemode='w', level=logging.DEBUG)
        config.read('config_production.ini')

logger.debug('Database: {0}'.format(config[c]['DATABASE']))

connection = {
    'origin': database.connect(user=config['ORIGIN']['USER'],
                     password=config['ORIGIN']['PASSWORD'],
                     database=config['ORIGIN']['DATABASE'],
                     port=config['ORIGIN']['PORT'],
                     host=config['ORIGIN']['HOST']
                     ),
    'archive': database.connect(user=config['ARCHIVE']['USER'],
                     password=config['ARCHIVE']['PASSWORD'],
                     database=config['ARCHIVE']['DATABASE'],
                     port=config['ARCHIVE']['PORT'],
                     host=config['ARCHIVE']['HOST']
                     )
}

ORIGIN = connection['origin'].cursor(buffered=True)
ARCHIVE = connection['archive'].cursor(buffered=True)
ORIGIN_TABLE = config['ORIGIN']['TABLE']
ARCHIVE_TABLE = config['ARCHIVE']['TABLE']
TIMEDELTA = config['STATE']['SYNC_DELTA_DAYS']