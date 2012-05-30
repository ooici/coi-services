#!/usr/bin/env python

"""
@file coi-services/ion/idk/idk_logger.py
@author Bill French
@brief Setup logging for the idk
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import sys
import logging

from ion.idk.common import Singleton

PATH = os.path.join(os.path.expanduser("~"), ".idk")
LOG_FILENAME = os.path.join(PATH, "idk.log")

class LoggerManager(Singleton):
    """
    Logger Manager.
    Handles all logging files.
    """
    def init(self):
        self.logger = logging.getLogger()
        handler = None
        if not os.path.isdir(PATH):
            try:
                os.mkdir(PATH) # create dir if it doesn't exist
            except:
                raise IOError("Couldn't create \"" + PATH + "\" folder. Check" \
                              " permissions")
        try:
            handler = logging.FileHandler(LOG_FILENAME, "a")
        except:
            raise IOError("Couldn't create/open file \"" + \
                          LOG_FILENAME + "\". Check permissions.")
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(levelname)-10s %(module)-25s %(lineno)-4d %(process)-6d %(threadName)-15s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def debug(self, loggername, msg):
        self.logger = logging.getLogger(loggername)
        self.logger.debug(msg)

    def error(self, loggername, msg):
        self.logger = logging.getLogger(loggername)
        self.logger.error(msg)

    def warn(self, loggername, msg):
        self.logger = logging.getLogger(loggername)
        self.logger.warn(msg)

    def info(self, loggername, msg):
        self.logger = logging.getLogger(loggername)
        self.logger.info(msg)

    def warning(self, loggername, msg):
        self.logger = logging.getLogger(loggername)
        self.logger.warning(msg)

class Logger(object):
    """
    Logger object.
    """
    def __init__(self, loggername="root"):
        self.lm = LoggerManager() # LoggerManager instance
        self.loggername = loggername # logger name

    def debug(self, msg):
        self.lm.debug(self.loggername, msg)

    def warn(self, msg):
        self.lm.warn(self.loggername, msg)

    def error(self, msg):
        self.lm.error(self.loggername, msg)

    def info(self, msg):
        self.lm.info(self.loggername, msg)

    def warning(self, msg):
        self.lm.warning(self.loggername, msg)

Log = Logger()
