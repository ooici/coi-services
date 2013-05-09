#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.logger
@file    ion/agents/platform/rsn/simulator/logger.py
@author  Carlos Rueda
@brief   Logger configuration for the OMS simulator.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


class Logger(object):
    log = None

    @classmethod
    def set_logger(cls, log):
        """
        Allows to specify the logger from outside (in particular to allow the
        use of pyon.public.log).
        """
        cls.log = log

    @classmethod
    def get_logger(cls):
        """
        Gets the logger for the simulator. This is configured here unless a
        logger has been specified via set_logger.
        """
        if not cls.log:
            import logging
            import os
            log = logging.getLogger('oms_simulator')
            level_expr = "logging.%s" % os.getenv("oms_simulator_loglevel", "WARN")
            print "oms_simulator: setting log level to: %s" % level_expr
            try:
                level = eval(level_expr)
            except Exception as ex:
                print "oms_simulator: Error evaluating %r: %s" % (level_expr, str(ex))
                print "oms_simulator: setting log level to: logging.DEBUG"
                level = logging.DEBUG
            log.setLevel(level)
            handler = logging.StreamHandler()
            handler.setLevel(level)
            format = '%(asctime)s %(levelname)-8s %(threadName)s %(name) -15s:%(lineno)d %(funcName)s %(message)s'
            formatter = logging.Formatter(format)
            handler.setFormatter(formatter)
            log.addHandler(handler)
            cls.log = log
        return cls.log
