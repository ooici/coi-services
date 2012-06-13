#!/usr/bin/env python

"""
@package ion.agents.instrument.driver_launcher
@file ion/agents.instrument/driver_launcher.py
@author Bill French
@brief Drive process class that provides a factory for different launch mechanisms
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import sys
import uuid
import time
from subprocess import Popen

from pyon.util.log import log
from ion.agents.instrument.common import BaseEnum

from ion.agents.instrument.exceptions import DriverLaunchException
from ion.agents.instrument.exceptions import NotImplementedException

PYTHON_PATH = 'bin/python'

class DriverProcessType(BaseEnum):
    """
    Base states for driver launcher types.
    """
    PYTHON_MODULE = 'ZMQPyClassDriverLauncher',
    EGG = 'ZMQEggDriverLauncherG'


class DriverProcess(object):
    """
    Base class for driver process launcher
    """
    _driver_process = None
    _driver_client = None

    _driver_event_file = None
    _driver_command_file = None

    _command_port = None
    _event_port = None

    _packet_factories = None

    @classmethod
    def get_process(cls, driver_config, test_mode = False):
        """
        Factory method to get the correct driver process type based on the type in driver_config
        """
        driver_module = driver_config.get('dvr_mod')
        type = driver_config.get("process_type")

        if not type:
            raise DriverLaunchException("missing driver config: process_type")

        # For some reason the enum wasn't working with the instrument agent.  I would see [emum] when called from
        # the IA, but (enum,) when called from this module.
        #elif type == DriverProcessType.PYTHON_MODULE:
        elif driver_module:
            return ZMQPyClassDriverProcess(driver_config, test_mode)

        elif type == DriverProcessType.EGG:
            raise NotImplementedException()
            return ZMQEggDriverProcess(driver_config, test_mode)

        else:
            raise DriverLaunchException("unknown driver process type: %s" % type)

    def launch(self):
        """
        Launch the driver process
        """
        log.info("Launch driver process")

        cmd = self._process_command()
        self._driver_process = self._spawn(cmd)

        if not self._driver_process or self._driver_process.poll():
            log.error("Failed to launch driver: %s" % cmd)
            raise DriverLaunchException('Error starting driver process')

        log.debug("driver process started, pid: %s" % self.getpid())

        self._command_port = self._get_port_from_file(self._driver_command_port_file())
        self._event_port = self._get_port_from_file(self._driver_event_port_file())

        log.debug("-- command port: %s, event port: %s" % (self._command_port, self._event_port))

    def stop(self):
        if self._driver_process:
            if self._driver_client:
                self._driver_client.done()
                self._driver_process.wait()
                self._driver_process = None
                self._driver_client = None
                log.debug("driver process stopped")
            else:
                try:
                    self._driver_process.kill()
                    self._driver_process.wait()
                    self._driver_process = None
                    log.debug("driver process killed")

                except OSError:
                    pass

    def getpid(self):
        if self._driver_process:
            return self._driver_process.pid
            if self._driver_process.poll():
                return self._driver_process.pid
        else:
            return None

    def _spawn(self, spawnargs):
        """
        Launch a process using popen
        """
        log.debug("run cmd: %s" % " ".join(spawnargs))
        return Popen(spawnargs, close_fds=True)


    def _process_command(self):
        raise DriverLaunchException("_process_command must be overloaded")

    def _get_port_from_file(self, filename):
        log.debug("read port from file: %s" % filename)

        #TODO: Should this timeout?
        while True:
            try:
                cmd_port_file = file(filename, 'r')
                port = int(cmd_port_file.read().strip())
                cmd_port_file.close()
                os.remove(filename)
                break

            except IOError, e:
                log.debug("failed to read file %s: %s (retry)" % (filename, e))
                time.sleep(.1)
                pass

        return port

    def _driver_workdir(self):
        return self.config.get('workdir', '/tmp')

    def _driver_command_port_file(self):
        if not self._driver_command_file:
            tag = str(uuid.uuid4())
            self._driver_command_file = os.path.join(self._driver_workdir(), 'dvr_cmd_port_%s.txt' % tag)

        return self._driver_command_file

    def _driver_event_port_file(self):
        if not self._driver_event_file:
            tag = str(uuid.uuid4())
            self._driver_event_file = os.path.join(self._driver_workdir(), 'dvr_evt_port_%s.txt' % tag)

        return self._driver_event_file




class ZMQPyClassDriverProcess(DriverProcess):
    """
    Object to facilitate ZMQ driver processes using a python class and module path
    """
    def __init__(self, driver_config, test_mode = False):
        self.config = driver_config
        self.test_mode = test_mode

    def _process_command(self):
        """
        Build the process command line using the driver_config dict
        """
        log.debug("cwd: %s" % os.getcwd())
        driver_module = self.config.get('dvr_mod')
        driver_class = self.config.get('dvr_cls')
        ppid = os.getpid() if self.test_mode else None

        python = PYTHON_PATH

        if not driver_module:
            raise DriverLaunchException("missing driver config: driver_module")
        if not driver_class:
            raise DriverLaunchException("missing driver config: driver_class")
        if not os.path.exists(python):
            raise DriverLaunchException("could not find python executable: %s" % python)

        cmd_port_fname = self._driver_command_port_file()
        evt_port_fname = self._driver_event_port_file()
        cmd_str = 'from %s import %s; dp = %s("%s", "%s", "%s", "%s", %s);dp.run()'\
        % ('mi.core.instrument.zmq_driver_process', 'ZmqDriverProcess', 'ZmqDriverProcess', driver_module,
           driver_class, cmd_port_fname, evt_port_fname, str(ppid))

        return [ python, '-c', cmd_str ]

    def get_client(self):
        # Start client messaging and verify messaging.
        if not self._driver_client:
            try:
                from mi.core.instrument.zmq_driver_client import ZmqDriverClient
                driver_client = ZmqDriverClient('localhost', self._command_port, self._event_port)
                self._driver_client = driver_client
            except Exception, e:
                self.stop()
                log.error('Error starting driver client: %s' % e)
                raise DriverLaunchException('Error starting driver client.')

        return self._driver_client

    def get_packet_factories(self):
        """
        Construct packet factories from packet_config member of the
        driver_config.
        @retval None
        """
        if not self._packet_factories:
            log.info("generating packet factories")
            self._packet_factories = {}

            driver_module = self.config.get('dvr_mod')
            if not driver_module:
                raise DriverLaunchException("missing driver config: driver_module")

            import_str = 'from %s import PACKET_CONFIG' % driver_module
            try:
                exec import_str
                log.debug("PACKET_CONFIG: %s", PACKET_CONFIG)
                for (name, val) in PACKET_CONFIG.iteritems():
                    if val:
                        try:
                            mod = val[0]
                            cls = val[1]
                            import_str = 'from %s import %s' % (mod, cls)
                            ctor_str = 'ctor = %s' % cls
                            exec import_str
                            exec ctor_str
                            self._packet_factories[name] = ctor

                        except Exception, e:
                            log.error('error creating packet factory: %s', e)

                        else:
                            log.info('created packet factory for stream %s', name)
            except Exception, e:
                log.error('Instrument agent %s had error creating packet factories. %s', e)

        return self._packet_factories

class ZMQEggDriverLauncher(DriverProcess):
    """
    Object to facilitate driver processes launch from an egg
    """
    def __init__(self):
        pass
