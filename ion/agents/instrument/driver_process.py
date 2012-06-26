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
import signal
import subprocess

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
        Factory method to get the correct driver process object based on the type in driver_config
        @param driver_config a dict containing configuration information for the driver process. This will be different
               for each driver process type, but each should minimally have 'process_type' which defines which object
               to use for launching.
        @param test_mode tell the driver you are running in test mode.  Some drivers use this to add a poison pill to
               the driver process.
        """
        type = driver_config.get("process_type")
        driver_module = driver_config.get('dvr_mod')

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
        Launch the driver process. Once the process is launched read the two status files that contain the event port
        and command port for the driver.
        @raises DriverLaunchException
        """
        log.info("Launch driver process")

        cmd = self._process_command()
        self._driver_process = self._spawn(cmd)

        if not self._driver_process and not self.poll():
            log.error("Failed to launch driver: %s" % cmd)
            raise DriverLaunchException('Error starting driver process')

        log.debug("driver process started, pid: %s" % self.getpid())

        self._command_port = self._get_port_from_file(self._driver_command_port_file())
        self._event_port = self._get_port_from_file(self._driver_event_port_file())

        log.debug("-- command port: %s, event port: %s" % (self._command_port, self._event_port))

    def poll(self):
        """
        Check to see if the driver process is alive.
        @return true if driver process is running, false otherwise
        """

        # The Popen.poll() doesn't seem to be returning reliable results.  Sending a signal 0 to the process might be
        # more reliable.

        if not self._driver_process:
            return False

        try:
            os.kill(self._driver_process.pid, 0)
        except OSError, e:
            log.warn("Could not send a signal to the driver, pid: %s" % self._driver_process.pid)
            return False

        return True




    def stop(self):
        """
        Stop the driver process.  We try to stop gracefully using the driver client if we can, otherwise a simple kill
        does the job.
        """
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
        """
        Get the pid of the current running process and ensure that it is running.
        @returns the pid of the driver process if it is running, otherwise None
        """
        if self._driver_process:
            if self.poll():
                return self._driver_process.pid
            else:
                log.warn("Driver process found, but poll failed for pid %s" % self._driver_process.pid)
        else:
            return None

    def memory_usage(self):
        """
        Get the current memory usage for the current driver process.
        @returns memory usage in KB of the current driver process
        """
        driver_pid = self.getpid()
        if not driver_pid:
            log.warn("no process running")
            return 0

        #ps_process = subprocess.Popen(["ps", "-p", self.getpid(), "-o", "rss,pid"])
        ps_process = subprocess.Popen(["ps", "-o rss,pid", "-p %s" % self.getpid()], stdout=subprocess.PIPE)
        retcode = ps_process.poll()

        usage = 0
        for line in ps_process.stdout:
            if not line.strip().startswith('RSS'):
                try:
                    fields = line.split()
                    pid = int(fields[1])
                    if pid == driver_pid:
                        usage = int(fields[0])
                except:
                    log.warn("Failed to parse output for memory usage: %s" % line)
                    usage = 0

        if usage:
            log.info("process memory usage: %dk" % usage)
        else:
            log.warn("process not running")

        return usage

    def _spawn(self, spawnargs):
        """
        Launch a process using popen
        @param spawnargs a list of arguments for the Popen command line.  The first argument must be a path to a
                         program and arguments much be in additional list elements.
        @returns subprocess.Popen object
        """
        log.debug("run cmd: %s" % " ".join(spawnargs))
        return subprocess.Popen(spawnargs, close_fds=True)


    def _process_command(self):
        """
        Define the command that is sent to _spawn.  This will be specific to each driver process type
        """
        raise DriverLaunchException("_process_command must be overloaded")

    def _get_port_from_file(self, filename):
        """
        Read the driver port from a status file.  The driver process writes two status files containing the port
        number for events and commands.  Currently it reads endlessly until the files is successfully opened, but
        we may want to raise an exception with a timeout.
        @param filename path to the port file
        @return port port number read from the file
        """
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
        """
        read the driver config and get the work directory.
        @returns path to the work directory, default /tmp
        """
        return self.config.get('workdir', '/tmp')

    def _driver_command_port_file(self):
        """
        Generate a uniq filename for the command port file.
        @returns path to a command port file
        """
        if not self._driver_command_file:
            tag = str(uuid.uuid4())
            self._driver_command_file = os.path.join(self._driver_workdir(), 'dvr_cmd_port_%s.txt' % tag)

        return self._driver_command_file

    def _driver_event_port_file(self):
        """
        Generate a uniq filename for the event port file.
        @returns path to a event port file
        """
        if not self._driver_event_file:
            tag = str(uuid.uuid4())
            self._driver_event_file = os.path.join(self._driver_workdir(), 'dvr_evt_port_%s.txt' % tag)

        return self._driver_event_file


class ZMQPyClassDriverProcess(DriverProcess):
    """
    Object to facilitate ZMQ driver processes using a python class and module path.

    Driver config requirements:
    dvr_mod :: the python module that defines the driver class
    dvr_cls :: the driver class defined in the module

    Example:

    driver_config = {
        dvr_mod: mi.instrument.seabird.sbe37smb.ooicore.driver
        dvr_cls: SBE37Driver

        process_type: DriverProcessType.PYTHON_MODULE
    }
    @param driver_config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    """
    def __init__(self, driver_config, test_mode = False):
        self.config = driver_config
        self.test_mode = test_mode

    def _process_command(self):
        """
        Build the process command line using the driver_config dict
        @return a list containing spawn args for the _spawn method
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
        """
        Get a python client for the driver process.
        @return an client object for the driver process
        """
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
        Construct packet factories from packet_config member of the driver_config.
        @retval a list of packet factories defined.
        """
        if not self._packet_factories:
            log.info("generating packet factories")
            self._packet_factories = {}

            driver_module = self.config.get('dvr_mod')
            if not driver_module:
                raise DriverLaunchException("missing driver config: driver_module")

            # Should we poll the driver process to give us these configurations?  If defined in an interface then
            # this method could be generalized for all driver processes.  It also seems like execing an import
            # might be unsafe.
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
    Object to facilitate driver processes launch from an egg as an 'eggsecutable'
    """
    def __init__(self):
        pass
