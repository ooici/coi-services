#!/usr/bin/env python

"""
@package ion.agents.instrument.driver_launcher
@file ion/agents.instrument/driver_launcher.py
@author Bill French
@brief Drive process class that provides a factory for different launch mechanisms
"""
from pyon.core.exception import ServerError

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import sys
import uuid
import time
import signal
import subprocess

from urllib2 import Request, urlopen, URLError, HTTPError
from pyon.util.log import log
from ion.agents.instrument.common import BaseEnum

from ion.agents.instrument.exceptions import DriverLaunchException
from ion.agents.instrument.exceptions import NotImplementedException
from ion.agents.instrument.packet_factory_man import create_packet_builder
from ion.agents.instrument.driver_client import ZmqDriverClient

PYTHON_PATH = 'bin/python'
CACHE_DIR = '/tmp'
REPO_BASE = 'http://sddevrepo.oceanobservatories.org/releases'

class DriverProcessType(BaseEnum):
    """
    Base states for driver launcher types.
    """
    PYTHON_MODULE = 'ZMQPyClassDriverLauncher'
    EGG = 'ZMQEggDriverLauncher'


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
        type = driver_config.get("process_type")[0]
        driver_module = driver_config.get('dvr_mod')

        if not type:
            raise DriverLaunchException("missing driver config: process_type")

        # For some reason the enum wasn't working with the instrument agent.  I would see [emum] when called from
        # the IA, but (enum,) when called from this module.
        #
        #
        elif type == DriverProcessType.PYTHON_MODULE:
            log.debug("DriverProcessType.PYTHON_MODULE")
            return ZMQPyClassDriverProcess(driver_config, test_mode)

        elif type == DriverProcessType.EGG:
            log.debug("DriverProcessType.EGG")
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
            log.error("Failed to launch driver: %s", cmd)
            raise DriverLaunchException('Error starting driver process')

        log.debug("driver process started, pid: %s", self.getpid())

        self._command_port = self._get_port_from_file(self._driver_command_port_file())
        self._event_port = self._get_port_from_file(self._driver_event_port_file())

        log.debug("-- command port: %s, event port: %s", self._command_port, self._event_port)

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

    def stop(self, force=False):
        """
        Stop the driver process.  We try to stop gracefully using the driver client if we can, otherwise a simple kill
        does the job.
        """

        if self._driver_process:

            if not force and self._driver_client:
                try:
                    log.info('Stopping driver process.') 
                    self._driver_client.done()
                    self._driver_process.wait()
                    log.info('Driver process stopped.')
                except:
                    try:
                        log.error('Exception stopping driver process...killing')
                        print '## polling'
                        self._driver_process.poll()
                        if not self._driver_process.returncode:
                            print '## poll return code is: ' + str(self._driver_process.returncode)
                            print '## killing'
                            self._driver_process.kill()
                            self._driver_process.wait()
                        print '## stopping'
                        #self._driver_client.stop_messaging()
                        #self._driver_client.stop_event_thread = True
                        #self._driver_client.event_thread.kill()
                        #self._driver_client.event_thread.join()
                        log.error('Driver process killed.')
                    except Exception as ex:
                        log.error('Exception killing driver process')
                        log.error(type(ex))
                        log.error(ex)

            else:
                try:
                    log.info('Killing driver process.')
                    print '## polling'
                    self._driver_process.poll()
                    if  not self._driver_process.returncode:
                        print '## poll return code is: ' + str(self._driver_process.returncode)
                        print '## killing'
                        self._driver_process.kill()
                        self._driver_process.wait()
                    print '## stopping'
                    self._driver_client.stop_messaging()
                    #self._driver_client.stop_event_thread = True
                    #self._driver_client.event_thread.kill()
                    #self._driver_client.event_thread.join()
                    log.info('Driver process killed.')
                except Exception as ex:
                    log.error('Exception killing driver process')
                    log.error(type(ex))
                    log.error(ex)

        self._driver_process = None
        self._driver_client = None

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
        log.debug("run cmd: %s", " ".join(spawnargs))
        return subprocess.Popen(spawnargs, close_fds=True)


    def _process_command(self):
        """
        Define the command that is sent to _spawn.  This will be specific to each driver process type
        """
        raise DriverLaunchException("_process_command must be overloaded")

    def _get_port_from_file(self, filename):
        """
        Read the driver port from a status file.  The driver process writes two status files containing the port
        number for events and commands.
        @param filename path to the port file
        @return port port number read from the file
        @raise ServerError if file not read w/in 10sec
        """
        maxWait=10          # try for up to 10sec
        waitInterval=0.5    # repeating every 1/2 sec
        log.debug("about to read port from file %s", filename)
        for n in xrange(int(maxWait/waitInterval)):
            try:
                with open(filename, 'r') as f:
                    port = int(f.read().strip())
                    log.debug("read port %d from file %s", port, filename)
                    return port
            except: pass
            time.sleep(waitInterval)
        raise ServerError('process PID file was not found: ' + filename)

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

    def get_client(self):
        """
        Get a python client for the driver process.
        @return an client object for the driver process
        """
        # Start client messaging and verify messaging.
        if not self._driver_client:
            try:
                driver_client = ZmqDriverClient('localhost', self._command_port, self._event_port)
                self._driver_client = driver_client
            except Exception, e:
                self.stop()
                log.error('Error starting driver client: %s', e)
                raise DriverLaunchException('Error starting driver client.')

        return self._driver_client

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
        log.debug("cwd: %s", os.getcwd())
        mi_repo = self.config.get('mi_repo', None)
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
        cmd_str = ''
        if mi_repo:
            cmd_str += 'import sys; sys.path.insert(0,"%s");' % mi_repo
        cmd_str += 'from %s import %s; dp = %s("%s", "%s", "%s", "%s", %s);dp.run()'\
        % ('mi.core.instrument.zmq_driver_process', 'ZmqDriverProcess', 'ZmqDriverProcess', driver_module,
           driver_class, cmd_port_fname, evt_port_fname, str(ppid))

        return [ python, '-c', cmd_str ]


class ZMQEggDriverProcess(DriverProcess):
    '''
    Object to facilitate ZMQ driver processes using a python egg

    Driver config requirements:
    dvr_egg :: the filename of the egg

    Example:

    driver_config = {
        dvr_egg: seabird_sbe37smb_ooicore-0.0.1-py2.7.egg

        process_type: DriverProcessType.EGG
    }
    @param driver_config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    '''
    def __init__(self, driver_config, test_mode = False):
        self.config = driver_config
        self.test_mode = test_mode

    def _egg_path(self, egg_name):
        log.debug("_egg_path")
        if not CACHE_DIR:
            raise ServerError("CACHE_DIR is %s'%s'" % (type(CACHE_DIR), CACHE_DIR))
        elif not egg_name:
            raise ServerError("egg_name for path is %s'%s'" % (type(egg_name), egg_name))

        return CACHE_DIR + "/" + egg_name

    def _egg_remotepath(self, egg_name):
        log.debug("_egg_remotepath: %s", egg_name)
        if not REPO_BASE:
            raise ServerError("REPO_BASE is %s'%s'" % (type(REPO_BASE), REPO_BASE))
        elif not egg_name:
            raise ServerError("egg_name for remotepath is %s'%s'" % (type(egg_name), egg_name))

        return REPO_BASE + "/" + egg_name

    def _check_cache_for_egg(self, egg_name):
        """
        Check if the egg is already cached, if so, return the path.
        @return: egg path if cached, else None
        """
        log.debug("_check_cache_for_egg: %s", egg_name)
        path = self._egg_path(egg_name)
        if os.path.exists(path):
            log.debug("_check_cache_for_egg cache hit PATH = " + str(path))
            return path
        else:
            log.debug("_check_cache_for_egg cache miss" + str(path))
            return None

    def _get_remote_egg(self, egg_name):
        """
        pull the egg from a remote server if present to the local cache dir.
        @return: returns the path, throws exception if not found.
        """
        log.debug("_get_remote_egg %s", egg_name)
        try:
            if egg_name.startswith("http://"):
                response = urlopen(egg_name)
            else:
                response = urlopen(self._egg_remotepath(egg_name))
            egg_yolk = response.read()
            log.debug("_fetch_egg GOT YOLK")
        except HTTPError, e:
            raise DriverLaunchException('failed to download egg: ' + str(e))
        except URLError, e:
            raise DriverLaunchException('bad egg URL ' + egg_name + ': ' + e.reason)
        
        filename = get_filename_from_uri(egg_name)
        path = self._egg_path(filename)
        log.debug("PATH = %s", path)
        try:
            egg_file = open(path, "wb")
            egg_file.write(egg_yolk)
        except IOError:
            raise DriverLaunchException("IOError writing egg file to cache")
        else:
            egg_file.close()

        return path

    def _get_egg(self, egg_uri):
        filename = get_filename_from_uri(egg_uri)
        log.debug("_get_egg: %s",filename)
        path = self._check_cache_for_egg(filename)
        if None == path:
            path = self._get_remote_egg(filename) # Will exception out if problem.

        return path

    def _process_command(self):
        """
        Build the process command line using the driver_config dict
        @return a list containing spawn args for the _spawn method

        1. check cache (CACHE_DIR = '/tmp')
        2. download to cache if not present.
        3. construct call command
        """

        log.debug("cwd: %s", os.getcwd())
        driver_package = self.config.get('dvr_egg')
        ppid = os.getpid() if self.test_mode else None

        python = PYTHON_PATH

        if not driver_package:
            raise DriverLaunchException("missing driver config: driver_package")
        if not os.path.exists(python):
            raise DriverLaunchException("could not find python executable: %s" % python)

        path = self._get_egg(self.config.get('dvr_egg'))
        log.debug("_process_command: %s", path)

        cmd_port_fname = self._driver_command_port_file()
        evt_port_fname = self._driver_event_port_file()

        cmd_str = "import sys; sys.path.insert(0, '%s/%s'); from mi.main import run; sys.exit(run(command_port_file='%s', event_port_file='%s', ppid=%s))" % \
                  (CACHE_DIR, get_filename_from_uri(driver_package), cmd_port_fname, evt_port_fname, str(ppid))

        return [ python, '-c', cmd_str ]

def get_filename_from_uri(uri):
    (base, seperator, filename) = uri.rpartition('/')
    return filename
