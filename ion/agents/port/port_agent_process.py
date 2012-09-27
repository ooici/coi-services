#!/usr/bin/env python

"""
@package ion.agents.instrument.port_agent_process
@file ion/agents.instrument/driver_launcher.py
@author Bill French
@brief Port agent process class that provides a factory for different launch mechanisms

USAGE:

config = {
    device_host : 'localhost',
    device_port : '4001'

    type : PortAgentType.ETHERNET,
    process_type : PortAgentProcessType.PYTHON,
}

# These lines can also be run as one command, launch_process
process = PortAgentType.get_process(config)
process.launch()

# alternative launch
process = launch_process(config)

pid = process.get_pid()
cmd_port = process.get_command_port()
data_port = process.get_data_port()
if(process.poll()):
    process.stop()

"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import time
import signal
import gevent
import tempfile
import subprocess

from pyon.util.log import log
from ion.agents.instrument.common import BaseEnum

from ion.agents.port.logger_process import EthernetDeviceLogger
from ion.agents.port.exceptions import PortAgentLaunchException
from ion.agents.port.exceptions import NotImplementedException
from ion.agents.port.exceptions import PortAgentTimeout
from ion.agents.port.exceptions import PortAgentMissingConfig

PYTHON_PATH = 'bin/python'
UNIX_PROCESS = 'port_agent'
DEFAULT_TIMEOUT = 60
PROCESS_BASE_DIR = '/tmp'
PID_FILE = "%s/port_agent_%d.pid"

class PortAgentProcessType(BaseEnum):
    """
    Defines the process types for the port agent.  i.e. C++ or Python
    """
    PYTHON = 'PYTHON',
    UNIX = 'UNIX',

class PortAgentType(BaseEnum):
    """
    What type of port agent are we running?  ethernet, serial, digi etc...
    """
    ETHERNET = 'ethernet',


class PortAgentProcess(object):
    """
    Base class for port agent process launcher
    """
    _command_port = None
    _data_port = None
    _pid = None

    def __init__(self, config, timeout = DEFAULT_TIMEOUT, test_mode = False):
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

    @classmethod
    def get_process(cls, config, timeout = DEFAULT_TIMEOUT, test_mode = False):
        """
        factory class to return the correct PortAgentProcess type based on the config.
        config must contain process_type and type.  Currently both of these default
        to python and ethernet respectively because we only have one type of port agent

        could use variable length parameter lists (**kwargs) here, but I am following the
        same pattern the initial port agent used for passing in configurations.

        @param config dictionary containing configuration information for the port agent.
        @param timeout timeout for port agent launch.  If exceeded an exception is raised
        @param test_mode enable test mode for the port agent
        """
        
        # Default to unix port agent
        process_type = config.get("process_type", PortAgentProcessType.UNIX)

        if process_type == PortAgentProcessType.PYTHON:
            return PythonPortAgentProcess(config, timeout, test_mode)

        if process_type == PortAgentProcessType.UNIX:
            return UnixPortAgentProcess(config, timeout, test_mode)

        else:
            raise PortAgentLaunchException("unknown port agent process type: %s" % process_type)

    @classmethod
    def launch_process(cls, config, timeout = DEFAULT_TIMEOUT, test_mode = False):
        """
        Just like the get_process factory method except we call launch with the new object.

        @param config dictionary containing configuration information for the port agent.
        @param timeout timeout for port agent launch.  If exceeded an exception is raised
        @param test_mode enable test mode for the port agent
        """
        process = cls.get_process(config, timeout, test_mode);
        process.launch()
        return process

    def launch(self):
        """
        Launch the port agent process. Must be overloaded.
        @raises NotImplementedException
        """
        raise NotImplementedException('launch()')


    def poll(self):
        """
        Check to see if the port agent process is alive.
        @return true if process is running, false otherwise
        """

        if not self._pid:
            return False

        try:
            os.kill(self._pid, 0)
        except OSError, e:
            log.warn("Could not send a signal to the driver, pid: %s" % self._pid)
            return False

        return True

    def stop(self):
        """
        Stop the driver process.  We just send a signal to a process.  We may be able to overload this to do something
        more graceful.
        """
        pid = self.get_pid()
        if pid:
            os.kill(pid, signal.SIGTERM)

    def get_pid(self):
        """
        Get the pid of the current running process and ensure that it is running.
        @returns the pid of the driver process if it is running, otherwise None
        """
        if self.poll():
            return self._pid
        else:
            return None

    def get_command_port(self):
        """
        Get the command port for the port agent process
        @returns port number
        """
        return self._command_port

    def get_data_port(self):
        """
        Get the data port for the port agent process
        @returns port number
        """
        return self._data_port


class PythonPortAgentProcess(PortAgentProcess):
    """
    Object to facilitate launching port agent processes using a python class and module path.

    Port Agent config requirements:
    dvr_mod :: the python module that defines the driver class
    dvr_cls :: the driver class defined in the module

    Example:

    port_agent_config = {
        device_addr: mi.instrument.seabird.sbe37smb.ooicore.driver
        device_port: SBE37Driver

        working_dir = "/tmp/"
        delimiter = ['<<','>>']

        type: PortAgentType.ETHERNET
    }
    @param config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    """

    _port_agent = None

    def __init__(self, config, timeout = DEFAULT_TIMEOUT, test_mode = False):
        """
        Initialize the Python port agent object using the passed in config.  This
        defaults to ethernet as the type because that is currently the only port
        agent we have.
        @raises PortAgentMissingConfig
        """
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

        # Verify our configuration is correct

        self._device_addr = config.get("device_addr")
        self._device_port = config.get("device_port")
        self._working_dir = config.get("working_dir", '/tmp/')
        self._delimiter = config.get("delimiter", ['<<','>>'])
        self._type = config.get("type", PortAgentType.ETHERNET)

        if not self._device_addr:
            raise PortAgentMissingConfig("missing config: device_addr")

        if not self._device_port:
            raise PortAgentMissingConfig("missing config: device_port")

        if not self._type == PortAgentType.ETHERNET:
            raise PortAgentLaunchException("unknown port agent type: %s" % self._type)

    def launch(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @retval return the pid to the logger process
        """
        log.info("Startup Port Agent")
        # Create port agent object.
        this_pid = os.getpid() if self._test_mode else None

        log.debug( " -- our pid: %s" % this_pid)
        log.debug( " -- address: %s, port: %s" % (self._device_addr, self._device_port))

        # Working dir and delim are hard coded here because this launch process
        # will change with the new port agent.
        self.port_agent = EthernetDeviceLogger.launch_process(
            self._device_addr,
            self._device_port,
            self._working_dir,
            self._delimiter,
            this_pid)


        log.debug( " Port agent object created" )

        start_time = time.time()
        expire_time = start_time + int(self._timeout)
        pid = self.port_agent.get_pid()
        while not pid:
            gevent.sleep(.1)
            pid = self.port_agent.get_pid()
            if time.time() > expire_time:
                log.error("!!!! Failed to start Port Agent !!!!")
                raise PortAgentTimeout('port agent could not be started')
        self._pid = pid

        port = self.port_agent.get_port()

        start_time = time.time()
        expire_time = start_time + int(self._timeout)
        while not port:
            gevent.sleep(.1)
            port = self.port_agent.get_port()
            if time.time() > expire_time:
                log.error("!!!! Port Agent could not bind to port !!!!")
                self.stop()
                raise PortAgentTimeout('port agent could not bind to port')
        self._data_port = port

        log.info('Started port agent pid %s listening at port %s' % (pid, port))
        return port

    def stop(self):
        if self.port_agent:
            pid = self.port_agent.get_pid()

        if pid:
            log.info('Stopping pagent pid %i' % pid)
            self.port_agent.stop()
        else:
            log.info('No port agent running.')
            
class UnixPortAgentProcess(PortAgentProcess):
    """
    Object to facilitate launching port agent processes using a c++ compiled port agent

    Port Agent config requirements:
    binary_path :: Path the the port agent executable
    command_port :: port number of the observatory command port to the port agent
    log_level :: how many -v options to add to the launch

    Example:

    port_agent_config = {
        device_addr: sbe37-simulator.oceanobservatories.org,
        device_port: 4001,
        
        binary_path: /bin/port_agent,
        command_port: 4000,
        data_port: 4002,
        log_level: 5,

        type: PortAgentType.ETHERNET
    }
    @param config configuration parameters for the driver process
    @param test_mode should the driver be run in test mode
    """

    _port_agent = None

    def __init__(self, config, timeout = DEFAULT_TIMEOUT, test_mode = False):
        """
        Initialize the Python port agent object using the passed in config.  This
        defaults to ethernet as the type because that is currently the only port
        agent we have.
        @raises PortAgentMissingConfig
        """
        self._config = config
        self._timeout = timeout
        self._test_mode = test_mode

        # Verify our configuration is correct

        self._device_addr = config.get("device_addr")
        self._device_port = config.get("device_port")
        self._binary_path = config.get("binary_path", "port_agent")
        self._command_port = config.get("command_port");
        self._data_port = config.get("data_port");
        self._log_level = config.get("log_level");
        self._type = config.get("type", PortAgentType.ETHERNET)

        if not self._device_addr:
            raise PortAgentMissingConfig("missing config: device_addr")

        if not self._device_port:
            raise PortAgentMissingConfig("missing config: device_port")

        if not self._command_port:
            raise PortAgentMissingConfig("missing config: command_port")

        if not self._data_port:
            raise PortAgentMissingConfig("missing config: data_port")

        if not self._binary_path:
            raise PortAgentMissingConfig("missing config: binary_path")

        if not self._type == PortAgentType.ETHERNET:
            raise PortAgentLaunchException("unknown port agent type: %s" % self._type)
        
        self._tmp_config = self.get_config();
        
    def get_config(self):
        """
        @brief Write a configuration file for the port agent to read.
        @ret NamedTemporaryFile object to the config file.
        """
        
        temp = tempfile.NamedTemporaryFile()
        temp.write("\n")
        temp.write("log_dir %s\n" % (PROCESS_BASE_DIR))
        temp.write("pid_dir %s\n" % (PROCESS_BASE_DIR))
        temp.write("data_dir %s\n" % (PROCESS_BASE_DIR))
        temp.write("instrument_type tcp\n")
        temp.write("instrument_data_port %d\n" % (self._device_port) )
        temp.write("instrument_addr %s\n" % (self._device_addr) )
        temp.write("data_port %d\n" % (self._data_port) )
        temp.flush()
        
        return temp;
        

    def launch(self):
        """
        @brief Launch the port agent process.
        @retval return the command port the process is listening on.
        """
        log.info("Startup Unix Port Agent")
        # Create port agent object.
        this_pid = os.getpid() if self._test_mode else None

        log.debug( " -- our pid: %s" % this_pid)
        log.debug( " -- command port: %s" % (self._command_port))
        log.debug( " -- address: %s, port: %s" % (self._device_addr, self._device_port))

        command_line = [ self._binary_path ]
        
        if(self._log_level > 0):
            for num in range(1, self._log_level):
                command_line.append("-v");

            
        if(self._test_mode):
            command_line.append("-s")

        command_line.append("-p")
        command_line.append("%s" % (self._command_port));

        command_line.append("-c")
        command_line.append(self._tmp_config.name);


        self.run_command(command_line);
        self._pid = self._read_pid();

        return self._command_port;
    
    def run_command(self, command_line):
        log.debug("run command: " + str(command_line));
        process = subprocess.Popen(command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE);
        gevent.sleep(1);

        process.poll()

        # We have failed!
        if(process.returncode and process.pid):
            output, error_message = process.communicate();
            log.error("Failed to run command: STDERR: %s" % (error_message))
            raise PortAgentLaunchException("failed to launch port agent");

        log.debug("command successful.  pid: %d" % (process.pid))
        
        return process.pid;

    def _read_pid(self):
        pid_file = PID_FILE % (PROCESS_BASE_DIR, self._command_port)
        start_time = time.time()
        boo = 0;

        log.debug("read pid file: " + pid_file)
        while(start_time + DEFAULT_TIMEOUT > time.time()):
            try:
                file = open(pid_file)
                pid = file.read().strip('\0\n\r')
                if(pid):
                    int(pid)
                    log.info("port agent pid: [%s]" % (pid))
                    return int(pid)
            except ValueError, e:
                log.warn("Failed to convert %s to an int '%s" % (pid, e) )
                break
            except:
                log.warn("Failed to open pid file: %s" % (pid_file))
                gevent.sleep(1);

        log.error("port agent startup failed");

        return None;

    def _read_config(self):
        self._tmp_config.seek(0);
        return "".join(self._tmp_config.readlines());
        

    def stop(self):
        log.info('Stop port agent')
        
        command_line = [ self._binary_path ]

        command_line.append("-c")
        command_line.append(self._tmp_config.name);

        command_line.append("-k")
        
        command_line.append("-p")
        command_line.append("%s" % (self._command_port));
        
        self.run_command(command_line);
