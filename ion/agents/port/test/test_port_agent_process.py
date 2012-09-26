#!/usr/bin/env python

"""
@package ion.agents.port.test.test_port_agent_process
@file ion/agents/port/test_port_agent_process.py
@author Bill French
@brief Test cases for DriverProcess.
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import unittest

from nose.plugins.attrib import attr
from ion.agents.port.port_agent_process import PortAgentProcess, PythonPortAgentProcess, UnixPortAgentProcess, PortAgentProcessType
from ion.agents.port.exceptions import PortAgentTimeout
from ion.agents.port.exceptions import PortAgentMissingConfig
from ion.agents.port.exceptions import PortAgentLaunchException

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/agents/port/test/test_port_agent_process.py

TEST_TIMEOUT = 2

@attr('HARDWARE', group='mi')
class TestPythonEthernetProcess(unittest.TestCase):
    """
    Unit tests for the Port Agent Process using python classes
    """
    def setUp(self):
        """
        Setup test cases.
        """
        self._port_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4001,

            'process_type': PortAgentProcessType.PYTHON
        }

    def test_driver_process(self):
        """
        Test port agent process launch with default values and a good host and port
        """
        process = PortAgentProcess.get_process(self._port_config, test_mode=True)
        self.assertTrue(process)
        self.assertTrue(isinstance(process, PythonPortAgentProcess))

        # Verify config
        self.assertEqual(process._device_addr, self._port_config.get("device_addr"))
        self.assertEqual(process._device_port, self._port_config.get("device_port"))
        self.assertEqual(process._working_dir, '/tmp/')
        self.assertEqual(process._delimiter, ['<<','>>'])

        # Try start
        process.launch()

        # Check that it launched properly
        self.assertTrue(process.get_pid() > 0)
        self.assertTrue(process.get_data_port())
        # Python logger has no command port
        self.assertEqual(process.get_command_port(), None)

        process.stop()
        self.assertFalse(process.get_pid())

    def test_driver_launch(self):
        """
        Test the alternate method for launching a port agent
        """
        process = PortAgentProcess.launch_process(self._port_config, test_mode=True)

        # Check that it launched properly
        self.assertTrue(process.get_pid() > 0)
        self.assertTrue(process.get_data_port())
        # Python logger has no command port
        self.assertEqual(process.get_command_port(), None)

        process.stop()
        self.assertFalse(process.get_pid())


    def test_invalid_process_type(self):
        """
        Test with a bogus process type. Should raise an exception
        """
        process = None
        config = self._port_config
        config['process_type'] = 'foo'
        with self.assertRaises(PortAgentLaunchException) as exp:
            process = PortAgentProcess.get_process(config, timeout = TEST_TIMEOUT, test_mode=True)
        self.assertRegexpMatches(str(exp.exception), '.*unknown port agent process type:.*', msg='exception message was ' + str(exp.exception))


    def test_invalid_type(self):
        """
        Test with a bogus port agent type. Should raise an exception
        """
        process = None
        config = self._port_config
        config['type'] = 'foo'
        with self.assertRaises(PortAgentLaunchException) as exp:
            process = PortAgentProcess.get_process(config, timeout = TEST_TIMEOUT, test_mode=True)
        self.assertRegexpMatches(str(exp.exception), 'unknown port agent type:')


    def test_missing_config(self):
        """
        Test if a required config parameter is missing
        """
        config = {'device_addr': 'localhost'}

        with self.assertRaises(PortAgentMissingConfig):
            process = PortAgentProcess.get_process(config, timeout = TEST_TIMEOUT, test_mode=True)

        config = {'device_port': '921'}

        with self.assertRaises(PortAgentMissingConfig):
            process = PortAgentProcess.get_process(config, timeout = TEST_TIMEOUT, test_mode=True)


    def test_bad_port(self):
        """
        Test the port agent startup with a bad port number.  This should eventually timeout and raise an
        exception.
        """
        port = 9999999999
        config = self._port_config
        config['device_port'] = port

        process = PortAgentProcess.get_process(self._port_config, timeout = TEST_TIMEOUT, test_mode=True)
        self.assertTrue(process)
        self.assertTrue(isinstance(process, PythonPortAgentProcess))

        # Verify config
        self.assertEqual(process._device_addr, self._port_config.get("device_addr"))
        self.assertEqual(process._device_port, port)
        self.assertEqual(process._working_dir, '/tmp/')
        self.assertEqual(process._delimiter, ['<<','>>'])

        # Try start
        with self.assertRaises(PortAgentTimeout):
            process.launch()

        self.assertFalse(process.poll())


    def test_bad_host(self):
        """
        Test the port agent startup with a bad hostname.  This should eventually timeout and raise an
        exception.
        """
        host = '127.0.0.0'
        config = self._port_config
        config['device_addr'] = host

        process = PortAgentProcess.get_process(self._port_config, timeout = TEST_TIMEOUT, test_mode=True)
        self.assertTrue(process)
        self.assertTrue(isinstance(process, PythonPortAgentProcess))

        # Verify config
        self.assertEqual(process._device_addr, host)
        self.assertEqual(process._device_port, self._port_config.get("device_port"))
        self.assertEqual(process._working_dir, '/tmp/')
        self.assertEqual(process._delimiter, ['<<','>>'])

        # Try start
        with self.assertRaises(PortAgentTimeout):
            process.launch()

        # Verify we don't have a process lingering
        self.assertFalse(process.poll())

@attr('HARDWARE', group='mi')
class TestUnixEthernetProcess(unittest.TestCase):
    """
    Unit tests for the Port Agent Process using python classes
    """
    def setUp(self):
        """
        Setup test cases.
        """
        self._port_config = {
            'device_addr': 'sbe37-simulator.oceanobservatories.org',
            'device_port': 4001,
            'process_type': PortAgentProcessType.UNIX,
            
            'binary_path': "port_agent",
            'command_port': 4002,
            'data_port': 4003,
            'log_level': 5,
        }

    def test_driver_process(self):
        """
        Test port agent process launch with default values and a good host and port
        """
        process = PortAgentProcess.get_process(self._port_config, test_mode=True)
        self.assertTrue(process)
        self.assertTrue(isinstance(process, UnixPortAgentProcess))

        # Verify config
        self.assertEqual(process._device_addr, self._port_config.get("device_addr"))
        self.assertEqual(process._device_port, self._port_config.get("device_port"))
        self.assertEqual(process._binary_path, self._port_config.get("binary_path"))
        self.assertEqual(process._command_port, self._port_config.get("command_port"))
        self.assertEqual(process._data_port, self._port_config.get("data_port"))
        self.assertEqual(process._log_level, self._port_config.get("log_level"))

        process.stop()
        
        # Try start
        process.launch()

        # Check that it launched properly
        self.assertTrue(process.get_pid() > 0)
        self.assertTrue(process.get_data_port(), 4003)
        self.assertEqual(process.get_command_port(), 4002)

        process.stop()



