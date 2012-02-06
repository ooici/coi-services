#!/usr/bin/env python

'''
@file ion/services/mi/drivers/test/test_satlantic_par.py
@author Steve Foley
@test ion.services.mi.drivers.satlantic_par
Unit test suite to test Satlantic PAR sensor
'''

import unittest
import logging
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import ion.services.mi.drivers.satlantic_par
from ion.services.mi.drivers.satlantic_par import SatlanticPARInstrumentProtocol
from ion.services.mi.drivers.satlantic_par import Parameter
from ion.services.mi.exceptions import InstrumentProtocolException

mi_logger = logging.getLogger('mi_logger')

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_satlantic_par.py
# Test device is at 10.180.80.173, port 2001

@attr('UNIT', group='mi')
class SatlanticParProtocolUnitTest(PyonTestCase):
    
    def setUp(self):
        self.mock_callback = Mock(name='callback')
        self.mock_logger = Mock(name='logger')
        self.mock_logger_client = Mock(name='logger_client')
        self.par_proto = SatlanticPARInstrumentProtocol(self.mock_callback)
        self.config_params = {'method':'ethernet',
                              'device_addr':'1.1.1.1',
                              'device_port':1,
                              'server_addr':'2.2.2.2',
                              'server_port':2}
        self.par_proto.configure(self.config_params)
        self.par_proto._logger = self.mock_logger 
        self.par_proto._logger_client = self.mock_logger_client
        self.par_proto._get_response = Mock(return_value=('$', None))
        self.par_proto.initialize()
        # Quick sanity check to make sure the logger got mocked properly
        self.assertEquals(self.par_proto._logger, self.mock_logger)
        self.assertEquals(self.par_proto._logger_client, self.mock_logger_client)
    
    def test_get_param(self):
        # try single
        result = self.par_proto.get([Parameter.TELBAUD])
        self.mock_logger_client.send.assert_called_with("show %s\n" %
                                                        Parameter.TELBAUD)
        
        result = self.par_proto.get([Parameter.MAXRATE])
        self.mock_logger_client.send.assert_called_with("show %s\n" %
                                                        Parameter.MAXRATE)
        
        # try group
        result = self.par_proto.get(Parameter.list())
        
        # try empty set
        result = self.par_proto.get([])
        self.assertEquals(result, None)
        result = self.par_proto.get(None)
        self.assertEquals(result, None)
        
        # try bad param
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.get,['bad_param'])
        
    def test_set_param(self):
        #@todo deal with success/fail flag or catch everywhere?
        self.par_proto.set({Parameter.TELBAUD:9600})
        self.mock_logger_client.send.assert_called_with("set %s 9600\n" %
                                                        Parameter.TELBAUD)
        
        self.par_proto.set({Parameter.MAXRATE:10})
        self.mock_logger_client.send.assert_called_with("set %s 10\n" %
                                                        Parameter.MAXRATE)
        
        # try group
        self.mock_logger_client.reset_mock()
        self.par_proto.set({Parameter.TELBAUD:4800,Parameter.MAXRATE:9})
        self.assertEqual(self.mock_logger_client.send.call_count, 2)

        # try empty set
        result = self.par_proto.set({})
        self.assertEquals(result, None)
        result = self.par_proto.set(None)
        self.assertEquals(result, None)
        result = self.par_proto.set([])
        self.assertEquals(result, None)
        result = self.par_proto.set(['something'])
        self.assertEquals(result, None)
        
        # try bad param
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.set,{'bad_param':0})
    
    def test_get_config(self):
        fetched_config = self.par_proto.get_config()
        #assert config is what
        pass
    
    def test_restore_config(self):
        pass
    
    def test_execute_command(self):
        pass
        
    def test_break_to_command(self):
        # do we need this?
        pass
    
    def test_connect_disconnect(self):
        pass
    
    def test_state_change_announcements(self):
        # fire state changes and check for publish_to_driver calls
        # includes resets, breaks, stops, autosample
        pass
    

@attr('INT', group='mi')
class SatlanticParProtocolIntegrationTest(PyonTestCase):
    
    def setUp(self):
        pass        

    def test_break_from_autosample(self):
        pass
        # test break from autosample at low data rates
        # test break from autosample at high data rates
        
    def test_get(self):
        pass
    
    def test_set(self):
        pass
    
    def test_get_config(self):
        pass
    
    def test_restore_config(self):
        pass
    
    def test_reset(self):
        pass
    
    def test_commands(self):
        pass