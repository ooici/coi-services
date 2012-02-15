#!/usr/bin/env python

'''
@file ion/services/mi/drivers/test/test_satlantic_par.py
@author Steve Foley
@test ion.services.mi.drivers.satlantic_par
Unit test suite to test Satlantic PAR sensor
'''

import unittest
import logging
from mock import Mock, call, DEFAULT
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from ion.services.mi.common import InstErrorCode
#import ion.services.mi.drivers.satlantic_par.satlantic_par
from ion.services.mi.data_decorator import ChecksumDecorator
from ion.services.mi.drivers.satlantic_par.satlantic_par import SatlanticPARInstrumentProtocol
from ion.services.mi.drivers.satlantic_par.satlantic_par import Parameter
from ion.services.mi.drivers.satlantic_par.satlantic_par import Command
from ion.services.mi.drivers.satlantic_par.satlantic_par import Event
from ion.services.mi.drivers.satlantic_par.satlantic_par import SatlanticChecksumDecorator
from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentDataException

mi_logger = logging.getLogger('mi_logger')

# Make tests verbose and provide stdout
# bin/nosetests -s -v ion/services/mi/drivers/test/test_satlantic_par.py
# Test device is at 10.180.80.173, port 2001

@attr('UNIT', group='mi')
class SatlanticParProtocolUnitTest(PyonTestCase):
    """
    @todo test timeout exceptions while transitioning states and handling commands
    """
    
    def setUp(self):
        def response_side_effect(*args, **kwargs):
            if args[0] == Command.SAMPLE:
                mi_logger.debug("*** side effecting!")
                return "SATPAR0229,10.01,2206748544,234"
            else:
                return DEFAULT
            
        self.mock_callback = Mock(name='callback')
        self.mock_logger = Mock(name='logger')
        self.mock_logger_client = Mock(name='logger_client')
#        self.mock_logger_client.send = Mock()
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
        self.mock_logger_client.reset_mock()
    
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
        self.mock_logger_client.reset_mock()
        result = self.par_proto.get([])
        self.assertEquals(result, {})
        self.assertEquals(self.mock_logger_client.send.call_count, 0)
        
        # try bad param
        self.assertRaises(InstrumentProtocolException, self.par_proto.get, None)
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
        self.assertEquals(self.mock_logger_client.send.call_count, 2)

        # try empty set
        self.mock_logger_client.reset_mock()
        result = self.par_proto.set({})
        self.assertEquals(self.mock_logger_client.send.call_count, 0)
        self.assertEquals(result, {})
        
        # some error cases
        self.assertRaises(InstrumentProtocolException, self.par_proto.set, [])
        self.assertRaises(InstrumentProtocolException, self.par_proto.set, None)
        self.assertRaises(InstrumentProtocolException, self.par_proto.set, ['foo'])
        
        # try bad param
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.set,{'bad_param':0})
    
    def test_get_config(self):
        fetched_config = {}
        fetched_config = self.par_proto.get_config()
        self.assert_(isinstance(fetched_config, dict))
        calls = [call("show %s\n" % Parameter.TELBAUD),
                 call("show %s\n" % Parameter.MAXRATE)]
        self.mock_logger_client.send.assert_has_calls(calls, any_order=True)
        
        self.assertEquals(len(fetched_config), 2)
        self.assertTrue(fetched_config.has_key(Parameter.TELBAUD))
        self.assertTrue(fetched_config.has_key(Parameter.MAXRATE))
    
    def test_restore_config(self):
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.restore_config, None)    
     
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.restore_config, {})
        
        self.assertRaises(InstrumentProtocolException,
                          self.par_proto.restore_config, {'bad_param':0})

        test_config = {Parameter.TELBAUD:19200, Parameter.MAXRATE:2}
        restore_result = self.par_proto.restore_config(test_config)
        calls = [call("set %s %s\n" % (Parameter.TELBAUD, 19200)),
                 call("set %s %s\n" % (Parameter.MAXRATE, 2))]
        self.mock_logger_client.send.assert_has_calls(calls, any_order=True)
        
    def test_get_single_value(self):
        result = self.par_proto.execute_poll()
        calls = [call("%s\n" % Command.EXIT),
                 call(Command.STOP),
                 call(Command.SAMPLE),
                 call(Command.AUTOSAMPLE),
                 call(Command.BREAK)]
        self.mock_logger_client.send.assert_has_calls(calls, any_order=False)
        
    def test_breaks(self):
        # test kick to autosample, then back
        result = self.par_proto.execute_exit()        
        self.mock_callback.reset_mock()
        result = self.par_proto.execute_break()
        self.mock_logger_client.send.assert_called_with(Command.BREAK)        
        self.assertEqual(self.mock_callback.call_count, 1)

        # test autosample to poll change
        result = self.par_proto.execute_exit()
        self.mock_callback.reset_mock()
        result = self.par_proto.execute_stop()
        self.mock_logger_client.send.assert_called_with(Command.STOP)
        self.assertEqual(self.mock_callback.call_count, 1)

        result = self.par_proto.execute_autosample()
        self.mock_callback.reset_mock()
        result = self.par_proto.execute_reset()
        self.mock_logger_client.send.assert_called_with(Command.RESET)
        self.assertEqual(self.mock_callback.call_count, 1)
  
    def test_got_data(self):
        # Cant trigger easily since the async command/response, so short circut
        # the test early.
        self.mock_callback.reset_mock()
        result = self.par_proto.execute_exit()
        self.assertEqual(self.mock_callback.call_count, 1)
        self.assert_(result)
        self.mock_callback.reset_mock()
        result = self.par_proto._got_data("SATPAR0229,10.01,2206748544,234\n")
        # check for publish
        self.assertEqual(self.mock_callback.call_count, 2)

        # check for correct parse
        
    
    def test_save(self):
        result = self.par_proto.execute_save()
        self.assertNotEqual(result, None)
        self.mock_logger_client.send.assert_called_with("%s\n" % Command.SAVE)
    
    def test_connect_disconnect(self):
        pass
    
    def test_get_status(self):
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
        # test get from wrong state (auto sample?)
        pass
    
    def test_set(self):
        # test set from wrong state (autosample?)
        pass
    
    def test_get_config(self):
        # test from wrong state
        pass
    
    def test_restore_config(self):
        # test from wrong state
        pass
    
    def test_reset(self):
        pass
    
    def test_commands(self):
        pass
    
    def got_data(self):
        pass
    
class SatlanticParDecoratorTest(PyonTestCase):
    
    def setUp(self):
        self.checksum_decorator = SatlanticChecksumDecorator()
    
    def test_checksum(self):
        self.assertEquals(("SATPAR0229,10.01,2206748544,234","SATPAR0229,10.01,2206748544,234"),
            self.checksum_decorator.handle_incoming_data("SATPAR0229,10.01,2206748544,234","SATPAR0229,10.01,2206748544,234"))
        self.assertRaises(InstrumentDataException,
                          self.checksum_decorator.handle_incoming_data,
                          "SATPAR0229,10.01,2206748544,235",
                          "SATPAR0229,10.01,2206748544,235")                        
  