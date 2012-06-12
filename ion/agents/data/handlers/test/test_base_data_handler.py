#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_base_data_handler
@file ion/agents/data/handlers/test/test_base_data_handler.py
@author Tim Giguere
@author Christopher Mueller
@brief Test cases for BaseDataHandler
"""

from pyon.public import log, CFG
from nose.plugins.attrib import attr
from mock import patch, Mock, call, sentinel, ANY
from pyon.util.unit_test import PyonTestCase
from pyon.core.exception import NotFound
import unittest

from pyon.ion.granule.taxonomy import TaxyTool
from ion.agents.instrument.exceptions import InstrumentParameterException, InstrumentDataException, InstrumentCommandException, NotImplementedException, InstrumentException
from interface.objects import Granule, Attachment

from ion.agents.data.handlers.base_data_handler import BaseDataHandler, ConfigurationError, DummyDataHandler, FibonacciDataHandler
# Standard imports.

# 3rd party imports.
from gevent import spawn
from gevent.event import AsyncResult
from gevent.coros import Semaphore
import gevent
import numpy

@attr('UNIT', group='eoi')
class TestBaseDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self._rr_cli = Mock()
        self._stream_registrar = Mock()
        dh_config = {'external_dataset_res_id' : 'external_ds'}
        self._bdh = BaseDataHandler(rr_cli=self._rr_cli, stream_registrar=self._stream_registrar, dh_config=dh_config)

    def test_set_event_callback(self):
        self._bdh.set_event_callback('test_callback')
        self.assertEqual(self._bdh._event_callback, 'test_callback')

    def test__dh_config(self):
        self._bdh._event_callback = Mock()
        self._bdh._dh_event(type='test_type', value='test_value')

    @patch.object(BaseDataHandler, 'execute_acquire_data')
    @patch('ion.agents.data.handlers.base_data_handler.time')
    def test__poll(self, time_mock, execute_acquire_data_mock):
        bdh=BaseDataHandler(None, None, None)
        bdh._params = {'POLLING_INTERVAL':1}
        glet = spawn(bdh._poll)

        self.counter = 0
        def inc_counter(*args):
            self.counter += 1
            if self.counter == 2:
                bdh._polling = False

        time_mock.sleep.side_effect = inc_counter

        glet.join(timeout=5)

        self.assertTrue(execute_acquire_data_mock.call_count >= 2)

    def test__publish_data_with_granules(self):
        publisher = Mock()

        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        BaseDataHandler._publish_data(publisher=publisher, data_generator=data_generator)

        expected = [call(granule1), call(granule2), call(granule3)]
        self.assertEqual(publisher.publish.call_args_list, expected)

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test__publish_data_no_granules(self, log_mock):
        publisher = Mock()

        granule1 = Mock()
        granule2 = Mock()
        granule3 = Mock()
        data_generator = [granule1, granule2, granule3]

        BaseDataHandler._publish_data(publisher=publisher, data_generator=data_generator)

        self.assertEqual(publisher.publish.call_count, 0)
        self.assertEqual(log_mock.warn.call_count, 3)

    def test__publish_data_no_generator(self):
        publisher = Mock()
        data_generator = Mock()

        self.assertRaises(InstrumentDataException, BaseDataHandler._publish_data, publisher=publisher, data_generator=data_generator)

    @patch.object(BaseDataHandler, '_constraints_for_historical_request')
    @patch.object(BaseDataHandler, '_init_acquisition_cycle')
    @patch.object(BaseDataHandler, '_get_data')
    @patch.object(BaseDataHandler, '_publish_data')
    def test__acquire_data_with_constraints(self, _publish_data_mock, _get_data_mock, _init_acquisition_cycle_mock, _get_archive_constraints_mock):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        _get_data_mock.return_value = data_generator

        config = {'constraints' : {'test_constraints':'value'}}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        update_new_data_check_attachment = Mock()
        BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback, update_new_data_check_attachment=update_new_data_check_attachment)

        _init_acquisition_cycle_mock.assert_called_once_with(config)
        _get_archive_constraints_mock.assert_called_once_with(config)
        _get_data_mock.assert_called_once_with(config)
        _publish_data_mock.assert_called_once_with(publisher, data_generator)

    @patch.object(BaseDataHandler, '_init_acquisition_cycle')
    @patch.object(BaseDataHandler, '_get_data')
    @patch.object(BaseDataHandler, '_publish_data')
    @patch.object(BaseDataHandler, '_constraints_for_new_request')
    @patch('ion.agents.data.handlers.base_data_handler.gevent')
    def test__acquire_data_no_constraints(self, gevent_mock, _new_data_constraints_mock, _publish_data_mock, _get_data_mock, _init_acquisition_cycle_mock):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        gevent_mock.getcurrent = Mock()

        _new_data_constraints_mock.return_value = {'constraints' : {'test_constraints':'value'}}
        _get_data_mock.return_value = data_generator

        config = {}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        update_new_data_check_attachment = Mock()
        BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback, update_new_data_check_attachment=update_new_data_check_attachment)

        _init_acquisition_cycle_mock.assert_called_once_with(config)
        _new_data_constraints_mock.assert_called_once_with(config)
        _get_data_mock.assert_called_once_with(config)
        _publish_data_mock.assert_called_once_with(publisher, data_generator)

    @patch.object(BaseDataHandler, '_init_acquisition_cycle')
    @patch.object(BaseDataHandler, '_get_data')
    @patch.object(BaseDataHandler, '_constraints_for_new_request')
    @patch('ion.agents.data.handlers.base_data_handler.gevent')
    def test__acquire_data_raise_exception(self, gevent_mock, _new_data_constraints_mock, _get_data_mock, _init_acquisition_cycle_mock):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        gevent_mock.getcurrent = Mock()

        _new_data_constraints_mock.return_value = None
        _get_data_mock.return_value = data_generator

        config = {}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        update_new_data_check_attachment = Mock()
        self.assertRaises(InstrumentParameterException, BaseDataHandler._acquire_data, config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback, update_new_data_check_attachment=update_new_data_check_attachment)

    @patch.object(BaseDataHandler, '_constraints_for_historical_request')
    @patch('ion.agents.data.handlers.base_data_handler.EventPublisher')
    @patch.object(BaseDataHandler, '_init_acquisition_cycle')
    @patch.object(BaseDataHandler, '_get_data')
    @patch.object(BaseDataHandler, '_publish_data')
    def test__acquire_data_with_constraints_testing_flag(self, _publish_data_mock, _get_data_mock, _init_acquisition_cycle_mock, EventPublisher_mock, _get_archive_constraints_mock):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        _get_data_mock.return_value = data_generator

        config = {'constraints' : {'test_constraints':'value'}, 'TESTING':True}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        update_new_data_check_attachment = Mock()
        EventPublisher_mock.publish_event = Mock()
        BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback, update_new_data_check_attachment=update_new_data_check_attachment)

        _init_acquisition_cycle_mock.assert_called_once_with(config)
        _get_data_mock.assert_called_once_with(config)
        _publish_data_mock.assert_called_once_with(publisher, data_generator)
        _get_archive_constraints_mock.assert_called_once_with(config)
        EventPublisher_mock.publish_event.assert_called_once()

    def test_cmd_dvr_initialize(self):
        self._bdh.initialize = Mock()
        self._bdh.initialize.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('initialize')
        self._bdh.initialize.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_get(self):
        self._bdh.get = Mock()
        self._bdh.get.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('get')
        self._bdh.get.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_set(self):
        self._bdh.set = Mock()
        self._bdh.set.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('set')
        self._bdh.set.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_get_resource_params(self):
        self._bdh.get_resource_params = Mock()
        self._bdh.get_resource_params.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('get_resource_params')
        self._bdh.get_resource_params.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_get_resource_commands(self):
        self._bdh.get_resource_commands = Mock()
        self._bdh.get_resource_commands.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('get_resource_commands')
        self._bdh.get_resource_commands.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_execute_acquire_data(self):
        self._bdh.execute_acquire_data = Mock()
        self._bdh.execute_acquire_data.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('execute_acquire_data')
        self._bdh.execute_acquire_data.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_execute_start_autosample(self):
        self._bdh.execute_start_autosample = Mock()
        self._bdh.execute_start_autosample.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('execute_start_autosample')
        self._bdh.execute_start_autosample.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def test_cmd_dvr_execute_stop_autosample(self):
        self._bdh.execute_stop_autosample = Mock()
        self._bdh.execute_stop_autosample.return_value = sentinel.ret_val
        ret = self._bdh.cmd_dvr('execute_stop_autosample')
        self._bdh.execute_stop_autosample.assert_called_once()
        self.assertEquals(ret, sentinel.ret_val)

    def _unused_cmd_log_output(self, cmd):
        return 'Command \'{0}\' not used by DataHandler'.format(cmd)

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_execute_acquire_sample(self, log_mock):
        ret = self._bdh.cmd_dvr('execute_acquire_sample')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('execute_acquire_sample'))

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_configure(self, log_mock):
        ret = self._bdh.cmd_dvr('configure')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('configure'))

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_connect(self, log_mock):
        ret = self._bdh.cmd_dvr('connect')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('connect'))

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_disconnect(self, log_mock):
        ret = self._bdh.cmd_dvr('disconnect')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('disconnect'))

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_get_current_state(self, log_mock):
        ret = self._bdh.cmd_dvr('get_current_state')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('get_current_state'))

    @patch('ion.agents.data.handlers.base_data_handler.log')
    def test_cmd_dvr_discover(self, log_mock):
        ret = self._bdh.cmd_dvr('discover')
        self.assertIsNone(ret)
        self.assertEqual(log_mock.info.call_args[0][0], self._unused_cmd_log_output('discover'))

    def test_cmd_dvr_unknown_command(self):
        self.assertRaises(InstrumentCommandException, self._bdh.cmd_dvr, 'unknown_command')

    def test_initialize(self):
        ret = self._bdh.initialize()
        self.assertIsInstance(self._bdh._glet_queue, list)
        self.assertIsInstance(self._bdh._semaphore, Semaphore)
        self.assertIsNone(ret)

    def test_configure_with_args(self):
        dh_config = {'test': 'test_configure'}
        ret = self._bdh.configure(dh_config)
        self.assertIsNone(ret)

    def test_configure_no_args(self):
        self.assertRaises(InstrumentParameterException, self._bdh.configure)

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_start_autosample(self, mock):
        self._bdh._polling = False
        self._bdh._polling_glet = None
        ret = self._bdh.execute_start_autosample()
        mock.assert_called_once()
        self.assertIsNone(ret)

    def test_execute_start_autosample_polling(self):
        self._bdh._polling = True
        self._bdh._polling_glet = None
        ret = self._bdh.execute_start_autosample()
        self.assertIsNone(ret)

    def test_execute_start_autosample_polling_glet(self):
        self._bdh._polling = False
        self._bdh._polling_glet = 'something'
        ret = self._bdh.execute_start_autosample()
        self.assertIsNone(ret)

    def test_execute_stop_autosample(self):
        self._bdh._polling = True
        self._bdh._polling_glet = Mock()
        ret = self._bdh.execute_stop_autosample()
        self.assertIsNone(ret)
        self.assertFalse(self._bdh._polling)
        self.assertIsNone(self._bdh._polling_glet)

    def test_get_resource_params(self):
        ret = self._bdh.get_resource_params()
        self.assertEqual(ret, ['PATCHABLE_CONFIG_KEYS','POLLING_INTERVAL'])

    def test_get_resource_commands(self):
        ret = self._bdh.get_resource_commands()
        self.assertEqual(ret, ['acquire_data', 'start_autosample', 'stop_autosample'])

    def test__init_acquisition_cycle(self):
        config = {}
        with self.assertRaises(NotImplementedException) as cm:
            BaseDataHandler._init_acquisition_cycle(config)
            ex = cm.exception
            # TODO: This is brittle as the message could change - do we need this check, or is getting the exception enough?
            self.assertEqual(ex.message, "ion.agents.data.handlers.base_data_handler.BaseDataHandler must implement '_init_acquisition_cycle'")

    def test__new_data_constraints(self):
        config = {}
        self.assertRaises(NotImplementedException, BaseDataHandler._constraints_for_new_request, config)

    def test__get_archive_constraints(self):
        config = {}
        self.assertRaises(NotImplementedException, BaseDataHandler._constraints_for_historical_request, config)

    def test__get_data(self):
        config = {}
        self.assertRaises(NotImplementedException, BaseDataHandler._get_data, config)

    def test__unlock_new_data_callback(self):
        self._bdh._semaphore = Mock()
        self._bdh._unlock_new_data_callback(None)
        self._bdh._semaphore.release.assert_called_once()

    def test_get_all(self):
        #TODO: Need to change back to enums instead of strings. Problem with BaseEnum.
        ret = self._bdh.get(['DRIVER_PARAMETER_ALL'])
        self.assertEqual(ret, {
            'POLLING_INTERVAL' : 3600,
            'PATCHABLE_CONFIG_KEYS' : ['stream_id','constraints']
        })

    def test_get_all_with_no_arguments(self):
        ret = self._bdh.get()

        self.assertEqual(ret, {
            'POLLING_INTERVAL' : 3600,
            'PATCHABLE_CONFIG_KEYS' : ['stream_id','constraints']
        })

    def test_get_not_list_or_tuple(self):
        self.assertRaises(InstrumentParameterException, self._bdh.get, 'not_found')

    def test_get_polling_interval(self):
        #TODO: Need to change back to enums instead of strings. Problem with BaseEnum.
        ret = self._bdh.get(['POLLING_INTERVAL'])
        self.assertEqual(ret, {'POLLING_INTERVAL' : 3600})

    def test_get_patchable_config_keys(self):
        #TODO: Need to change back to enums instead of strings. Problem with BaseEnum.
        ret = self._bdh.get(['PATCHABLE_CONFIG_KEYS'])
        self.assertEqual(ret, {'PATCHABLE_CONFIG_KEYS' : ['stream_id','constraints']})

    def test_get_polling_and_patchable_config_keys(self):
        #TODO: Need to change back to enums instead of strings. Problem with BaseEnum.
        ret = self._bdh.get(['POLLING_INTERVAL', 'PATCHABLE_CONFIG_KEYS'])
        self.assertEqual(ret, {
            'POLLING_INTERVAL' : 3600,
            'PATCHABLE_CONFIG_KEYS' : ['stream_id','constraints']
        })

    def test_get_key_error(self):
        with self.assertRaises(InstrumentParameterException) as cm:
            self._bdh.get(['not_found'])

    def test_set_no_parameter(self):
        with self.assertRaises(InstrumentParameterException) as cm:
            self._bdh.set()

    def test_set_not_dictionary(self):
        with self.assertRaises(InstrumentParameterException) as cm:
            self._bdh.set('not_found')

    def test_set_polling(self):
        #TODO: Need to change back to enums instead of strings. Problem with BaseEnum.
        self._bdh.set({'POLLING_INTERVAL' : 7200})
        self.assertEqual(self._bdh.get(['POLLING_INTERVAL']), {'POLLING_INTERVAL' : 7200})

    def test_set_not_found(self):
        with self.assertRaises(InstrumentParameterException) as cm:
            self._bdh.set({'not_found': 1})

    def test_execute_acquire_data_not_dictionary(self):
        with self.assertRaises(ConfigurationError) as cm:
            self._bdh.execute_acquire_data('not_found')

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_with_stream_id_not_new(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._glet_queue = Mock()
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id', 'constraints' : 'test_constraints'})
        self._stream_registrar.create_publisher.assert_called_once_with(stream_id='test_stream_id')

    def test_execute_acquire_data_with_stream_id_new_already_acquiring(self):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = False
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})
        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_with_stream_id_new_not_already_acquiring(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = True
        self._bdh._glet_queue = Mock()
        attachment = Attachment()
        attachment.keywords = ['NewDataCheck', 'NotFound']
        attachment2 = Attachment()
        attachment2.keywords = ['NotTheRightKeyword']
        self._bdh._find_new_data_check_attachment = Mock()
        self._bdh._find_new_data_check_attachment.return_value = [attachment2, attachment]
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})

        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)
        self._bdh._find_new_data_check_attachment.assert_called_once()

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_with_stream_id_new_not_already_acquiring_res_not_found(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = True

        self._bdh._find_new_data_check_attachment = Mock()
        self._bdh._find_new_data_check_attachment.side_effect = InstrumentException

        self.assertRaises(InstrumentException, self._bdh.execute_acquire_data, {'stream_id' : 'test_stream_id'})
        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)
        self._bdh._find_new_data_check_attachment.assert_called_once()

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_no_attachments(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = True
        self._bdh._glet_queue = Mock()
        self._bdh._find_new_data_check_attachment = Mock()
        self._bdh._find_new_data_check_attachment.return_value = []

        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})

        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)
        self._bdh._find_new_data_check_attachment.assert_called_once()

@attr('UNIT',group='eoi')
class TestDummyDataHandlerUnit(PyonTestCase):
    def setUp(self):
        pass

    @patch.object(DummyDataHandler, '_init_acquisition_cycle')
    def test__init_acquisition_cycle(self, _init_acquisition_cycle_mock):
        config = {}
        DummyDataHandler._init_acquisition_cycle(config)
        _init_acquisition_cycle_mock.assert_called_once_with(config)

    def test__new_data_constraints(self):
        max_rec = 10
        config = {'max_records':max_rec}
        ret = DummyDataHandler._constraints_for_new_request(config)
        self.assertTrue('array_len' in ret)
        i=ret['array_len']
        self.assertIsInstance(i, int)
        self.assertTrue(max_rec+1 <= i <= max_rec+10)

    @patch('ion.agents.data.handlers.base_data_handler.RecordDictionaryTool')
    @patch('ion.agents.data.handlers.base_data_handler.TaxyTool')
    @patch('ion.agents.data.handlers.base_data_handler.build_granule')
    def test__get_data(self, build_granule_mock, TaxyTool_mock, RecordDictionaryTool_mock):
        config={'constraints':{'array_len':6},'max_records':4,'data_producer_id':sentinel.dprod_id,'taxonomy':sentinel.taxy_tool}

        TaxyTool_mock.load.return_value = sentinel.ttool_ret_val
        RecordDictionaryTool_mock.side_effect = lambda **kwargs: {}

        for x in DummyDataHandler._get_data(config):
            RecordDictionaryTool_mock.assert_called_with(taxonomy=sentinel.ttool_ret_val)
            build_granule_mock.assert_called_with(taxonomy=sentinel.ttool_ret_val, data_producer_id=sentinel.dprod_id, record_dictionary=ANY)
            cargs=build_granule_mock.call_args
            self.assertIn('data', cargs[1]['record_dictionary'])
            self.assertIsInstance(cargs[1]['record_dictionary']['data'], numpy.ndarray)

        self.assertEquals(build_granule_mock.call_count, 2)
        self.assertEquals(RecordDictionaryTool_mock.call_count, 2)

        TaxyTool_mock.load.assert_called_once_with(sentinel.taxy_tool)



@attr('UNIT',group='eoi')
class TestFibonacciDataHandlerUnit(PyonTestCase):
    def setUp(self):
        pass

    @patch.object(FibonacciDataHandler, '_init_acquisition_cycle')
    def test__init_acquisition_cycle(self, _init_acquisition_cycle_mock):
        config = {}
        FibonacciDataHandler._init_acquisition_cycle(config)
        _init_acquisition_cycle_mock.assert_called_once_with(config)

    def test__new_data_constraints(self):
        ret = FibonacciDataHandler._constraints_for_new_request({})
        self.assertTrue('count' in ret)
        i=ret['count']
        self.assertIsInstance(i, int)
        self.assertTrue(5 <= i <= 20)

    @patch('ion.agents.data.handlers.base_data_handler.RecordDictionaryTool')
    @patch('ion.agents.data.handlers.base_data_handler.TaxyTool')
    @patch('ion.agents.data.handlers.base_data_handler.build_granule')
    def test__get_data(self, build_granule_mock, TaxyTool_mock, RecordDictionaryTool_mock):
        config={'constraints':{'count':6},'max_records':4,'data_producer_id':sentinel.dprod_id,'taxonomy':sentinel.taxy_tool}

        TaxyTool_mock.load.return_value = sentinel.ttool_ret_val
        RecordDictionaryTool_mock.side_effect = lambda **kwargs: {}

        for x in FibonacciDataHandler._get_data(config):
            RecordDictionaryTool_mock.assert_called_with(taxonomy=sentinel.ttool_ret_val)
            build_granule_mock.assert_called_with(taxonomy=sentinel.ttool_ret_val, data_producer_id=sentinel.dprod_id, record_dictionary=ANY)
            cargs=build_granule_mock.call_args
            self.assertIn('data', cargs[1]['record_dictionary'])
            self.assertIsInstance(cargs[1]['record_dictionary']['data'], numpy.ndarray)

        self.assertEquals(build_granule_mock.call_count, 2)
        self.assertEquals(RecordDictionaryTool_mock.call_count, 2)

        TaxyTool_mock.load.assert_called_once_with(sentinel.taxy_tool)