#!/usr/bin/env python

"""
@package ion.agents.data.test.test_base_data_handler
@file ion/agents/data/test/test_base_data_handler.py
@author Tim Giguere
@author Christopher Mueller
@brief Test cases for R2 BaseDataHandler
"""

from pyon.public import log, CFG
from nose.plugins.attrib import attr
from mock import patch, Mock, call
from pyon.util.unit_test import PyonTestCase
import unittest

from ion.services.mi.exceptions import InstrumentParameterException, InstrumentDataException, InstrumentCommandException, NotImplementedException
from interface.objects import Granule, Attachment

from ion.agents.data.handlers.base_data_handler import BaseDataHandler, ConfigurationError

from pyon.core.exception import InstParameterError, NotFound
# Standard imports.

# 3rd party imports.
from gevent import spawn
from gevent.event import AsyncResult
import gevent

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

    def test_publish_data_with_granules(self):
        publisher = Mock()

        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        BaseDataHandler._publish_data(publisher=publisher, data_generator=data_generator)

        expected = [call(granule1), call(granule2), call(granule3)]
        self.assertEqual(publisher.publish.call_args_list, expected)

    def test_publish_data_no_granules(self):
        publisher = Mock()

        granule1 = Mock()
        granule2 = Mock()
        granule3 = Mock()
        data_generator = [granule1, granule2, granule3]

        BaseDataHandler._publish_data(publisher=publisher, data_generator=data_generator)

        self.assertEqual(publisher.publish.call_count, 0)

    def test_publish_data_no_generator(self):
        publisher = Mock()
        data_generator = Mock()

        with self.assertRaises(TypeError) as cm:
            BaseDataHandler._publish_data(publisher=publisher, data_generator=data_generator)

    def test_acquire_data_with_constraints(self):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        BaseDataHandler._init_acquisition_cycle = Mock()
        BaseDataHandler._get_data = Mock(auto_spec=data_generator)
        BaseDataHandler._get_data.return_value = data_generator
        BaseDataHandler._publish_data = Mock()

        config = {'constraints' : 'test_constraints'}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback)

        BaseDataHandler._init_acquisition_cycle.assert_called_once_with(config)
        BaseDataHandler._get_data.assert_called_once_with(config)
        BaseDataHandler._publish_data.assert_called_once_with(publisher, data_generator)

    @patch('ion.agents.data.handlers.base_data_handler.gevent')
    def test_acquire_data_no_constraints(self, mock_gevent):
        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        mock_gevent.getcurrent = Mock()

        BaseDataHandler._init_acquisition_cycle = Mock()
        BaseDataHandler._new_data_constraints = Mock()
        BaseDataHandler._new_data_constraints.return_value = {'constraints' : 'test_constraints'}
        BaseDataHandler._get_data = Mock(auto_spec=data_generator)
        BaseDataHandler._get_data.return_value = data_generator
        BaseDataHandler._publish_data = Mock()

        config = {}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback)

        BaseDataHandler._init_acquisition_cycle.assert_called_once_with(config)
        BaseDataHandler._new_data_constraints.assert_called_once_with(config)
        BaseDataHandler._get_data.assert_called_once_with(config)
        BaseDataHandler._publish_data.assert_called_once_with(publisher, data_generator)

    @patch('ion.agents.data.handlers.base_data_handler.gevent')
    def test_acquire_data_raise_exception(self, mock_gevent):

        granule1 = Mock(spec=Granule)
        granule2 = Mock(spec=Granule)
        granule3 = Mock(spec=Granule)
        data_generator = [granule1, granule2, granule3]

        mock_gevent.getcurrent = Mock()

        BaseDataHandler._init_acquisition_cycle = Mock()
        BaseDataHandler._new_data_constraints = Mock()
        BaseDataHandler._new_data_constraints.return_value = None
        BaseDataHandler._get_data = Mock(auto_spec=data_generator)
        BaseDataHandler._get_data.return_value = data_generator
        BaseDataHandler._publish_data = Mock()

        config = {}
        publisher = Mock()
        unlock_new_data_callback = Mock()
        with self.assertRaises(InstrumentParameterException) as cm:
            BaseDataHandler._acquire_data(config=config, publisher=publisher, unlock_new_data_callback=unlock_new_data_callback)

    def test_cmd_dvr_configure(self):
        ret = self._bdh.cmd_dvr('configure')
        self.assertIsNone(ret)

    def test_cmd_dvr_initialize(self):
        self._bdh.initialize = Mock()
        self._bdh.initialize.return_value = None
        ret = self._bdh.cmd_dvr('initialize')
        self._bdh.initialize.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_get(self):
        self._bdh.get = Mock()
        self._bdh.get.return_value = None
        ret = self._bdh.cmd_dvr('get')
        self._bdh.get.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_set(self):
        self._bdh.set = Mock()
        self._bdh.set.return_value = None
        ret = self._bdh.cmd_dvr('set')
        self._bdh.set.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_get_resource_params(self):
        self._bdh.get_resource_params = Mock()
        self._bdh.get_resource_params.return_value = None
        ret = self._bdh.cmd_dvr('get_resource_params')
        self._bdh.get_resource_params.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_get_resource_commands(self):
        self._bdh.get_resource_commands = Mock()
        self._bdh.get_resource_commands.return_value = None
        ret = self._bdh.cmd_dvr('get_resource_commands')
        self._bdh.get_resource_commands.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_execute_acquire_data(self):
        self._bdh.execute_acquire_data = Mock()
        self._bdh.execute_acquire_data.return_value = None
        ret = self._bdh.cmd_dvr('execute_acquire_data')
        self._bdh.execute_acquire_data.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_execute_acquire_sample(self):
        self._bdh.execute_acquire_sample = Mock()
        self._bdh.execute_acquire_sample.return_value = None
        ret = self._bdh.cmd_dvr('execute_acquire_sample')
        self._bdh.execute_acquire_sample.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_execute_start_autosample(self):
        self._bdh.execute_start_autosample = Mock()
        self._bdh.execute_start_autosample.return_value = None
        ret = self._bdh.cmd_dvr('execute_start_autosample')
        self._bdh.execute_start_autosample.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_execute_stop_autosample(self):
        self._bdh.execute_stop_autosample = Mock()
        self._bdh.execute_stop_autosample.return_value = None
        ret = self._bdh.cmd_dvr('execute_stop_autosample')
        self._bdh.execute_stop_autosample.assert_called_once()
        self.assertIsNone(ret)

    def test_cmd_dvr_connect(self):
        ret = self._bdh.cmd_dvr('connect')
        self.assertIsNone(ret)

    def test_cmd_dvr_disconnect(self):
        ret = self._bdh.cmd_dvr('disconnect')
        self.assertIsNone(ret)

    def test_cmd_dvr_get_current_state(self):
        ret = self._bdh.cmd_dvr('get_current_state')
        self.assertIsNone(ret)

    def test_cmd_dvr_discover(self):
        ret = self._bdh.cmd_dvr('discover')
        self.assertIsNone(ret)

    def test_cmd_dvr_command_not_found(self):
        ret = None
        with self.assertRaises(InstrumentCommandException) as cm:
            ret = self._bdh.cmd_dvr('not_found')

        self.assertIsNone(ret)

    def test_initialize(self):
        ret = self._bdh.initialize()
        self.assertIsNone(ret)

    def test_configure_with_args(self):
        dh_config = {'test': 'test_configure'}
        ret = self._bdh.configure(dh_config)
        self.assertIsNone(ret)

    def test_configure_no_args(self):
        ret = None
        with self.assertRaises(InstrumentParameterException) as cm:
            ret = self._bdh.configure()

        self.assertIsNone(ret)

    def test_execute_acquire_sample(self):
        self._bdh._dh_event = Mock()
        ret = self._bdh.execute_acquire_sample()
        self.assertEqual(ret, {'stream_name':'data_stream','p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]})

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
        self.assertEqual(ret, [('POLLING_INTERVAL',)])

    def test_get_resource_commands(self):
        ret = self._bdh.get_resource_commands()
        self.assertEqual(ret, ['acquire_data', 'acquire_sample', 'start_autosample', 'stop_autosample'])

    def test__init_acquisition_cycle(self):
        config = {}
        with self.assertRaises(NotImplementedError) as cm:
            BaseDataHandler._init_acquisition_cycle(config)

        ex = cm.exception
        self.assertEqual(ex.message, "Initialize acquisition cycle not implemented in data handler")

    def test__new_data_constraints(self):
        config = {}
        with self.assertRaises(NotImplementedException) as cm:
            BaseDataHandler._new_data_constraints(config)

    def test__get_data(self):
        config = {}
        with self.assertRaises(NotImplementedException) as cm:
            BaseDataHandler._get_data(config)

    def test__calc_iter_cnt(self):
        total_recs = 100
        max_rec = 10
        ret = BaseDataHandler._calc_iter_cnt(total_recs=total_recs, max_rec=max_rec)
        self.assertEqual(ret, 10)

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
        with self.assertRaises(InstrumentParameterException) as cm:
            self._bdh.get('not_found')

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

    def test_execute_acquire_data_with_stream_id_new_acquiring(self):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = False
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})
        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_with_stream_id_new_not_acquiring(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = True
        attachment = Attachment()
        attachment.keywords = ['NewDataCheck', 'NotFound']
        self._rr_cli.find_objects.return_value = [attachment], ''
        self._bdh._glet_queue = Mock()
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})

        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)
        self._rr_cli.find_objects.assert_called_once()

    @patch('ion.agents.data.handlers.base_data_handler.spawn')
    def test_execute_acquire_data_no_attachments(self, mock):
        self._bdh._semaphore = Mock()
        self._bdh._semaphore.acquire.return_value = True
        self._rr_cli.find_objects.return_value = [], ''
        self._bdh._glet_queue = Mock()
        self._bdh.execute_acquire_data({'stream_id' : 'test_stream_id'})

        self._bdh._semaphore.acquire.assert_called_once_with(blocking=False)
        self._rr_cli.find_objects.assert_called_once()