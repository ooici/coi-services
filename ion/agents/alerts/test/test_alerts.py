#!/usr/bin/env python

"""
@package ion.agents.alarms.test_alarms
@file ion/agents/alarms/test/test_alarms.py
@author Edward Hunter
@brief Test cases for agent alarms.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Import pyon first for monkey patching.

# Pyon log and config objects.
from pyon.public import log

# Standard library.
import copy

# Gevent async.
from gevent.event import AsyncResult
import gevent

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
import unittest

# Event pubsub support.
from pyon.event.event import EventSubscriber

# Alarm types and events.
from interface.objects import StreamAlertType, AggregateStatusType

# Alarm objects.
from pyon.public import IonObject
from ion.agents.alerts.alerts import *

# Resource agent.
from pyon.agent.agent import ResourceAgentState, ResourceAgentEvent

"""
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_greater_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_less_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_two_sided_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_late_data
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_state_alert
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_command_error_alert
"""

@attr('INT', group='sa')
class TestAlerts(IonIntegrationTestCase):
    """
    """
    
    ############################################################################
    # Setup, teardown.
    ############################################################################
        
    def setUp(self):
        """
        Set up subscribers for alarm events.
        """
        
        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        self._event_count = 0
        self._events_received = []
        self._async_event_result = AsyncResult()
        self._resource_id = 'abc123'
        self._origin_type = "InstrumentDevice"

        def consume_event(*args, **kwargs):
            log.debug('Test recieved ION event: args=%s, kwargs=%s, event=%s.',
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._event_count > 0 and \
                self._event_count == len(self._events_received):
                self._async_event_result.set()
            
            
        self._event_subscriber = EventSubscriber(
            event_type='DeviceStatusAlertEvent', callback=consume_event,
            origin=self._resource_id)
        
        self._event_subscriber.start()
        self._event_subscriber._ready_event.wait(timeout=5)

        def stop_subscriber():
            self._event_subscriber.stop()
            self._event_subscriber = None
            
        self.addCleanup(stop_subscriber)

    ###############################################################################
    # Tests.
    ###############################################################################

    def test_greater_than_interval(self):
        """
        """
        alert_def = {
            'name' : 'current_warning_interval',
            'description' : 'Current is above normal range.',
            'aggregate_type' : AggregateStatusType.AGGREGATE_DATA,
            'alert_type' : StreamAlertType.WARNING,
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
            'stream_name' : 'fakestreamname',
            'value_id' : 'port_current',
            'lower_bound' : 10.5,
            'lower_rel_op' : '<',
            'upper_bound' : None,
            'upper_rel_op' : None,
            'alert_class' : 'IntervalAlert'
        }

        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()
        """
        {'status': None, 'alert_type': 1, 'lower_bound': 10.5, 'upper_rel_op': '<',
        'alert_class': 'IntervalAlert', 'message': 'Current is above normal range.',
        'stream_name': 'fakestreamname', 'name': 'current_warning_interval',
        'upper_bound': None, 'value': None, 'value_id': 'port_current',
        'lower_rel_op': None}
        """
        # This sequence will produce 5 alerts:
        # All clear on 30,
        # Warning on 5.5
        # All clear on 15.1
        # Warning on 3.3
        # All clear on 15.0
        self._event_count = 5
        test_vals = [30, 30.4, 5.5, 5.6, 15.1, 15.2,
                     15.3, 3.3, 3.4, 15.0, 15.5]

        for x in test_vals:
            alert.eval_alert(stream_name='fakestreamname',
                             value=x, value_id='port_current')

        self._async_event_result.get(timeout=30)        
        """
        {'origin': 'abc123', 'status': 1, '_id': '04ccd20d67574b2ea3df869f2b6d4123', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [30], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659152082', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '050d2c66eb47435888ecab9d58399922', 'description': 'Current is above normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [5.5], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659152089', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '3294c5f7e2be413c806604e93b69e973', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [15.1], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659152095', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '99a98e19a1454740a8464dab8de4dc0e', 'description': 'Current is above normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [3.3], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659152101', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '93a214ee727e424e8b6a7e024a4d89be', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [15.0], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659152108', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        """

    def test_less_than_interval(self):
        """
        """
        alert_def = {
            'name' : 'current_warning_interval',
            'stream_name' : 'fakestreamname',
            'description' : 'Current is below normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'value_id' : 'port_current',
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
            'lower_bound' : None,
            'lower_rel_op' : None,
            'upper_bound' : 4.0,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }

        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()

        # This sequence will produce 5 alerts:
        # Warning on the 5.5
        # All clear on 3.3,
        # Warning on 4.5
        # All clear on 3.3
        # Warning on 4.8
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 3.3, 3.3, 4.5, 4.5, 3.3, 3.3, 4.8]

        for x in test_vals:
            alert.eval_alert(stream_name='fakestreamname',
                             value=x, value_id='port_current')

        self._async_event_result.get(timeout=30)        
        """
        {'origin': 'abc123', 'status': 1, '_id': '43c83af591a84fa3adc3a77a5d97ed2b', 'description': 'Current is below normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [5.5], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659238728', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '36c45d09286d4ff38f5003ff97bef6a1', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [3.3], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659238735', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'd004b9c2d50f4b9899d6fbdd3c8c50d2', 'description': 'Current is below normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [4.5], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659238741', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'cb0cbaf1be0b4aa387e1a5ba4f1adb2c', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [3.3], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659238747', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'd4f6e4b4105e494083a0f8e34362f275', 'description': 'Current is below normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [4.8], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659238754', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        """
        
    def test_two_sided_interval(self):
        """
        """
        alert_def = {
            'name' : 'current_warning_interval',
            'stream_name' : 'fakestreamname',
            'description' : 'Current is outside normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'value_id' : 'port_current',
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,            
            'lower_bound' : 10,
            'lower_rel_op' : '<',
            'upper_bound' : 20,
            'upper_rel_op' : '<',
            'alert_class' : 'IntervalAlert'
        }

        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()

        # This sequence will produce 5 alerts:
        # Warning on the 5.5
        # All clear on 10.2,
        # Warning on 23.3
        # All clear on 17.5
        # Warning on 8.8
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 10.2, 10.3, 10.5, 15.5,
                     23.3, 23.3, 24.8, 17.5, 16.5, 12.5, 8.8, 7.7]

        for x in test_vals:
            event_data = alert.eval_alert(stream_name='fakestreamname',
                                          value=x, value_id='port_current')

        self._async_event_result.get(timeout=30)        
        """
        {'origin': 'abc123', 'status': 1, '_id': '45296cc01f3d42c59e1aeded4fafb33d', 'description': 'Current is outside normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [5.5], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659411921', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'd1a87a248f6640ceafdf8d09b66f4c6f', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [10.2], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659411927', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'e9945ff47b79436096f67ba9d373a889', 'description': 'Current is outside normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [23.3], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659411934', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': '649f5297997740dc83b39d23b6fbc5b9', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [17.5], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659411940', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'status': 1, '_id': 'c766e52fda05497f9e4a18024e4eb0d6', 'description': 'Current is outside normal range.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [8.8], 'value_id': 'port_current', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659411947', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'current_warning_interval'}
        """
        
    def test_late_data(self):
        """
        """

        def get_state():
            return ResourceAgentState.STREAMING

        alert_def = {
            'name' : 'late_data_warning',
            'stream_name' : 'fakestreamname',
            'description' : 'Expected data has not arrived.',
            'alert_type' : StreamAlertType.WARNING,
            'value_id' : None,
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,            
            'time_delta' : 3,
            'get_state' : get_state,
            'alert_class' : 'LateDataAlert'
        }
                
        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()        
        
        # This sequence will produce 6 events:
        # All clear on the first check data.
        # Warning during the first 10s delay.
        # All clear during the 1s samples following the first 10s delay.
        # Warning during the 2nd 10s delay.
        # All clear during the 1s samples following the second 10s delay.
        # Warning during the final 10s delay.
        self._event_count = 6
        #sleep_vals = [0.5, 0.7, 1.0, 1.1, 2.5, 2.3, 0.75, 0.5, 2.25, 0.5, 0.5, 2.5, 2.5]
        sleep_vals = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 10, 1, 1, 1, 1,
                      1, 1, 1, 1, 1, 1, 1, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                      1, 1, 1, 1, 1, 1, 1, 10]
        for x in sleep_vals:
            event_data = alert.eval_alert(stream_name='fakestreamname')
            gevent.sleep(x)

        self._async_event_result.get(timeout=30)
        """
        {'origin': 'abc123', 'status': 1, '_id': '455f5916fbf845acb0f71da229fa46a4', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659773.60301], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659773603', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '01b74790959d40c99da09fd1e52b447f', 'description': 'Expected data has not arrived.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659785.624983], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659791612', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'db73d083bddc4ebbb4dd4e6541d70bf3', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659795.625946], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659795626', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'ddd501a1599f47a49886ff89cda2f980', 'description': 'Expected data has not arrived.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659806.649514], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659812631', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '0c1dda72a53c47908c2755dfdcf87d15', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659816.650299], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659816651', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '6b0f2fb6eedd4cc39ab3e6dc6c2c2d69', 'description': 'Expected data has not arrived.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [1366659832.673774], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': 'fakestreamname', 'ts_created': '1366659836651', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'late_data_warning'}
        """        
        alert.stop()

    @unittest.skip('Waiting to be finished and verified.')
    def test_rsn_event_alert(self):
        """
        """
        alert_def = {
            'name' : 'input_voltage',
            'description' : 'input_voltage is not in range range.',
            'alert_type' : StreamAlertType.WARNING,
            'value_id' : 'input_voltage',
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
            'alert_class' : 'RSNEventAlert',
            'aggregate_type' : AggregateStatusType.AGGREGATE_POWER

        }


        # Example from CI-OMS interface spec
        # need: tag for the value, current value and warning/error
#        {
#          "group": "power",
#          "url": "http://localhost:8000",
#          "timestamp": 3573569514.295556,
#          "ref_id": "44.78",
#          "platform_id": "TODO_some_platform_id_of_type_UPS",
#          "message": "low battery (synthetic event generated from simulator)"
#        }


        #proposed structure:
#        {
#        "group": "power",
#        "name" : "low_voltage_warning",
#        "value_id" : "input_voltage",
#        "value" : "1.2",
#        "alert_type" : "warning",
#        "url": "http://localhost:8000",
#        "timestamp": 3573569514.295556,
#        "ref_id": "44.78",
#        "platform_id": "TODO_some_platform_id_of_type_UPS",
#        "message": "low battery (synthetic event generated from simulator)"
#        }



        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()

        test_val = \
        {
        "group": "power",
        "name" : "low_voltage_warning",
        "value_id" : "input_voltage",
        "value" : "1.2",
        "alert_type" : "warning",
        "url": "http://localhost:8000",
        "ref_id": "44.78",
        "platform_id": "e88f8b325b274dafabcc7d7d1e85bc5d",
        "description": "low battery (synthetic event generated from simulator)"
        }

        alert.eval_alert(rsn_alert=test_val)

        status = alert.get_status()
        log.debug('test_rsn_event_alert status: %s', alert)

        #self._async_event_result.get(timeout=30)

    def test_state_alert(self):
        """
        """
        alert_def = {
            'name' : 'comms_warning',
            'description' : 'Detected comms failure.',
            'alert_type' : StreamAlertType.WARNING,
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
            'alert_states' : [
                ResourceAgentState.LOST_CONNECTION,
                ResourceAgentState.ACTIVE_UNKNOWN
                              ],
            'clear_states' : [
                ResourceAgentState.IDLE,
                ResourceAgentState.COMMAND,
                ResourceAgentState.STREAMING
                ],
            'alert_class' : 'StateAlert'
        }

        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()

        # This sequence will produce 5 alerts:
        # All clear on uninitialized (prev value None)
        # Warning on lost connection,
        # All clear on streaming,
        # Warning on lost connection,
        # All clear on idle.
        self._event_count = 5
        test_vals = [
            ResourceAgentState.UNINITIALIZED,
            ResourceAgentState.INACTIVE,
            ResourceAgentState.IDLE,
            ResourceAgentState.COMMAND,
            ResourceAgentState.STREAMING,
            ResourceAgentState.LOST_CONNECTION,
            ResourceAgentState.STREAMING,
            ResourceAgentState.LOST_CONNECTION,
            ResourceAgentState.UNINITIALIZED,
            ResourceAgentState.INACTIVE,
            ResourceAgentState.IDLE,
            ResourceAgentState.COMMAND,
            ResourceAgentState.STREAMING
            ]


        for x in test_vals:
            alert.eval_alert(state=x)

        self._async_event_result.get(timeout=30)        
        """
        {'origin': 'abc123', 'status': 1, '_id': '36e733662e674388ba2cfb8165315b86', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_UNINITIALIZED'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366659466203', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'ee9578bd37ed45c088479131f4b71509', 'description': 'Detected comms failure.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_LOST_CONNECTION'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366659466210', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '237db9d3bb8e455a98e58429df6c08ea', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_STREAMING'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366659466216', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'bef0987444254adb9db8fa752b2e1c81', 'description': 'Detected comms failure.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_LOST_CONNECTION'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366659466222', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '4d78cede6e01419a881eac288cf1dbd3', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': ['RESOURCE_AGENT_STATE_IDLE'], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366659466229', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        """
        
    def test_command_error_alert(self):
        """
        """
        alert_def = {
            'name' : 'comms_warning',
            'description' : 'Detected comms failure.',
            'alert_type' : StreamAlertType.WARNING,
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
            'command' : ResourceAgentEvent.GO_ACTIVE,
            'clear_states' : [
                ResourceAgentState.IDLE,
                ResourceAgentState.COMMAND,
                ResourceAgentState.STREAMING
                ],
            'alert_class' : 'CommandErrorAlert'
        }

        cls = alert_def.pop('alert_class')
        alert = eval('%s(**alert_def)' % cls)

        status = alert.get_status()

        # This sequence will produce 5 alerts:
        # All clear on initialize success (prev value None)
        # Warning on go active failure,
        # All clear on go active success,
        # Warning on go active failure
        # All clear on transition to idle (reconnect)
        self._event_count = 5
        test_vals = [
            {'state': ResourceAgentState.UNINITIALIZED },
            {'command': ResourceAgentEvent.INITIALIZE, 'command_success': True},
            {'state': ResourceAgentState.INACTIVE },
            {'command': ResourceAgentEvent.GO_ACTIVE, 'command_success': False},
            {'state': ResourceAgentState.INACTIVE },
            {'command': ResourceAgentEvent.RESET, 'command_success': True},
            {'state': ResourceAgentState.UNINITIALIZED },
            {'command': ResourceAgentEvent.INITIALIZE, 'command_success': True},
            {'state': ResourceAgentState.INACTIVE },
            {'command': ResourceAgentEvent.GO_ACTIVE, 'command_success': True},
            {'state': ResourceAgentState.IDLE },
            {'command': ResourceAgentEvent.RESET, 'command_success': True},
            {'state': ResourceAgentState.UNINITIALIZED },
            {'command': ResourceAgentEvent.INITIALIZE, 'command_success': True},
            {'state': ResourceAgentState.INACTIVE },
            {'command': ResourceAgentEvent.GO_ACTIVE, 'command_success': False},
            {'state': ResourceAgentState.INACTIVE },
            {'state': ResourceAgentState.IDLE }
        ]

        for x in test_vals:
            alert.eval_alert(**x)

        self._async_event_result.get(timeout=30)            
        """
        {'origin': 'abc123', 'status': 1, '_id': '44ec7f5452004cceb3d8dbaa941f08ef', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366740538561', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'ae33b508a3d845feacd05d9d25992924', 'description': 'Detected comms failure.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366740538571', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': 'f9eb2f3c477c46f1af0076fd4eab58f1', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366740538579', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '23153a1c48de4f25bce6f84cfab8444a', 'description': 'Detected comms failure.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366740538586', 'sub_type': 1, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        {'origin': 'abc123', 'status': 1, '_id': '18b85d52ac1a438a9f5f8e69e5f4f6e8', 'description': 'The alert is cleared.', 'time_stamps': [], 'type_': 'DeviceStatusAlertEvent', 'valid_values': [], 'values': [None], 'value_id': '', 'base_types': ['DeviceStatusEvent', 'DeviceEvent', 'Event'], 'stream_name': '', 'ts_created': '1366740538592', 'sub_type': 3, 'origin_type': 'InstrumentDevice', 'name': 'comms_warning'}
        """
        