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

# Event pubsub support.
from pyon.event.event import EventSubscriber

# Alarm types and events.
from interface.objects import StreamAlertType

# Alarm objects.
from pyon.public import IonObject
from ion.agents.alerts.alerts import *

# Resource agent.
from pyon.agent.agent import ResourceAgentState

"""
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_greater_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_less_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_two_sided_interval
bin/nosetests -s -v --nologcapture ion/agents/alerts/test/test_alerts.py:TestAlerts.test_late_data
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
            event_type='StreamAlertEvent', callback=consume_event,
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
            'stream_name' : 'fakestreamname',
            'message' : 'Current is above normal range.',
            'alert_type' : StreamAlertType.WARNING,
            'value_id' : 'port_current',
            'resource_id' : self._resource_id,
            'origin_type' : self._origin_type,
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
            alert.eval_alert(x)

        self._async_event_result.get(timeout=30)

        """
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 30, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '513d65cddb1d4947ab4179b047f2c8ed', 'ts_created': '1363389051198', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 5.5, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '4812fdb9403c4a929d9d4506404b9379', 'ts_created': '1363389051204', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 15.1, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '3b8fba21abb14bcebdaa4882ced2edcb', 'ts_created': '1363389051213', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 3.3, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '44506d0c78964d69911f7f81d2e7a9f1', 'ts_created': '1363389051220', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 15.0, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '142c64e105244e0581fe4c9e8b08f8e3', 'ts_created': '1363389051226', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        """

    def test_less_than_interval(self):
        """
        """
        alert_def = {
            'name' : 'current_warning_interval',
            'stream_name' : 'fakestreamname',
            'message' : 'Current is below normal range.',
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
            alert.eval_alert(x)

        self._async_event_result.get(timeout=30)
        """
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 5.5, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': 'e672da7a7f1f4d24b06eb4547d3f9f3f', 'ts_created': '1363389965362', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 3.3, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': '298c0d904a9c497dae9e8dad6c6d128d', 'ts_created': '1363389965367', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 4.5, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': '945d5c194dcb4293aa6df24fb3694642', 'ts_created': '1363389965374', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 3.3, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': 'b7e96a949d3a46d5bce420c96642c53c', 'ts_created': '1363389965380', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 4.8, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': '28cc193567f341e58fcb27d519f96c65', 'ts_created': '1363389965386', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        """
        
    def test_two_sided_interval(self):
        """
        """
        alert_def = {
            'name' : 'current_warning_interval',
            'stream_name' : 'fakestreamname',
            'message' : 'Current is below normal range.',
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
            event_data = alert.eval_alert(x)

        self._async_event_result.get(timeout=30)
        """
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 5.5, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': '631f52d6c22d4861b93255d4bbd41f3a', 'ts_created': '1363389925804', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 10.2, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': 'dd0d081e70404babafaa5da2ac602505', 'ts_created': '1363389925809', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 23.3, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': 'd1848f80e8494ce7b8c3db585a326ad3', 'ts_created': '1363389925815', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.ALL_CLEAR', 'value': 17.5, 'type_': 'StreamAllClearAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': '9d461495cc9642648284c7c261f396b3', 'ts_created': '1363389925821', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'description': '', 'type': 'StreamAlertType.WARNING', 'value': 8.8, 'type_': 'StreamWarningAlertEvent', 'value_id': '', 'base_types': ['StreamAlertEvent', 'ResourceEvent', 'Event'], 'message': 'Current is below normal range.', '_id': 'fabe8864598b41e3ae538817ca5c9300', 'ts_created': '1363389925828', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}
        """
        
    def test_late_data(self):
        """
        """

        def get_state():
            return ResourceAgentState.STREAMING

        alert_def = {
            'name' : 'late_data_warning',
            'stream_name' : 'fakestreamname',
            'message' : 'Expected data has not arrived.',
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
            event_data = alert.eval_alert()
            gevent.sleep(x)

        self._async_event_result.get(timeout=30)
        
        alert.stop()


    def test_rsn_event_alert(self):
        """
        """
        alert_def = {
            'name' : 'input_voltage',
            'message' : 'input_voltage is not in range range.',
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
        "message": "low battery (synthetic event generated from simulator)"
        }

        alert.eval_alert(test_val)

        status = alert.get_status()
        log.debug('test_rsn_event_alert status: %s', alert)

        #self._async_event_result.get(timeout=30)

