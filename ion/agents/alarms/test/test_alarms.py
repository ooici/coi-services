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

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Event pubsub support.
from pyon.event.event import EventSubscriber, EventPublisher

# Alarm types and events.
from interface.objects import StreamAlarmType

# Alarm objects.
from pyon.public import IonObject
from ion.agents.alarms.alarms import IntervalAlarm
from ion.agents.alarms.alarms import DoubleIntervalAlarm
from ion.agents.alarms.alarms import SetMembershipAlarm
from ion.agents.alarms.alarms import UserDefinedAlarm
from ion.agents.alarms.alarms import construct_alarm_expression
from ion.agents.alarms.alarms import eval_alarm
from ion.agents.alarms.alarms import make_event_data


"""
bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms
bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_greater_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_less_than_interval
bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_two_sided_interval
"""

TEST_ION_OBJECTS=True

class TestAlarms(IonIntegrationTestCase):
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
        
        def consume_event(*args, **kwargs):
            log.info('Test recieved ION event: args=%s, kwargs=%s, event=%s.', 
                     str(args), str(kwargs), str(args[0]))
            self._events_received.append(args[0])
            if self._event_count > 0 and \
                self._event_count == len(self._events_received):
                self._async_event_result.set()
            
            
        self._event_subscriber = EventSubscriber(
            event_type='StreamAlarmEvent', callback=consume_event,
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
        test_greater_than_interval
        Test interval alarm and alarm event publishing for a greater than
        inteval.
        """

        kwargs = {
            'name' : 'current_warning_interval',
            'stream_name' : 'fakestreamname',
            'value_id' : 'port_current',
            'message' : 'Current is above normal range.',
            'type' : StreamAlarmType.WARNING,
            'lower_bound' : 10.5,
            'lower_rel_op' : '<'
        }

        # Create alarm object.
        alarm = IonObject('IntervalAlarmDef', **kwargs)
        alarm = construct_alarm_expression(alarm)

        # This sequence will produce 5 alarms:
        # Warning on the first value,
        # All clear on 30,
        # Warning on 5.5
        # All clear on 15.1
        # Warning on 3.3
        self._event_count = 5
        test_vals = [5.5, 5.4, 5.5, 5.6, 30, 30.4, 5.5, 5.6, 15.1, 15.2,
                     15.3, 3.3, 3.4]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = None
            eval_alarm(alarm, x)
            if alarm.first_time == 1:
                event_data = make_event_data(alarm)
                
            elif alarm.first_time > 1:
                if alarm.status != alarm.old_status:
                    event_data = make_event_data(alarm)

            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
        
        """
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.5<x', 'value': 5.5, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'port_current', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '8e424a0cde254c0e97d0b7556ce8f7ee', 'ts_created': '1362071937165', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': '10.5<x', 'value': 30, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'port_current', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': '7471fd2e6dc347bcbde041af48476c31', 'ts_created': '1362071937171', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.5<x', 'value': 5.5, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'port_current', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': '612d2e71c0b44fbb87dba12d9a8dc27b', 'ts_created': '1362071937177', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': '10.5<x', 'value': 15.1, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'port_current', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': 'd1d3481807364678825f23eaa0c25080', 'ts_created': '1362071937183', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.5<x', 'value': 3.3, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'port_current', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Current is above normal range.', '_id': 'dabf102c62c343d790d3eb7473e63295', 'ts_created': '1362071937189', 'sub_type': '', 'origin_type': '', 'name': 'current_warning_interval'}.
        """
        
    def test_less_than_interval(self):
        """
        test_less_than_interval
        Test interval alarm and alarm event publishing for a less than
        inteval.
        """

        kwargs = {
            'name' : 'reserve_power_warning',
            'stream_name' : 'fakestreamname',
            'value_id' : 'battery_level',
            'message' : 'Battery is below normal range.',
            'type' : StreamAlarmType.WARNING,
            'upper_bound' : 4.0,
            'upper_rel_op' : '<'            
        }

        # Create alarm object.
        alarm = IonObject('IntervalAlarmDef', **kwargs)
        alarm = construct_alarm_expression(alarm)

        # This sequence will produce 5 alarms:
        # 5.5 warning
        # 3.3 all clear
        # 4.5 warning
        # 3.3 all clear
        # 4.8 warning
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 3.3, 3.3, 4.5, 4.5, 3.3, 3.3, 4.8]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = None
            eval_alarm(alarm, x)
            if alarm.first_time == 1:
                event_data = make_event_data(alarm)
                
            elif alarm.first_time > 1:
                if alarm.status != alarm.old_status:
                    event_data = make_event_data(alarm)

            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
        
        """        
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': 'x<4.0', 'value': 5.5, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'battery_level', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Battery is below normal range.', '_id': '3cd266d62afd442bb50340eb135e00a2', 'ts_created': '1362080735202', 'sub_type': '', 'origin_type': '', 'name': 'reserve_power_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': 'x<4.0', 'value': 3.3, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'battery_level', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': '138ef5dabf0547fabcea8a9b02c67ec8', 'ts_created': '1362080735210', 'sub_type': '', 'origin_type': '', 'name': 'reserve_power_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': 'x<4.0', 'value': 4.5, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'battery_level', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Battery is below normal range.', '_id': '0a5b6d8f187047cbb3c66b449c4f7cec', 'ts_created': '1362080735216', 'sub_type': '', 'origin_type': '', 'name': 'reserve_power_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': 'x<4.0', 'value': 3.3, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'battery_level', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': 'ff96853728b24280b64f96415649e5e3', 'ts_created': '1362080735223', 'sub_type': '', 'origin_type': '', 'name': 'reserve_power_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': 'x<4.0', 'value': 4.8, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'battery_level', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Battery is below normal range.', '_id': 'e2104f3119eb498e93b0661cf8616df4', 'ts_created': '1362080735229', 'sub_type': '', 'origin_type': '', 'name': 'reserve_power_warning'}.
        """
        
    def test_two_sided_interval(self):
        """
        test_two_sided_interval
        Test interval alarm and alarm event publishing for a closed
        inteval.
        """

        kwargs = {
            'name' : 'temp_high_warning',
            'stream_name' : 'fakestreamname',
            'value_id' : 'temp',
            'message' : 'Temperature is above normal range.',
            'type' : StreamAlarmType.WARNING,
            'lower_bound' : 10.0,
            'lower_rel_op' : '<',
            'upper_bound' : 20.0,
            'upper_rel_op' : '<'            
        }

        # Create alarm object.
        alarm = IonObject('IntervalAlarmDef', **kwargs)
        alarm = construct_alarm_expression(alarm)

        # This sequence will produce 5 alarms.
        # 5.5 warning
        # 10.2 all clear
        # 23.3 warning
        # 17.5 all clear
        # 8.8 warning
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 10.2, 10.3, 10.5, 15.5,
                     23.3, 23.3, 24.8, 17.5, 16.5, 12.5, 8.8, 7.7]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = None
            eval_alarm(alarm, x)
            if alarm.first_time == 1:
                event_data = make_event_data(alarm)
                
            elif alarm.first_time > 1:
                if alarm.status != alarm.old_status:
                    event_data = make_event_data(alarm)

            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
 
        """ 
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.0<x<20.0', 'value': 5.5, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Temperature is above normal range.', '_id': 'bc9dec259e3049c4b06d1d965c3f33c8', 'ts_created': '1362080871789', 'sub_type': '', 'origin_type': '', 'name': 'temp_high_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': '10.0<x<20.0', 'value': 10.2, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': '7480eabc9e79428495d6041577b70d6c', 'ts_created': '1362080871794', 'sub_type': '', 'origin_type': '', 'name': 'temp_high_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.0<x<20.0', 'value': 23.3, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Temperature is above normal range.', '_id': 'af2b146e3c944e3587bb4805703bda36', 'ts_created': '1362080871801', 'sub_type': '', 'origin_type': '', 'name': 'temp_high_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.ALL_CLEAR', 'description': '', 'expr': '10.0<x<20.0', 'value': 17.5, 'type_': 'StreamAllClearAlarmEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Alarm is cleared.', '_id': '96e857953fb04dd7a89ce4fec488d31f', 'ts_created': '1362080871806', 'sub_type': '', 'origin_type': '', 'name': 'temp_high_warning'}.
        {'origin': 'abc123', 'stream_name': 'fakestreamname', 'type': 'StreamAlaramType.WARNING', 'description': '', 'expr': '10.0<x<20.0', 'value': 8.8, 'type_': 'StreamWarningAlaramEvent', 'value_id': 'temp', 'base_types': ['StreamAlarmEvent', 'ResourceEvent', 'Event'], 'message': 'Temperature is above normal range.', '_id': '98676d83472b453ea17e9ed6d388206b', 'ts_created': '1362080871814', 'sub_type': '', 'origin_type': '', 'name': 'temp_high_warning'}.
        """

 
        