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
from ion.agents.alarms.alarms import IntervalAlarm
from ion.agents.alarms.alarms import DoubleIntervalAlarm
from ion.agents.alarms.alarms import SetMembershipAlarm
from ion.agents.alarms.alarms import UserDefinedAlarm

# bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms
# bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_greater_than_interval
# bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_less_than_interval
# bin/nosetests -s -v --nologcapture ion/agents/alarms/test/test_alarms.py:TestAlarms.test_two_sided_interval

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

        alarm = IntervalAlarm(**kwargs)

        # This sequence will produce 5 alarms:
        # All clear on the first value,
        # Warning on the first 30,
        # All clear on the following 5.5,
        # Warning on the 15.1,
        # All clear on the following 3.3.
        self._event_count = 5
        test_vals = [5.5, 5.4, 5.5, 5.6, 30, 30.4, 5.5, 5.6, 15.1, 15.2,
                     15.3, 3.3, 3.4]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = alarm.eval_alarm(x)
            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
         
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

        alarm = IntervalAlarm(**kwargs)

        # This sequence will produce 5 alarms:
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 3.3, 3.3, 4.5, 4.5, 3.3, 3.3, 4.8]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = alarm.eval_alarm(x)
            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
        
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

        alarm = IntervalAlarm(**kwargs)

        # This sequence will produce 5 alarms.
        self._event_count = 5
        test_vals = [5.5, 5.5, 5.4, 4.6, 4.5, 10.2, 10.3, 10.5, 15.5,
                     23.3, 23.3, 24.8, 17.5, 16.5, 12.5, 8.8, 7.7]

        pub = EventPublisher(event_type="StreamAlarmEvent",
            node=self.container.node)

        for x in test_vals:
            event_data = alarm.eval_alarm(x)
            if event_data:
                pub.publish_event(origin=self._resource_id, **event_data)
        
        self._async_event_result.get(timeout=30)
 
 
 
 
        