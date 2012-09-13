#!/usr/bin/env python


from pyon.core.exception import BadRequest
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from nose.plugins.attrib import attr
from ion.services.cei.scheduler_service import SchedulerService
import gevent
import datetime
from datetime import timedelta
import time
import math

class FakeProcess(LocalContextMixin):
    name = ''

@attr('seman', group='cei')
class TestSchedulerService(IonIntegrationTestCase):

    def setUp(self):
        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 3

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        pass

    def tearDown(self):
        gevent.sleep(10)

    def test_create_single_timer(self):
        # test creating a new timer that is one-time-only

        # create the timer resource
        # create the event listener
        # call scheduler to set the timer

        # create then cancel the timer, verify that event is not received

        # create the timer resource
        # create the event listener
        # call scheduler to set the timer
        # call scheduler to cancel the timer
        # wait until after expiry to verify that event is not sent
        self.single_timer_count = 0
        event_origin = "Time of Day"

        sub = EventSubscriber(event_type="ResourceEvent", callback=self.single_timer_call_back, origin=event_origin)
        sub.start()

        # Time out in 3 seconds
        now = datetime.datetime.utcnow() + timedelta(seconds=3)
        times_of_day =[{'hour': str(now.hour),'minute' : str(now.minute), 'second':str(now.second) }]
        ss = SchedulerService()
        id = ss.create_time_of_day_timer(times_of_day=times_of_day,  expires=time.time()+25200+60, event_origin=event_origin, event_subtype="")

        self.assertEqual(type(id), str)
        ss.cancel_timer(id)
        gevent.sleep(5)
        # Validate the event is not sent
        self.assertEqual(self.single_timer_count, 0)

    def single_timer_call_back (self, *args, **kwargs):
        self.single_timer_count =+ 1


    def test_create_interval_timer(self):
        # test creating a new timer that is one-time-only

        # create the interval timer resource
        # create the event listener
        # call scheduler to set the timer
        # receive a few intervals, validate that arrival time is as expected
        # cancel the timer
        # wait until after next interval to verify that timer was correctly cancelled

        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 3
        self.interval_timer_number_of_intervals = 4

        event_origin = "Interval Timer"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
        sub.start()

        ss = SchedulerService()
        id = ss.create_interval_timer(start_time= time.time(), interval=self.interval_timer_interval,
                                      number_of_intervals=self.interval_timer_number_of_intervals,
                                      event_origin=event_origin, event_subtype="")
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Wait until two events are published
        gevent.sleep((self.interval_timer_interval * 2) + 1)
        ss.cancel_timer(id)

        # Validate the timer id is invalid once it has been canceled
        with self.assertRaises(BadRequest):
            ss.cancel_timer(id)
        # Wait until all events are generated
        gevent.sleep(self.interval_timer_interval * self.interval_timer_number_of_intervals)
        # Validate events are not generated after canceling the timer
        self.assertEqual(self.interval_timer_count, 2)

    def interval_timer_callback(self, *args, **kwargs):
        self.interval_timer_received_time = datetime.datetime.utcnow()
        self.interval_timer_count += 1
        time_diff = math.fabs( ((self.interval_timer_received_time - self.interval_timer_sent_time).total_seconds())
                                - (self.interval_timer_interval * self.interval_timer_count) )
        # Assert expire time is within +-2 seconds
        self.assertTrue(time_diff <= 2)

    def test__create_forever_interval_timer(self):
        # Test creating interval timer that runs forever

        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 3
        self.interval_timer_number_of_intervals = -1

        event_origin = "Interval Timer Forever"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
        sub.start()

        ss = SchedulerService()
        id = ss.create_interval_timer(start_time= time.time(), interval=self.interval_timer_interval,
                                      number_of_intervals=self.interval_timer_number_of_intervals,
                                      event_origin=event_origin, event_subtype="")
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Wait for 5 events to be published
        gevent.sleep((self.interval_timer_interval * 5) + 1)
        ss.cancel_timer(id)

        # Validate the timer id is invalid once it has been canceled
        with self.assertRaises(BadRequest):
            ss.cancel_timer(id)

        # Validate events are not generated after canceling the timer
        self.assertEqual(self.interval_timer_count, 5)




    def test_timeoffday_timer(self):
        # test creating a new timer that is one-time-only
        # create the timer resource
        # get the current time, set the timer to several seconds from current time
        # create the event listener
        # call scheduler to set the timer
        # verify that  event arrival is within one/two seconds of current time

        ss = SchedulerService()
        event_origin = "Time Of Day2"
        self.expire_sec_1 = 4
        self.expire_sec_2 = 4
        self.tod_count = 0
        expire1 = datetime.datetime.utcnow() + timedelta(seconds=self.expire_sec_1)
        expire2 = datetime.datetime.utcnow() + timedelta(seconds=self.expire_sec_2)
        # Create two timers
        times_of_day =[{'hour': str(expire1.hour),'minute' : str(expire1.minute), 'second':str(expire1.second) },
                       {'hour': str(expire2.hour),'minute' : str(expire2.minute), 'second':str(expire2.second)}]

        sub = EventSubscriber(event_type="ResourceEvent", callback=self.tod_callback, origin=event_origin)
        sub.start()
        # Expires in one days
        e = time.mktime((datetime.datetime.utcnow() + timedelta(days=1)).timetuple())
        self.tod_sent_time = datetime.datetime.utcnow()
        id = ss.create_time_of_day_timer(times_of_day=times_of_day, expires=e, event_origin=event_origin, event_subtype="")
        self.assertEqual(type(id), str)
        gevent.sleep(15)
        # After waiting for 15 seconds, validate only 2 events are generated.
        self.assertTrue(self.tod_count == 2)


    def tod_callback(self,*args, **kwargs):
        tod_receive_time = datetime.datetime.utcnow()
        self.tod_count += 1
        if self.tod_count == 1:
            time_diff = math.fabs((tod_receive_time - self.tod_sent_time).total_seconds() - self.expire_sec_1)
            self.assertTrue(time_diff <= 2)
        elif self.tod_count == 2:
            time_diff = math.fabs((tod_receive_time - self.tod_sent_time).total_seconds() - self.expire_sec_2)
            self.assertTrue(time_diff <= 2)


