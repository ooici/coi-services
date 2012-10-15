#!/usr/bin/env python


from pyon.core.exception import BadRequest, NotFound
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber
from pyon.core.bootstrap import CFG
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from nose.plugins.attrib import attr
from ion.services.cei.scheduler_service import SchedulerService
import gevent
import datetime
from datetime import timedelta
import time
import math
import unittest

class FakeProcess(LocalContextMixin):
    name = 'scheduler_test'
    id = 'scheduler_client'
    process_type = 'simple'

@attr('INT', group='cei')
class TestSchedulerService(IonIntegrationTestCase):

    def setUp(self):
        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 3

        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        process = FakeProcess()
        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=process)
        self.rrclient = ResourceRegistryServiceProcessClient(node=self.container.node, process=process)

    def tearDown(self):
        pass

    def now_utc(self):
        return time.mktime(datetime.datetime.utcnow().timetuple())

    def test_create_interval_timer(self):
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

        event_origin = "Interval_Timer_233"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
        sub.start()

        start_time = self.now_utc()
        self.interval_timer_end_time = start_time + 10
        id = self.ssclient.create_interval_timer(start_time="now" , interval=self.interval_timer_interval,
            end_time=self.interval_timer_end_time,
            event_origin=event_origin, event_subtype="")
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Validate the timer is stored in RR
        ss = self.rrclient.read(id)
        self.assertEqual(ss.entry.event_origin, event_origin)

        # Wait until two events are published
        gevent.sleep((self.interval_timer_interval * 2) + 1)

        #Cancle the timer
        ss = self.ssclient.cancel_timer(id)

        # wait until after next interval to verify that timer was correctly cancelled
        gevent.sleep(self.interval_timer_interval)

        # Validate the timer correctly cancelled
        with self.assertRaises(BadRequest):
            self.ssclient.cancel_timer(id)


        # Validate the timer is removed from resource regsitry
        with self.assertRaises(NotFound):
            self.rrclient.read(id)

        # Validate only 2 events are published
        self.assertEqual(self.interval_timer_count, 2)

    def test_system_restart(self):
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

        event_origin = "Interval_Timer_4444"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.on_restart_callback, origin=event_origin)
        sub.start()

        start_time = self.now_utc()
        self.interval_timer_end_time = start_time + 20
        id = self.ssclient.create_interval_timer(start_time="now" , interval=self.interval_timer_interval,
            end_time=self.interval_timer_end_time,
            event_origin=event_origin, event_subtype="")
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Validate the timer is stored in RR
        ss = self.rrclient.read(id)
        self.assertEqual(ss.entry.event_origin, event_origin)

        # Wait until 1 event is published
        gevent.sleep((self.interval_timer_interval) + 1)
        # Validate 1 event is published
        self.assertEqual(self.interval_timer_count, 1)

        self.ssclient.on_system_restart()

        # after system restart, validate the timer is restored
        ss = self.rrclient.read(id)
        self.assertEqual(ss.entry.event_origin, event_origin)

        # Wait until another event is published
        gevent.sleep((self.interval_timer_interval * 2) + 1)

        # Validate 1 event is published
        self.assertGreater(self.interval_timer_count, 2)

        #Cancle the timer
        ss = self.ssclient.cancel_timer(id)

        # wait until after next interval to verify that timer was correctly cancelled
        gevent.sleep(self.interval_timer_interval)

        # Validate the timer correctly cancelled
        with self.assertRaises(BadRequest):
            self.ssclient.cancel_timer(id)

        # Validate the timer is removed from resource regsitry
        with self.assertRaises(NotFound):
            self.rrclient.read(id)

    def on_restart_callback(self, *args, **kwargs):
        self.interval_timer_count += 1

    def test_create_interval_timer_with_end_time(self):
        # create the interval timer resource
        # create the event listener
        # call scheduler to set the timer
        # receive a few intervals, validate that arrival time is as expected
        # Validate no more events are published after end_time expires
        # Validate the timer was canceled after the end_time expires

        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 2

        event_origin = "Interval Timer"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
        sub.start()

        start_time = self.now_utc()
        self.interval_timer_end_time = start_time + 5
        id = self.ssclient.create_interval_timer(start_time="now" , interval=self.interval_timer_interval,
            end_time=self.interval_timer_end_time,
            event_origin=event_origin, event_subtype="")
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Wait until all events are published
        gevent.sleep((self.interval_timer_end_time - start_time) + self.interval_timer_interval + 1)

        # Validate only 2 events are published
        self.assertEqual(self.interval_timer_count, 2)

        # Validate the timer was canceled after the end_time is expired
        with self.assertRaises(BadRequest):
            self.ssclient.cancel_timer(id)

    def interval_timer_callback(self, *args, **kwargs):
        self.interval_timer_received_time = datetime.datetime.utcnow()
        self.interval_timer_count += 1
        time_diff = math.fabs( ((self.interval_timer_received_time - self.interval_timer_sent_time).total_seconds())
                               - (self.interval_timer_interval * self.interval_timer_count) )
        # Assert expire time is within +-10 seconds
        self.assertTrue(time_diff <= 10)

    def test_cancel_single_timer(self):
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
        event_origin = "Time_of_Day"

        sub = EventSubscriber(event_type="ResourceEvent", callback=self.single_timer_call_back, origin=event_origin)
        sub.start()

        now = datetime.datetime.utcnow() + timedelta(seconds=3)
        times_of_day =[{'hour': str(now.hour),'minute' : str(now.minute), 'second':str(now.second) }]
        id = self.ssclient.create_time_of_day_timer(times_of_day=times_of_day,  expires=self.now_utc()+3, event_origin=event_origin, event_subtype="test")
        self.assertEqual(type(id), str)
        self.ssclient.cancel_timer(id)
        gevent.sleep(3)

        # Validate the event is not generated
        self.assertEqual(self.single_timer_count, 0)


    def single_timer_call_back (self, *args, **kwargs):
        self.single_timer_count =+ 1

    def test_create_forever_interval_timer(self):
        # Test creating interval timer that runs forever

        self.interval_timer_count = 0
        self.interval_timer_sent_time = 0
        self.interval_timer_received_time = 0
        self.interval_timer_interval = 3

        event_origin = "Interval Timer Forever"
        sub = EventSubscriber(event_type="ResourceEvent", callback=self.interval_timer_callback, origin=event_origin)
        sub.start()

        id = self.ssclient.create_interval_timer(start_time= self.now_utc(), interval=self.interval_timer_interval,
            end_time=-1,
            event_origin=event_origin, event_subtype=event_origin)
        self.interval_timer_sent_time = datetime.datetime.utcnow()
        self.assertEqual(type(id), str)

        # Wait for 4 events to be published
        gevent.sleep((self.interval_timer_interval * 4) + 1)
        self.ssclient.cancel_timer(id)

        # Validate the timer id is invalid once it has been canceled
        with self.assertRaises(BadRequest):
            self.ssclient.cancel_timer(id)

        # Validate events are not generated after canceling the timer
        self.assertEqual(self.interval_timer_count, 4)

    def test_timeoffday_timer(self):
        # test creating a new timer that is one-time-only
        # create the timer resource
        # get the current time, set the timer to several seconds from current time
        # create the event listener
        # call scheduler to set the timer
        # verify that  event arrival is within one/two seconds of current time

        event_origin = "Time Of Day2"
        self.expire_sec_1 = 4
        self.expire_sec_2 = 5
        self.tod_count = 0
        expire1 = datetime.datetime.utcnow() + timedelta(seconds=self.expire_sec_1)
        expire2 = datetime.datetime.utcnow() + timedelta(seconds=self.expire_sec_2)
        # Create two timers
        times_of_day =[{'hour': str(expire1.hour),'minute' : str(expire1.minute), 'second':str(expire1.second) },
                       {'hour': str(expire2.hour),'minute' : str(expire2.minute), 'second':str(expire2.second)}]

        sub = EventSubscriber(event_type="ResourceEvent", callback=self.tod_callback, origin=event_origin)
        sub.start()

        # Expires in one days
        expires = time.mktime((datetime.datetime.utcnow() + timedelta(days=2)).timetuple())
        self.tod_sent_time = datetime.datetime.utcnow()
        id = self.ssclient.create_time_of_day_timer(times_of_day=times_of_day, expires=expires, event_origin=event_origin, event_subtype="")
        self.assertEqual(type(id), str)

        # Wait until all events are generated
        gevent.sleep(9)

        # After waiting for 15 seconds, validate only 2 events are generated.
        self.assertTrue(self.tod_count == 2)

        # Cancel the timer
        self.ssclient.cancel_timer(id)


    def tod_callback(self, *args, **kwargs):
        tod_receive_time = datetime.datetime.utcnow()
        self.tod_count += 1
        if self.tod_count == 1:
            time_diff = math.fabs((tod_receive_time - self.tod_sent_time).total_seconds() - self.expire_sec_1)
            self.assertTrue(time_diff <= 2)
        elif self.tod_count == 2:
            time_diff = math.fabs((tod_receive_time - self.tod_sent_time).total_seconds() - self.expire_sec_2)
            self.assertTrue(time_diff <= 2)


