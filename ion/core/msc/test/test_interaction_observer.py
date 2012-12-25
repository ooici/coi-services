#!/usr/bin/env python

__author__ = 'Prashant Kediyal <pkediyal@ucsd.edu>'
__license__ = 'Apache 2.0'

import time

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from pyon.net.endpoint import Publisher
from pyon.event.event import EventPublisher
from ion.core.msc.interaction_observer import InteractionObserver

@attr('INT',group='mscweb')
class TestMSCWebProcess(IonIntegrationTestCase):

    def setUp(self):

        self._start_container()
        self.container.start_rel_from_url()

    def test_sub(self):

        #start interaction observer
        io = InteractionObserver()
        io.start()

        # publish a message
        msg_pub = Publisher()
        msg_pub.publish(to_name='anyone', msg="msg")

        #publish an event
        ev_pub = EventPublisher(event_type="ResourceEvent")
        ev_pub.publish_event(origin="specific", description="event")


        # give 2 seconds for the messages to arrive
        time.sleep(2)

        #verify that two messages (an event and a message) are seen
        self.assertEquals(len(io.msg_log), 2)

        #iterate through the messages observed
        for item in io.msg_log:
            # if event
            if item[2]:
                #verify that the origin is what we sent
                self.assertEquals(item[1]['origin'], 'specific')
