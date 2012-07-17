#!/usr/bin/env python

from pyon.public import Container, log, IonObject
from pyon.public import RT
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='eoi')
@unittest.skip('not completed')
class TestSchedulerService(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)

    def tearDown(self):
        pass


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


        pass


    def test_create_interval_timer(self):
        # test creating a new timer that is one-time-only

        # create the interval timer resource
        # create the event listener
        # call scheduler to set the timer
        # receive a few intervals, validate that arrival time is as expected
        # cancel the timer
        # wait until after next interval to verify that timer was correctly cancelled

        pass

    
    def test_timeoffday_timer(self):
        # test creating a new timer that is one-time-only

        # create the timer resource
        # get the current time, set the timer to several seconds from current time
        # create the event listener
        # call scheduler to set the timer
        # verify that  event arrival is within one/two seconds of current time
        pass