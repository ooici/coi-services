#!/usr/bin/env python

__author__ = 'Edward Hunter'

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.agent.instrument_fsm import ThreadSafeFSM
from pyon.agent.common import BaseEnum

from nose.plugins.attrib import attr
from mock import patch
import gevent

import unittest
import time


class MyStates(BaseEnum):
    STATE_1 = 'STATE_1'
    STATE_2 = 'STATE_2'

class MyEvents(BaseEnum):
    EVENT_1 = 'EVENT_1'
    EVENT_2 = 'EVENT_2'
    ENTER = 'EVENT_ENTER'
    EXIT = 'EVENT_EXIT'

"""
bin/nosetests -s -v --nologcapture ion/agents/instrument/test/test_fsm.py:TestThreadSafeFSM.test_state
"""

@attr('UNIT')
class TestThreadSafeFSM(PyonTestCase):

    def setUp(self):
        
        self.fsm = ThreadSafeFSM(MyStates, MyEvents, MyEvents.ENTER, MyEvents.EXIT)
        
        self.fsm.add_handler(MyStates.STATE_1, MyEvents.EVENT_1, self._handler_s1_e1)
        self.fsm.add_handler(MyStates.STATE_1, MyEvents.EVENT_2, self._handler_s1_e2)
        self.fsm.add_handler(MyStates.STATE_2, MyEvents.EVENT_1, self._handler_s2_e1)
        self.fsm.add_handler(MyStates.STATE_2, MyEvents.EVENT_2, self._handler_s2_e2)
        
        self.test_value = None
        
        self.fsm.start(MyStates.STATE_1)

    def _handler_s1_e1(self, value, *args, **kwargs):
        start_time = time.time()
        gevent.sleep(0)
        self.test_value = value
        end_time = time.time()
        return (None, (start_time, end_time))
    
    def _handler_s1_e2(self, *args, **kwargs):
        return (None, None)

    def _handler_s2_e1(self, *args, **kwargs):
        return (None, None)

    def _handler_s2_e2(self, *args, **kwargs):
        return (None, None)
    
    def test_state(self):
        
        gl1 = gevent.spawn(self.fsm.on_event, MyEvents.EVENT_1, 100)
        gl2 = gevent.spawn(self.fsm.on_event, MyEvents.EVENT_1, 200)
                
        (g1_start, g1_end) = gl1.get()
        print str(self.test_value)
        self.assertEquals(self.test_value, 100)
        (g2_start, g2_end) = gl2.get()
        print str(self.test_value)
        self.assertEquals(self.test_value, 200)
        self.assertGreater(g2_start, g1_end)
        
    

