#!/usr/bin/env python

__author__ = 'Michael Meisinger'
__license__ = 'Apache 2.0'

import uuid

import gevent

from pyon.public import log, PRED
from pyon.core.exception import NotFound, BadRequest
from pyon.event.event import EventPublisher

from interface.services.cei.ischeduler_service import BaseSchedulerService

class SchedulerService(BaseSchedulerService):


    def start_timer(self, timer_schedule=None):
        """
        Create a timer which will send TimerEvents as requested for a given schedule.
        The schedule request is expressed through a specific subtype of TimerSchedulerEntry.
        The event is delivered as a TimeEvent to which processes can subscribe. The creator
        defines the fields of the event. A GUID-based id prefixed by readable process name
        is recommended for the origin. Because the delivery of the event is via the ION Exchange
        there is potential for a small deviation in precision.
        Returns a timer_id which can be used to cancel the timer.

        @param timer_schedule    TimerSchedulerEntry
        @retval timer_id    str
        @throws BadRequest    if timer is misformed and can not be scheduled
        """
        pass

    def cancel_timer(self, timer_id=''):
        """
        Cancels an existing timer which has not reached its expire time.

        @param timer_id    str
        @throws NotFound    if timer_id doesn't exist
        """
        pass

  