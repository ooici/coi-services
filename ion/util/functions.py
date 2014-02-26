#!/usr/bin/env python

'''
Contains generic functions

The initial purpose was to play home to parameter functions that 
can be used for testing and non-scientific purposes
'''

__author__ = "Luke"

from pyon.ion.event import EventPublisher
from pyon.public import OT


def fail(x):
    event_publisher = EventPublisher(OT.GranuleIngestionErrorEvent)
    try:
        event_publisher.publish_event(error_msg='failure')

        raise StandardError('Something you tried to do failed')
    finally:
        event_publisher.close()

