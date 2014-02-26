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
    '''
    The goal behind this function is to publish an event so that threads
    can synchronize with it to verify that it was run, regardless of context
    '''
    event_publisher = EventPublisher(OT.GranuleIngestionErrorEvent)
    try:
        event_publisher.publish_event(error_msg='failure')

        raise StandardError('Something you tried to do failed')
    finally:
        event_publisher.close()

