#!/usr/bin/env python

"""
@package ion.processes.event
@file ion/processes/event/notification_sent_scanner.py
@author Brian McKenna <bmckenna@asascience.com>
@brief NotificationSentScanner plugin. An EventPersister plugin scanning for, and keeping state(count) of, NotificationEvent's
"""

from collections import Counter
from pyon.core import bootstrap
from pyon.public import log, OT

NOTIFICATION_EVENTS = {OT.NotificationSentEvent}

class NotificationSentScanner(object):

    def __init__(self, container=None):
        self.container = container or bootstrap.container_instance
        self.store = self.container.object_store
        # initalize volatile counts (memory only, should be routinely persisted)
        self._initialize_counts()

    def process_events(self, event_list):
        for e in event_list:
            # skip if not a NotificationEvent
            if e.type_ not in NOTIFICATION_EVENTS:
                continue
            user_id = e.user_id
            notification_request_id = e.notification_request_id
            notification_request_max = e.notification_request_max
            # initialize user_id if necessary
            if user_id not in self.counts:
                self.counts[user_id] = Counter() # TODO:will Counter persist?
            # increment counts
            self.counts[user_id]['all'] += 1 # tracks total notifications by user
            self.counts[user_id][notification_request_id] += 1
            # disable notifications if max_notifications reached
            if self.counts[user_id][notification_request_id] >= notification_request_max:
                _disable_notification_request(notification_request_id) #TODO

    def _initalize_counts(self):
        """ initialize the volatile (memory only) counts """
        try:
            self.counts = self.store.read('notification_counts')
        except NotFound:
            self.counts = {}

    def _persist_counts(self):
        """ persist the counts to ObjectStore """
        try:
            persisted_counts = self.store.read('notification_counts')
        except NotFound:
            persisted_counts = {}
            self.store.create('notification_counts',persisted_counts)
        persisted_counts.update(self.counts)
        self.store.update(persisted_counts)

    def _reset_counts(self):
        """ clears the persisted counts """
        self.store.delete('notification_counts')
        self.store.create('notification_counts',{})
