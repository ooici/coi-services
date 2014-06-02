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

        self.persist_interval = 300 # interval in seconds to persist/reload counts TODO CFG
        self.time_last_persist # time of last persist

        # initalize volatile counts (memory only, should be routinely persisted)
        self._initialize_counts()

    def process_events(self, event_list):
        counts_updated = False # boolean to check if counts may be persisted
        for e in event_list:
            # skip if not a NotificationEvent
            if e.type_ not in NOTIFICATION_EVENTS:
                continue
            user_id = e.user_id
            notification_id = e.notification_id
            notification_max = e.notification_max
            # initialize user_id if necessary
            if user_id not in self.counts:
                self.counts[user_id] = Counter()
            # increment counts
            self.counts[user_id]['all'] += 1 # tracks total notifications by user
            self.counts[user_id][notification_id] += 1
            counts_updated = True
            # disable notifications if max_notifications reached
            if self.counts[user_id][notification_id] >= notification_max:
                _disable_notification(notification_id)
        # only attempt to persist counts if updated
        if counts_updated:
            if time.time() > (self.time_last_persist + self.persist_interval):
                self._persist_counts()

    def _initalize_counts(self):
        """ initialize the volatile (memory only) counts from ObjectStore if available """
        try:
            self.counts = self.store.read('notification_counts')
            # persisted as standard dicts, convert to Counter objects
            self.counts = {k:Counter(v) for k,v in self.counts.items()}
        except NotFound:
            self.counts = {}
        self._persist_counts()

    def _persist_counts(self):
        """ persist the counts to ObjectStore """
        try:
            persisted_counts = self.store.read('notification_counts')
        except NotFound:
            persisted_counts = {}
            self.store.create('notification_counts', persisted_counts)
        # Counter objects cannot be persisted, convert to standard dicts
        persisted_counts.update({k:dict(v) for k,v in self.counts.items()})
        self.store.update(persisted_counts)
        self.time_last_persist = time.time()

    def _reset_counts(self):
        """ clears the persisted counts """
        self.store.delete('notification_counts')
        self.store.create('notification_counts',{})

    def _disable_notification(notification_id):
        """ set the disabled_by_system boolean to True """
        notification = self.store.read(notification_id)
        notification.disabled_by_system = True
        self.store.update(notification)
        
