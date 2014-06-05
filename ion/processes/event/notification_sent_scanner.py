#!/usr/bin/env python

"""
@package ion.processes.event
@file ion/processes/event/notification_sent_scanner.py
@author Brian McKenna <bmckenna@asascience.com>
@brief NotificationSentScanner plugin. An EventPersister plugin scanning for, and keeping state(count) of, NotificationEvent's
"""

import time
from datetime import date, datetime, timedelta
from collections import Counter
from pyon.container.cc import Container
from pyon.core import bootstrap
from pyon.core.exception import NotFound
from pyon.event.event import EventPublisher
from pyon.public import log, OT

NOTIFICATION_EVENTS = {OT.NotificationSentEvent}

class NotificationSentScanner(object):

    def __init__(self, container=None):

        #self.container = container or bootstrap.container_instance
        self.container = container or Container.instance
        self.object_store = self.container.object_store
        self.resource_registry = self.container.resource_registry
        self.event_publisher = EventPublisher()

        # next_midnight is used to flush the counts (see NOTE in method)
        self.next_midnight = self._midnight(days=1)

        self.persist_interval = 300 # interval in seconds to persist/reload counts TODO: use CFG
        self.time_last_persist = 0

        # initalize volatile counts (memory only, should be routinely persisted)
        self._initialize_counts()

    def process_events(self, event_list):
        counts_updated = False # boolean to check if counts may be persisted
        notifications = [] # list of notifications to disable
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
            # disable notification if notification_max reached
            if self.counts[user_id][notification_id] >= notification_max:
                notifications.append(_disable_notification(notification_id))
        # update notifications that have been disabled
        if notifications:
            self._update_notification(notifications)
        # only attempt to persist counts if updated
        if counts_updated:
            if time.time() > (self.time_last_persist + self.persist_interval):
                self._persist_counts()
        # reset counts if reset_interval has elapsed
        if time.time() > self.next_midnight:
            self._reset_counts()

    def _initialize_counts(self):
        """ initialize the volatile (memory only) counts from ObjectStore if available """
        try:
            self.counts = self.object_store.read('notification_counts')
            # persisted as standard dicts, convert to Counter objects
            self.counts = {k:Counter(v) for k,v in self.counts.items()}
        except NotFound:
            self.counts = {}
        self._persist_counts()

    def _persist_counts(self):
        """ persist the counts to ObjectStore """
        try:
            persisted_counts = self.object_store.read('notification_counts')
        except NotFound:
            persisted_counts = {}
            self.object_store.create('notification_counts', persisted_counts)
        # Counter objects cannot be persisted, convert to standard dicts
        persisted_counts.update({k:dict(v) for k,v in self.counts.items()})
        self.object_store.update(persisted_counts)
        self.time_last_persist = time.time()

    def _reset_counts(self):
        """ clears the persisted counts """
        self.object_store.delete('notification_counts')
        self.object_store.create('notification_counts',{})
        self._initialize_counts() # NOTE: NotificationRequest boolean disabled_by_system reset by UNS
        self.next_midnight = self._midnight(days=1)

    def _disable_notification(notification_id):
        """ set the disabled_by_system boolean to True """
        notification = self.object_store.read(notification_id)
        notification.disabled_by_system = True
        return notification

    def _update_notifications(notifications):
        """ updates notifications and publishes ReloadUserInfoEvent """
        self.resource_registry.update_mult(notifications)
        self.event_publisher.publish_event(event_type=OT.ReloadUserInfoEvent)

    def _midnight(self, days=0):
        """ NOTE: this is midnight PDT (+0700) """
        dt = datetime.combine(date.today(), datetime.min.time()) + timedelta(days=days,hours=7)
        return (dt - datetime.utcfromtimestamp(0)).total_seconds()

