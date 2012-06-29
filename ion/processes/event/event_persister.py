#!/usr/bin/env python

"""Process that subscribes to ALL events and persists them efficiently into the events datastore"""

import time
from gevent.greenlet import Greenlet

from pyon.core import bootstrap
from pyon.event.event import EventSubscriber
from pyon.ion.process import StandaloneProcess
from pyon.public import log


"""
TODO:
- Make sure this is a singleton or use shared queue
- More than one process
- How fast can this receive event messages?
"""


class EventPersister(StandaloneProcess):

    def on_init(self):
        # Time in between event persists
        self.persist_interval = 1.0

        # Holds received events FIFO
        self.event_queue = []

        # Temporarily holds list of events to persist while datastore operation not yet completed
        self.events_to_persist = None

        # Keeps references to running threads
        self.thread_list = []

        # The event subscriber
        self.event_sub = None

    def on_start(self):
        # Persister thread
        g = Greenlet(self._trigger_func, self.persist_interval)
        g.start()
        log.warn('Publisher Greenlet started in "%s"' % self.__class__.__name__)
        self.thread_list.append(g)

        # Event subscription
        self.event_sub = EventSubscriber(pattern=EventSubscriber.ALL_EVENTS, callback=self._on_event)
        self.event_sub.start()

    def on_quit(self):
        # Stop event subscriber
        self.event_sub.stop()

        # Wait for persister thread to finish persisting to datastore
        while self.events_to_persist is not None:
            log.warn("Persisting events in progress, waiting")
            time.sleep(0.2)

        for greenlet in self.thread_list:
            greenlet.kill()

        events_to_persist = list(self.event_queue)
        del self.event_queue[:]
        self._persist_events(events_to_persist)

    def _on_event(self, event, *args, **kwargs):
        self.event_queue.append(event)

    def _trigger_func(self, persist_interval):
        log.debug('Starting event persister thread with persist_interval=%s', persist_interval)

        while True:
            try:
                self.events_to_persist = list(self.event_queue)

                # Clear contents of queue
                del self.event_queue[:]

                self._persist_events(self.events_to_persist)
                self.events_to_persist = None
            except Exception as ex:
                log.exception("Failed to persist received events")
                return False

            time.sleep(persist_interval)

    def _persist_events(self, event_list):
        if event_list:
            bootstrap.container_instance.event_repository.put_events(event_list)
