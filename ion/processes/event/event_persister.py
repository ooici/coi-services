#!/usr/bin/env python

"""Process that subscribes to ALL events and persists them efficiently into the events datastore"""

from pyon.core import bootstrap
from pyon.event.event import EventSubscriber
from pyon.ion.process import StandaloneProcess
from pyon.util.async import spawn
from gevent.queue import Queue
from gevent.event import Event
from pyon.public import log


"""
TODO:
- How fast can this receive event messages?
"""


class EventPersister(StandaloneProcess):

    def on_init(self):
        # Time in between event persists
        self.persist_interval = 1.0

        # Holds received events FIFO
        self.event_queue = Queue()

        # Temporarily holds list of events to persist while datastore operation not yet completed
        self.events_to_persist = None

        # bookkeeping for timeout greenlet
        self._persist_greenlet = None
        self._terminate_persist = Event() # when set, exits the timeout greenlet

        # The event subscriber
        self.event_sub = None

    def on_start(self):
        # Persister thread
        self._persist_greenlet = spawn(self._trigger_func, self.persist_interval)
        log.debug('Publisher Greenlet started in "%s"', self.__class__.__name__)

        # Event subscription
        self.event_sub = EventSubscriber(pattern=EventSubscriber.ALL_EVENTS,
                                         callback=self._on_event,
                                         queue_name="event_persister")

        self.event_sub.start()

    def on_quit(self):
        # Stop event subscriber
        self.event_sub.stop()

        # tell the trigger greenlet we're done
        self._terminate_persist.set()

        # wait on the greenlet to finish cleanly
        self._persist_greenlet.join(timeout=10)

    def _on_event(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def _trigger_func(self, persist_interval):
        log.debug('Starting event persister thread with persist_interval=%s', persist_interval)

        # Event.wait returns False on timeout (and True when set in on_quit), so we use this to both exit cleanly and do our timeout in a loop
        while not self._terminate_persist.wait(timeout=persist_interval):
            try:
                self.events_to_persist = [self.event_queue.get() for x in xrange(self.event_queue.qsize())]

                self._persist_events(self.events_to_persist)
                self.events_to_persist = None
            except Exception as ex:
                log.exception("Failed to persist received events")
                return False

    def _persist_events(self, event_list):
        if event_list:
            bootstrap.container_instance.event_repository.put_events(event_list)
