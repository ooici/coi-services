#!/usr/bin/env python

"""Process that subscribes to ALL events and persists them efficiently in bulk into the events datastore"""

import pprint
from gevent.queue import Queue
from gevent.event import Event

from pyon.event.event import EventSubscriber
from pyon.ion.process import StandaloneProcess
from pyon.util.async import spawn
from pyon.util.containers import named_any
from pyon.public import log

PROCESS_PLUGINS = [("DeviceStateManager", "ion.processes.event.device_state.DeviceStateManager", {}),
                   ("NotificationSentScanner","ion.processes.event.notification_sent_scanner.NotificationSentScanner", {})]


class EventPersister(StandaloneProcess):

    def on_init(self):
        # Time in between event persists
        self.persist_interval = float(self.CFG.get_safe("process.event_persister.persist_interval", 1.0))

        self.persist_blacklist = self.CFG.get_safe("process.event_persister.persist_blacklist", {})

        self._event_type_blacklist = [entry['event_type'] for entry in self.persist_blacklist if entry.get('event_type', None) and len(entry) == 1]
        self._complex_blacklist = [entry for entry in self.persist_blacklist if not (entry.get('event_type', None) and len(entry) == 1)]
        if self._complex_blacklist:
            log.warn("EventPersister does not yet support complex blacklist expressions: %s", self._complex_blacklist)

        # Time in between view refreshs
        self.refresh_interval = float(self.CFG.get_safe("process.event_persister.refresh_interval", 60.0))

        # Holds received events FIFO in syncronized queue
        self.event_queue = Queue()

        # Temporarily holds list of events to persist while datastore operation are not yet completed
        # This is where events to persist will remain if datastore operation fails occasionally.
        self.events_to_persist = None

        # Number of unsuccessful attempts to persist in a row
        self.failure_count = 0

        # bookkeeping for greenlet
        self._persist_greenlet = None
        self._terminate_persist = Event() # when set, exits the persister greenlet
        self._refresh_greenlet = None
        self._terminate_refresh = Event() # when set, exits the refresher greenlet

        # The event subscriber
        self.event_sub = None

        # Registered event process plugins
        self.process_plugins = {}
        for plugin_name, plugin_cls, plugin_args in PROCESS_PLUGINS:
            try:
                plugin = named_any(plugin_cls)(**plugin_args)
                self.process_plugins[plugin_name]= plugin
                log.info("Loaded event processing plugin %s (%s)", plugin_name, plugin_cls)
            except Exception as ex:
                log.error("Cannot instantiate event processing plugin %s (%s): %s", plugin_name, plugin_cls, ex)


    def on_start(self):
        # Persister thread
        self._persist_greenlet = spawn(self._persister_loop, self.persist_interval)
        log.debug('EventPersister persist greenlet started in "%s" (interval %s)', self.__class__.__name__, self.persist_interval)

        # View trigger thread
        self._refresh_greenlet = spawn(self._refresher_loop, self.refresh_interval)
        log.debug('EventPersister view refresher greenlet started in "%s" (interval %s)', self.__class__.__name__, self.refresh_interval)

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
        self._terminate_refresh.set()

        # wait on the greenlets to finish cleanly
        self._persist_greenlet.join(timeout=5)
        self._refresh_greenlet.join(timeout=5)

    def _on_event(self, event, *args, **kwargs):
        self.event_queue.put(event)

    def _in_blacklist(self, event):
        if event.type_ in self._event_type_blacklist:
            return True
        if event.base_types:
            for base_type in event.base_types:
                if base_type in self._event_type_blacklist:
                    return True
            # TODO: Complex event blacklist
        return False

    def _persister_loop(self, persist_interval):
        log.debug('Starting event persister thread with persist_interval=%s', persist_interval)

        # Event.wait returns False on timeout (and True when set in on_quit), so we use this to both exit cleanly and do our timeout in a loop
        while not self._terminate_persist.wait(timeout=persist_interval):
            try:
                # leftover events_to_persist indicate previous attempt did not succeed
                if self.events_to_persist and self.failure_count > 2:
                    bad_events = []
                    log.warn("Attempting to persist %s events individually" % (len(self.events_to_persist)))
                    for event in self.events_to_persist:
                        try:
                            self.container.event_repository.put_event(event)
                        except Exception:
                            bad_events.append(event)

                    if len(self.events_to_persist) != len(bad_events):
                        log.warn("Succeeded to persist some of the events - rest must be bad")
                        self._log_events(bad_events)
                    elif bad_events:
                        log.error("Discarding %s events after %s attempts!!" % (len(bad_events), self.failure_count))
                        self._log_events(bad_events)

                    self.events_to_persist = None
                    self.failure_count = 0

                elif self.events_to_persist:
                    # There was an error last time and we need to retry
                    log.info("Retry persisting %s events" % len(self.events_to_persist))
                    self._persist_events(self.events_to_persist)
                    self.events_to_persist = None

                # process ALL events (not retried on fail like peristing is)
                events_to_process = [self.event_queue.get() for x in xrange(self.event_queue.qsize())]
                # only persist events not in blacklist
                self.events_to_persist = [x for x in events_to_process if not self._in_blacklist(x)]

                try:
                    self._persist_events(self.events_to_persist)
                finally:
                    self._process_events(events_to_process)
                self.events_to_persist = None
                self.failure_count = 0
            except Exception as ex:
                # Note: Persisting events may fail occasionally during test runs (when the "events" datastore is force
                # deleted and recreated). We'll log and keep retrying forever.
                log.exception("Failed to persist %s received events. Will retry next cycle" % len(self.events_to_persist))
                self.failure_count += 1
                self._log_events(self.events_to_persist)

    def _persist_events(self, event_list):
        if event_list:
            self.container.event_repository.put_events(event_list)

    def _process_events(self, event_list):
        for plugin_name, plugin in self.process_plugins.iteritems():
            try:
                plugin.process_events(event_list)
            except Exception as ex:
                log.exception("Error processing events in plugin %s", plugin_name)

    def _log_events(self, events):
        events_str = pprint.pformat([event.__dict__ for event in events]) if events else ""
        log.warn("EVENTS:\n%s", events_str)

    def _refresher_loop(self, refresh_interval):
        log.debug('Starting event view refresher thread with refresh_interval=%s', refresh_interval)

        # Event.wait returns False on timeout (and True when set in on_quit), so we use this to both exit cleanly and do our timeout in a loop
        while not self._terminate_persist.wait(timeout=refresh_interval):
            try:
                self.container.event_repository.find_events(limit=1)
            except Exception as ex:
                log.exception("Failed to refresh events views")


class EventProcessor(object):
    """Callback interface for event processors"""

    def process_events(self, event_list):
        raise NotImplemented("Must override")
