#!/usr/bin/env python

"""Process that subscribes to ALL events and persists them efficiently into the events datastore"""

from pyon.ion.conversation_log import ConvSubscriber, ConvRepository
from pyon.ion.process import StandaloneProcess
from pyon.util.async import spawn
from gevent.queue import Queue
from gevent.event import Event
from pyon.public import log
from pyon.core import bootstrap
from pyon.public import CFG


"""
TODO:
- Make sure this is a singleton or use shared queue
- More than one process
- How fast can this receive event messages?
"""


class ConversationPersister(StandaloneProcess):

    def on_init(self):

        #get the fields from the header that need to be logged
        self.header_fields = CFG.get_safe('container.messaging.conversation_log.header_fields', [])

        # Time in between event persists
        self.persist_interval = 1.0

        # Holds received events FIFO
        self.conv_queue = Queue()

        # Temporarily holds list of events to persist while datastore operation not yet completed
        self.convs_to_persist = None

        # bookkeeping for timeout greenlet
        self._persist_greenlet = None
        self._terminate_persist = Event() # when set, exits the timeout greenlet

        # The event subscriber
        self.conv_sub = None

        # The conv repository
        self.conv_repository = None

    def on_start(self):
        # Persister thread
        self._persist_greenlet = spawn(self._trigger_func, self.persist_interval)
        log.debug('Publisher Greenlet started in "%s"' % self.__class__.__name__)

        # Conv subscription to as many as it takes
        self.conv_sub = ConvSubscriber(callback=self._on_message)
        self.conv_sub.start()

        # Open repository
        self.conv_repository = ConvRepository()

    def on_quit(self):
        # Stop event subscriber
        self.conv_sub.stop()

        # tell the trigger greenlet we're done
        self._terminate_persist.set()

        # wait on the greenlet to finish cleanly
        self._persist_greenlet.join(timeout=10)

        # Close the repository
        self.conv_repository.close()

    def _on_message(self, msg, *args, **kwargs):
        conv = {}
        unknown = 'unknown'
        conv['sender'] = args[0].get('sender', unknown)
        conv['recipient'] = args[0].get('receiver', unknown)
        conv['conversation_id'] = args[0].get('conv-id', unknown)
        conv['protocol'] = args[0].get('protocol', unknown)
        if (len(self.header_fields) == 0):
            conv['headers'] = args[0]
        else:
            conv['headers'] = {}
            for key in self.header_fields:
                conv['headers'][key] = args[0].get([key], unknown)
        conv_msg = bootstrap.IonObject("ConversationMessage", conv)
        self.conv_queue.put(conv_msg)

    def _trigger_func(self, persist_interval):
        log.debug('Starting conv persister thread with persist_interval=%s', persist_interval)

        # Event.wait returns False on timeout (and True when set in on_quit),
        # so we use this to both exit cleanly and do our timeout in a loop
        while not self._terminate_persist.wait(timeout=persist_interval):
            try:
                self.convs_to_persist = [self.conv_queue.get() for x in xrange(self.conv_queue.qsize())]

                self._persist_convs(self.convs_to_persist)
                self.convs_to_persist = None
            except Exception as ex:
                log.exception("Failed to persist received conversations")
                return False

    def _persist_convs(self, conv_list):
        if conv_list:
            self.conv_repository.put_convs(conv_list)