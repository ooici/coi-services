#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell(at)ASAScience(dot)com>
@file ion/processes/data/ingestion/ingestion_manager.py
@brief A process to manage balancing for ingestion workers
'''


from pyon.ion.process import SimpleProcess
from pyon.event.event import EventSubscriber

class IngestionManager(SimpleProcess):
    def on_start(self):
        SimpleProcess.on_start(self)

        overflow_listener = EventSubscriber(event_type='IngestionOverflow',queue_name='ingestion_balancer', callback=self.recv_event)
        self.listener = overflow_listener.start()

    def on_quit(self):
        SimpleProcess.on_quit(self)
        self.listener.stop()

    def overflow(self, process_id):
        print '%s is overflowing' % process_id

    def recv_event(self, event, headers):
        self.overflow(event.process_id)



