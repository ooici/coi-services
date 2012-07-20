#!/usr/bin/env python

'''
@author: Tim Giguere <tgiguere@asascience.com>
@file: pyon/ion/transforma.py
@description: New Implementation for TransformBase class
'''

from gevent import spawn

from pyon.core.bootstrap import get_sys_name
from pyon.ion.process import SimpleProcess
from pyon.net.endpoint import Subscriber, Publisher
from pyon.event.event import EventSubscriber, EventPublisher
from ion.services.dm.utility.query_language import QueryLanguage

from pyon.util.log import log
from pyon.ion.stream import SimpleStreamPublisher, SimpleStreamSubscriber

class TransformBase(SimpleProcess):
    def on_start(self):
        super(TransformBase,self).on_start()

class TransformStreamProcess(TransformBase):
    pass

class TransformEventProcess(TransformBase):
    def on_start(self):
        log.warn('TransformDataProcess.on_start()')
        TransformStreamListener.on_start(self)
        TransformStreamPublisher.on_start(self)

class TransformStreamListener(TransformStreamProcess):

    def on_start(self):
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)

        # @TODO: queue_name is really exchange_name, rename
        self.subscriber = SimpleStreamSubscriber.new_subscriber(self.container, self.queue_name, self.recv_packet)
        self.subscriber.start()

    def recv_packet(self, msg, headers):
        raise NotImplementedError('Method recv_packet not implemented')

    def on_quit(self):
        self.subscriber.stop()

class TransformStreamPublisher(TransformStreamProcess):

    def on_start(self):
        self.exchange_point = self.CFG.get_safe('process.exchange_point', '')

        self.publisher = SimpleStreamPublisher.new_publisher(self.container,self.exchange_point,'')

    def publish(self, msg, to_name):
        raise NotImplementedError('Method publish not implemented')

    def on_quit(self):
        self.publisher.close()

class TransformEventListener(TransformEventProcess):

    def on_start(self):

        event_type = self.CFG.get_safe('process.event_type', '')
        event_origin = self.CFG.get_safe('process.event_origin', '')
        event_origin_type = self.CFG.get_safe('process.event_origin_type', '')
        event_subtype = self.CFG.get_safe('process.event_subtype', '')

        self.algorithm = self.CFG.get_safe('process.algorithm')

        self.listener = EventSubscriber(  origin=event_origin,
            origin_type = event_origin_type,
            event_type=event_type,
            sub_type=event_subtype,
            callback=self.process_event   )

        self.listener.start()

    def process_event(self, msg, headers):

        # do something with the event and the algorithm which is a TransformAlgorithm object

        raise NotImplementedError('Method process_event not implemented')

    def on_quit(self):
        self.listener.stop()

class TransformEventPublisher(TransformEventProcess):

    def on_start(self):

        event_type = self.CFG.get_safe('process.event_type', '')
        self.publisher = EventPublisher(event_type=event_type)

    def publish_event(self, *args, **kwargs):

        self.publisher.publish_event(**kwargs)

    def on_quit(self):
        self.publisher.close()

class TransformDatasetProcess(TransformBase):
    pass

class TransformDataProcess(TransformStreamListener, TransformStreamPublisher):

    def on_start(self):
        log.warn('TransformDataProcess.on_start()')
        self.algorithm = self.CFG.get_safe('process.algorithm')

        # pass in the configs to the listener

        # config to the listener (event types etc and the algorithm)

        self.transform_event_listener = TransformEventListener()
        self.transform_event_publisher = TransformEventPublisher()






class TransformAlgorithm(object):
    '''
    An algorithm object. One can make any number of algorithm objects and pass them to
    transforms. They can act on parameters to determine if a particular condition
    is satisfied.
    '''

    def __init__(self, statement = ''):
        self.ql = QueryLanguage()
        self.statement = statement

    def execute(self, *args, **kwargs):

        cond = False

        query_dict = self.ql.parse(self.statement)




        return cond