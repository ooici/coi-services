#!/usr/bin/env python

'''
@author: Tim Giguere <tgiguere@asascience.com>
@file: pyon/ion/transform.py
@description: New Implementation for TransformBase class
'''

from pyon.ion.process import SimpleProcess
from pyon.event.event import EventSubscriber, EventPublisher
from pyon.ion.stream import StreamPublisher, StreamSubscriber
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from pyon.net.endpoint import RPCServer, RPCClient
import logging
import gevent
import re
from pyon.util.log import log
from copy import copy
dot = logging.getLogger('dot')

class TransformBase(SimpleProcess):
    '''
    TransformBase is the base class for all Transform Processes
    '''
    def __init__(self):
        super(TransformBase,self).__init__()
        self._stats = {} # Container for statistics information
    
    def on_start(self):
        '''
        Begins listening for incoming RPC calls.
        '''
        super(TransformBase,self).on_start()
        self._rpc_server = self.container.proc_manager._create_listening_endpoint(from_name=self.id,
                                                                                  process=self)
        self.add_endpoint(self._rpc_server)

    def _stat(self):
        return self._stats

    @classmethod
    def stats(cls,pid):
        '''
        RPC Method for querying a Transform's internal statistics
        '''
        rpc_cli = RPCClient(to_name=pid)
        return rpc_cli.request({},op='_stat')

class TransformStreamProcess(TransformBase):
    '''
    Transforms which interact with Ion Streams.
    '''
    def __init__(self):
        super(TransformStreamProcess,self).__init__()
    
    def on_start(self):
        super(TransformStreamProcess,self).on_start()


class TransformEventProcess(TransformBase):
    '''
    Transforms which interact with Ion Events.
    '''
    def __init__(self):
        super(TransformEventProcess,self).__init__()
    def on_start(self):
        super(TransformEventProcess,self).on_start()


class TransformStreamListener(TransformStreamProcess):
    '''
    Transforms which listen to a queue for incoming
    Ion Streams.

    Parameters:
      process.queue_name Name of the queue to listen on.
    '''
    def __init__(self):
        super(TransformStreamListener,self).__init__()

    def on_start(self):
        '''
        Sets up the subscribing endpoint and begins consuming.
        '''
        super(TransformStreamListener,self).on_start()
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)

        self.subscriber = StreamSubscriber(process=self, exchange_name=self.queue_name, callback=self.recv_packet)
        self.add_endpoint(self.subscriber)

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        To be implemented by the transform developer.
        This method is called on receipt of an incoming message from a stream.
        '''
        raise NotImplementedError('Method recv_packet not implemented')

class TransformStreamPublisher(TransformStreamProcess):
    '''
    Transforms which publish on a stream.

    Parameters:
      process.stream_id      Outgoing stream identifier.
      process.exchange_point Route's exchange point.
      process.routing_key    Route's routing key.

    Either the stream_id or both the exchange_point and routing_key need to be provided.
    '''
    def __init__(self):
        super(TransformStreamPublisher,self).__init__()

    def on_start(self):
        '''
        Binds the publisher to the transform
        '''
        super(TransformStreamPublisher,self).on_start()
        self.stream_id      = self.CFG.get_safe('process.stream_id', '')
        self.exchange_point = self.CFG.get_safe('process.exchange_point', 'science_data')
        self.routing_key    = self.CFG.get_safe('process.routing_key', '')

        # We do not want processes to make service calls
        # A StreamPublisher has a behavior built-in to create a stream
        # If no stream_id and route are specified. 
        # We will use the container attached endpoints instead of making a new stream
        if not (self.stream_id or self.routing_key):
            output_streams = copy(self.CFG.get_safe('process.publish_streams'))
            first_stream   = output_streams.popitem()
            try:
                self.publisher = getattr(self,first_stream[0])
            except AttributeError:
                log.warning('no publisher endpoint located')
                self.publisher = None
        else:
            self.publisher = StreamPublisher(process=self, stream_id=self.stream_id, exchange_point=self.exchange_point, routing_key=self.routing_key)

    def publish(self, msg, to_name):
        '''
        To be implemented by the transform developer.
        '''
        raise NotImplementedError('Method publish not implemented')

    def on_quit(self):
        if self.publisher:
            self.publisher.close()
        super(TransformStreamPublisher,self).on_quit()

class TransformEventListener(TransformEventProcess):

    def __init__(self):
        super(TransformEventListener,self).__init__()

    def on_start(self):
        super(TransformEventListener,self).on_start()
        event_type = self.CFG.get_safe('process.event_type', '')
        queue_name = self.CFG.get_safe('process.queue_name', None)

        self.listener = EventSubscriber(event_type=event_type, queue_name=queue_name, callback=self.process_event)
        self.add_endpoint(self.listener)

    def process_event(self, msg, headers):
        raise NotImplementedError('Method process_event not implemented')

class TransformEventPublisher(TransformEventProcess):

    def __init__(self):
        super(TransformEventPublisher,self).__init__()

    def on_start(self):
        super(TransformEventPublisher,self).on_start()
        event_type = self.CFG.get_safe('process.event_type', '')

        self.publisher = EventPublisher(event_type=event_type)

    def publish_event(self, *args, **kwargs):
        raise NotImplementedError('Method publish_event not implemented')

    def on_quit(self):
        self.publisher.close()
        super(TransformEventPublisher,self).on_quit()

class TransformDatasetProcess(TransformBase):

    def __init__(self):
        super(TransformDatasetProcess,self).__init__()

class TransformDataProcess(TransformStreamListener, TransformStreamPublisher):
    '''
    Transforms which have an incoming stream and an outgoing stream.

    Parameters:
      process.stream_id      Outgoing stream identifier.
      process.exchange_point Route's exchange point.
      process.routing_key    Route's routing key.
      process.queue_name     Name of the queue to listen on.
      
    Either the stream_id or both the exchange_point and routing_key need to be provided.
    '''

    def __init__(self):
        super(TransformDataProcess,self).__init__()

    def on_start(self):
        super(TransformDataProcess,self).on_start()
        if dot.isEnabledFor(logging.INFO):
            pubsub_cli = PubsubManagementServiceProcessClient(process=self)
            self.streams = self.CFG.get_safe('process.publish_streams',{})
            for k,v in self.streams.iteritems():
                stream_route = pubsub_cli.read_stream_route(v)
                queue_name = re.sub(r'[ -]', '_', self.queue_name)

                dot.info('   %s -> %s' %( queue_name, stream_route.routing_key.strip('.stream')))

    def on_quit(self):
        super(TransformDataProcess, self).on_quit()

class TransformAlgorithm(object):

    @staticmethod
    def execute(*args, **kwargs):
        raise NotImplementedError('Method execute not implemented')



    
