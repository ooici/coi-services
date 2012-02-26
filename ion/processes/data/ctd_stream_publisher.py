#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher')

'''
from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time

import random

from prototype.sci_data.ctd_stream import ctd_stream_packet, ctd_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

class SimpleCtdPublisher(StandaloneProcess):
    def __init__(self, *args, **kwargs):
        super(StandaloneProcess, self).__init__(*args,**kwargs)
        #@todo Init stuff

    def on_start(self):
        '''
        Creates a publisher for each stream_id passed in as publish_streams
        Creates an attribute with the name matching the stream name which corresponds to the publisher
        ex: say we have publish_streams:{'output': my_output_stream_id }
          then the instance has an attribute output which corresponds to the publisher for the stream
          in my_output_stream_id
        '''

        # Get the stream(s)
        stream_id = self.CFG.get('process',{}).get('stream_id','')

        self.greenlet_queue = []

        # Stream creation is done in SA, but to make the example go for demonstration create one here if it is not provided...
        if not stream_id:

            ctd_def = ctd_stream_definition(stream_id=stream_id)
            pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
            stream_id = pubsub_cli.create_stream(
                name='Example CTD Data',
                stream_definition=ctd_def,
                original=True,
                encoding='ION R2')

        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)
        # Needed to get the originator's stream_id
        self.stream_id= stream_id


        self.publisher = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)


        self.last_time = 0


        g = Greenlet(self._trigger_func, stream_id)
        log.debug('Starting publisher thread for simple ctd data.')
        g.start()
        self.greenlet_queue.append(g)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(SimpleCtdPublisher,self).on_quit()


    def _trigger_func(self, stream_id):

        while True:

            length = random.randint(1,20)

            c = [random.uniform(0.0,75.0)  for i in xrange(length)]

            t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

            p = [random.lognormvariate(1,2) for i in xrange(length)]

            lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

            lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

            tvar = [self.last_time + i for i in xrange(1,length+1)]

            self.last_time = max(tvar)

            ctd_packet = ctd_stream_packet(stream_id=stream_id,
                c=c, t=t, p=p, lat=lat, lon=lon, time=tvar)

            log.warn('SimpleCtdPublisher sending %d values!' % length)
            self.publisher.publish(ctd_packet)

            time.sleep(2.0)

class PointCtdPublisher(StandaloneProcess):

    #overriding trigger function here to use PointSupplementConstructor
    def _trigger_func(self, stream_id):

        point_def = ctd_stream_definition(stream_id=stream_id)
        point_constructor = PointSupplementConstructor(point_definition=point_def)

        while True:

            length = 1

            c = [random.uniform(0.0,75.0)  for i in xrange(length)]

            t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

            p = [random.lognormvariate(1,2) for i in xrange(length)]

            lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

            lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

            tvar = [self.last_time + i for i in xrange(1,length+1)]

            self.last_time = max(tvar)

            point_id = point_constructor.add_point(time=tvar,location=(lon[0],lat[0]))
            point_constructor.add_point_coverage(point_id=point_id, coverage_id='temperature', values=t)
            point_constructor.add_point_coverage(point_id=point_id, coverage_id='pressure', values=p)
            point_constructor.add_point_coverage(point_id=point_id, coverage_id='conductivity', values=c)

            ctd_packet = point_constructor.get_stream_granule()

            log.warn('SimpleCtdPublisher sending %d values!' % length)
            self.publisher.publish(ctd_packet)

            time.sleep(2.0)