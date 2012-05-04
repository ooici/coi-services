#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
bin/pycc --rel res/deploy/r2dm.yml
### In the shell...

# create a stream id and pass it in...
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
pmsc = PubsubManagementServiceClient(node=cc.node)
stream_id = pmsc.create_stream(name='pfoo')
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher',config={'process':{'stream_id':stream_id}})


OR...

# just let the simple ctd publisher create it on its own for simple cases...
cc.spawn_process(name="viz_data_realtime", module="ion.processes.data.ctd_stream_publisher", cls="SimpleCtdPublisher")
'''
from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time
from uuid import uuid4
import random

from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

### For new granule and stream interface

from prototype.coverage.granule_and_record import build_granule, RecordDictionaryTool
from prototype.coverage.taxonomy import TaxyCab
from interface.objects import Granule, Taxonomy



class SimpleCtdPublisher(StandaloneProcess):
    def __init__(self, *args, **kwargs):
        super(SimpleCtdPublisher, self).__init__(*args,**kwargs)
        #@todo Init stuff

    outgoing_stream_def = SBE37_CDM_stream_definition()


    def on_start(self):


        log.warn('Entering On Start!!!')
        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id',{})

        self.greenlet_queue = []


        # Stream creation is done in SA, but to make the example go for demonstration create one here if it is not provided...
        if not stream_id:

            pubsub_cli = PubsubManagementServiceClient(node=self.container.node)

            stream_def_id = pubsub_cli.create_stream_definition(name='Producer stream %s' % str(uuid4()),container=self.outgoing_stream_def)


            stream_id = pubsub_cli.create_stream(
                name='Example CTD Data',
                stream_definition_id = stream_def_id,
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
        log.warn('Publisher Greenlet started in "%s"' % self.__class__.__name__)
        self.greenlet_queue.append(g)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(SimpleCtdPublisher,self).on_quit()


    def _trigger_func(self, stream_id):

        while True:

            length = random.randint(1,20)

            ctd_packet = self._get_ctd_packet(stream_id, length)

            log.info('SimpleCtdPublisher sending %d values!' % length)
            self.publisher.publish(ctd_packet)

            time.sleep(2.0)

    def _get_ctd_packet(self, stream_id, length):

        c = [random.uniform(0.0,75.0)  for i in xrange(length)]

        t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

        p = [random.lognormvariate(1,2) for i in xrange(length)]

        lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

        lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

        tvar = [self.last_time + i for i in xrange(1,length+1)]

        self.last_time = max(tvar)

        ctd_packet = ctd_stream_packet(stream_id=stream_id,
            c=c, t=t, p=p, lat=lat, lon=lon, time=tvar)

        return ctd_packet

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

            log.info('SimpleCtdPublisher sending %d values!' % length)
            self.publisher.publish(ctd_packet)

            time.sleep(2.0)

# New Stream and Granule Stuff....
'''
To Run:
bin/pycc --rel res/deploy/r2dm.yml
### In the shell...

# create a stream id and pass it in...
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
pmsc = PubsubManagementServiceClient(node=cc.node)
stream_id = pmsc.create_stream(name='pfoo')
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='NewGranuleCTDPublisher',config={'process':{'stream_id':stream_id}})


'''


### Taxonomies are defined before hand out of band... somehow.
tx = TaxyCab()
tx.add_taxonomy_set('temp','long name for temp')
tx.add_taxonomy_set('cond','long name for cond')
tx.add_taxonomy_set('lat','long name for latitude')
tx.add_taxonomy_set('lon','long name for longitude')
tx.add_taxonomy_set('pres','long name for pres')
tx.add_taxonomy_set('time','long name for time')
# This is an example of using groups it is not a normative statement about how to use groups
tx.add_taxonomy_set('group1','This group contains coordinates...')
tx.add_taxonomy_set('group0','This group contains data...')

class NewGranuleCTDPublisher(SimpleCtdPublisher):

    #overriding trigger function here to use PointSupplementConstructor
    def _trigger_func(self, stream_id):

        while True:

            length = 1

            c = [random.uniform(0.0,75.0)  for i in xrange(length)]

            t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

            p = [random.lognormvariate(1,2) for i in xrange(length)]

            lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

            lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

            tvar = [self.last_time + i for i in xrange(1,length+1)]

            self.last_time = max(tvar)

            rdt = RecordDictionaryTool(taxonomy=tx)

            # This is an example of using groups it is not a normative statement about how to use groups

            rdt0 = RecordDictionaryTool(taxonomy=tx)

            rdt0['temp'] = t
            rdt0['cond'] = c
            rdt0['pres'] = p

            rdt1 = RecordDictionaryTool(taxonomy=tx)

            rdt1['time'] = tvar
            rdt1['lat'] = lat
            rdt1['lon'] = lon

            rdt['group1'] = rdt1
            rdt['group0'] = rdt0

            g = build_granule(data_producer_id='Bobs Potatoes', taxonomy=tx, record_dictionary=rdt)

            log.info('%s sending %d values!' % (self.__class__.__name__, length))
            self.publisher.publish(g)

            time.sleep(2.0)
