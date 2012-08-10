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
from pyon.ion.stream import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time
from uuid import uuid4
import random
import math

from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.taxonomy import TaxyTool
from ion.services.dm.utility.granule.granule import build_granule
import numpy


### Taxonomies are defined before hand out of band... somehow.
tx = TaxyTool()
tx.add_taxonomy_set('temp','long name for temp')
tx.add_taxonomy_set('cond','long name for cond')
tx.add_taxonomy_set('lat','long name for latitude')
tx.add_taxonomy_set('lon','long name for longitude')
tx.add_taxonomy_set('pres','long name for pres')
tx.add_taxonomy_set('time','long name for time')
tx.add_taxonomy_set('height','long name for height')



class SinusoidalCtdPublisher(SimpleCtdPublisher):
    def __init__(self, *args, **kwargs):
        super(SinusoidalCtdPublisher, self).__init__(*args,**kwargs)
        #@todo Init stuff


    def _trigger_func(self, stream_id):

        sine_ampl = 2.0 # Amplitude in both directions
        samples = 60
        sine_curr_deg = 0 # varies from 0 - 360

        startTime = time.time()
        count = samples #something other than zero

        while not self.finished.is_set():
            count = time.time() - startTime
            sine_curr_deg = (count % samples) * 360 / samples

            c = numpy.array( [sine_ampl * math.sin(math.radians(sine_curr_deg))] )
            t = numpy.array( [sine_ampl * 2 * math.sin(math.radians(sine_curr_deg + 45))] )
            p = numpy.array( [sine_ampl * 4 * math.sin(math.radians(sine_curr_deg + 60))] )

            lat = lon = numpy.array([0.0])
            tvar = numpy.array([time.time()])

#            ctd_packet = ctd_stream_packet(stream_id=stream_id,
#                c=c, t=t, p = p, lat = lat, lon = lon, time=tvar)
            rdt = RecordDictionaryTool(taxonomy=tx)

            h = numpy.array([random.uniform(0.0, 360.0)])

            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon
            rdt['height'] = h
            rdt['temp'] = t
            rdt['cond'] = c
            rdt['pres'] = p

            g = build_granule(data_producer_id=stream_id, taxonomy=tx, record_dictionary=rdt)

            log.info('SinusoidalCtdPublisher sending 1 record!')
            self.publisher.publish(g)

            time.sleep(1.0)
