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
import numpy
import gevent

from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
### For new granule and stream interface
from pyon.ion.transforma import TransformStreamPublisher
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

class SimpleCtdPublisher(TransformStreamPublisher):
    def on_start(self):
        self.exchange_point = self.CFG.get_safe('process.exchange_point', 'science_data')
        self.CFG.process.exchange_point = self.exchange_point
        super(SimpleCtdPublisher,self).on_start()
        self.stream_id = self.CFG.get_safe('process.stream_id',{})
        self.interval  = self.CFG.get_safe('process.interval', 1.0)
        self.last_time = self.CFG.get_safe('process.last_time', 0)

        # Stream creation is done in SA, but to make the example go for demonstration create one here if it is not provided...
        if not self.stream_id:

            pubsub_cli = PubsubManagementServiceClient()
            self.stream_id = pubsub_cli.create_stream( name='Example CTD Data')

        self.greenlet = gevent.spawn(self._trigger_func, self.stream_id)
        self.finished = gevent.event.Event()
        log.info('SimpleCTDPublisher started, publishing to %s->%s', self.exchange_point, self.stream_id)

    def on_quit(self):
        if self.greenlet:
            self.finished.set()
            self.greenlet.join(10)
        super(SimpleCtdPublisher,self).on_quit() 

    def publish(self, msg, to_name=''):
        if to_name:
            self.publisher.publish(msg,stream_id=to_name)
        else:
            log.info('Publishing on %s->%s', self.exchange_point, self.stream_id)
            self.publisher.publish(msg,stream_id=self.stream_id)


    def _trigger_func(self, stream_id):

        while not self.finished.is_set():

            #Length of packets should be uniform
            length = 10

            ctd_packet = self._get_new_ctd_packet(stream_id, length)

            log.info('SimpleCtdPublisher sending %d values!' % length)
            self.publish(ctd_packet)

            gevent.sleep(self.interval)

    def _create_parameter(self):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=numpy.float32))
        pres_ctxt.uom = 'Pascal'
        pres_ctxt.fill_value = 0x0
        pdict.add_context(pres_ctxt)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=numpy.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        pdict.add_context(temp_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=numpy.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        pdict.add_context(cond_ctxt)

        return pdict

    def _add_location_time_ctxt(self, pdict):

        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=numpy.int64))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 1970-01-01'
        t_ctxt.fill_value = 0x0
        pdict.add_context(t_ctxt)

        lat_ctxt = ParameterContext('lat', param_type=QuantityType(value_encoding=numpy.float32))
        lat_ctxt.reference_frame = AxisTypeEnum.LAT
        lat_ctxt.uom = 'degree_north'
        lat_ctxt.fill_value = 0e0
        pdict.add_context(lat_ctxt)

        lon_ctxt = ParameterContext('lon', param_type=QuantityType(value_encoding=numpy.float32))
        lon_ctxt.reference_frame = AxisTypeEnum.LON
        lon_ctxt.uom = 'degree_east'
        lon_ctxt.fill_value = 0e0
        pdict.add_context(lon_ctxt)

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=numpy.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict

    def _get_new_ctd_packet(self, stream_id, length):

        parameter_dictionary = self._create_parameter()
        rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        #Explicitly make these numpy arrays...
        c = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)]) 
        t = numpy.array([random.uniform(-1.7, 21.0) for i in xrange(length)]) 
        p = numpy.array([random.lognormvariate(1,2) for i in xrange(length)])
        lat = numpy.array([random.uniform(-90.0, 90.0) for i in xrange(length)]) 
        lon = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)]) 
        h = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)]) 
        tvar = numpy.array([self.last_time + i for i in xrange(1,length+1)]) 
        self.last_time = max(tvar)

        log.warn('Got time: %s' % str(tvar))
        log.warn('Got t: %s' % str(t))

        rdt['time'] = tvar
        rdt['lat'] = lat
        rdt['lon'] = lon
        rdt['depth'] = h
        rdt['temp'] = t
        rdt['conductivity'] = c
        rdt['pressure'] = p

#        rdt['coordinates'] = rdt0
#        rdt['data'] = rdt1

        g = build_granule(data_producer_id=stream_id, param_dictionary=parameter_dictionary, record_dictionary=rdt)

        return g


class PointCtdPublisher(StandaloneProcess):

    def on_start(self):
        super(PointCtdPublisher,self).on_start()
        self.finished = gevent.event.Event()

    def on_quit(self):
        self.finished.set()
        super(PointCtdPublisher,self).on_quit()

    #overriding trigger function here to use PointSupplementConstructor
    def _trigger_func(self, stream_id):

        point_def = ctd_stream_definition(stream_id=stream_id)
        point_constructor = PointSupplementConstructor(point_definition=point_def)

        while not self.finished.is_set():

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

            time.sleep(1.0)

