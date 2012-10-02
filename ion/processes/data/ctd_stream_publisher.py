#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Fri Sep  7 12:05:34 EDT 2012
@file ion/processes/data/ctd_stream_publisher.py
@brief A process which produces a simple data stream
'''

from pyon.ion.transforma import TransformStreamPublisher
from pyon.public import log
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule_utils import ParameterContext, ParameterDictionary, QuantityType, AxisTypeEnum

import random
import numpy
import gevent

class SimpleCtdPublisher(TransformStreamPublisher):
    '''
    A very very simple CTD streaming producer
    The only parameter you need to provide is process.stream_id
    Example:
        stream_id, route = pn.pubsub_management.create_stream('ctd publisher', exchange_point='science_data')
        pid = cc.spawn_process('ctdpublisher', 'ion.processes.data.ctd_stream_publisher','SimpleCtdPublisher',{'process':{'stream_id':stream_id}})
    '''
    def on_start(self):
        super(SimpleCtdPublisher,self).on_start()
        self.stream_id = self.CFG.get_safe('process.stream_id',{})
        self.interval  = self.CFG.get_safe('process.interval', 1.0)
        self.last_time = self.CFG.get_safe('process.last_time', 0)


        self.finished = gevent.event.Event()
        self.greenlet = gevent.spawn(self.publish_loop)
        self._stats['publish_count'] = 0
        log.info('SimpleCTDPublisher started, publishing to %s', self.publisher.stream_route.__dict__)

    def on_quit(self):
        if self.greenlet:
            self.finished.set()
            self.greenlet.join(10)
            self.greenlet.kill()
        super(SimpleCtdPublisher,self).on_quit() 

    def publish(self, msg, to_name=''):
        log.debug("self._stats: %s" % self._stats)
        self._stats['publish_count'] += 1
        if to_name:
            self.publisher.publish(msg,stream_id=to_name)
        else:
            log.info('Publishing on (%s,%s)', self.publisher.stream_route.exchange_point, self.publisher.stream_route.routing_key)
            self.publisher.publish(msg)

    def publish_loop(self):

        while not self.finished.is_set():

            #Length of packets should be uniform
            length = 10

            ctd_packet = self._get_new_ctd_packet(length)
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

    def _get_new_ctd_packet(self, length):

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

        rdt['time'] = tvar
        rdt['lat'] = lat
        rdt['lon'] = lon
        rdt['depth'] = h
        rdt['temp'] = t
        rdt['conductivity'] = c
        rdt['pressure'] = p

#        rdt['coordinates'] = rdt0
#        rdt['data'] = rdt1

        g = rdt.to_granule()

        return g
