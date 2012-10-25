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
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time
from uuid import uuid4
import random
import math

from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import numpy
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

class SinusoidalCtdPublisher(SimpleCtdPublisher):
    def on_start(self, *args, **kwargs):
        super(SinusoidalCtdPublisher, self).on_start(*args,**kwargs)
        #@todo Init stuff


    def publish_loop(self):

        sine_ampl = 2.0 # Amplitude in both directions
        samples = 60

        startTime = time.time()
        count = samples #something other than zero

        self.dataset_management =  DatasetManagementServiceClient(node=self.container.node)

        while not self.finished.is_set():
            count = time.time() - startTime
            sine_curr_deg = (count % samples) * 360 / samples

            c = numpy.array( [sine_ampl * math.sin(math.radians(sine_curr_deg))] )
            t = numpy.array( [sine_ampl * 2 * math.sin(math.radians(sine_curr_deg + 45))] )
            p = numpy.array( [sine_ampl * 4 * math.sin(math.radians(sine_curr_deg + 60))] )

            lat = lon = numpy.array([0.0])
            tvar = numpy.array([time.time()])

            parameter_dictionary = self._create_parameter()
            #parameter_dictionary = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')

            rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

            h = numpy.array([random.uniform(0.0, 360.0)])

            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon
            rdt['temp'] = t
            rdt['conductivity'] = c
            rdt['pressure'] = p

            g = rdt.to_granule(data_producer_id=self.id)

            log.info('SinusoidalCtdPublisher sending 1 record!')
            self.publisher.publish(g, self.stream_id)

            time.sleep(1.0)

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

        return pdict
