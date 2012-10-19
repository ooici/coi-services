#!/usr/bin/env python

from pyon.util.log import log
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, ParameterContext, ParameterDictionary, QuantityType, AxisTypeEnum, CoverageCraft
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from interface.objects import Granule
from pyon.core.exception import NotFound
from ion.util.parameter_yaml_IO import get_param_dict
import numpy
import random
import gevent

class BetterDataProducer(SimpleCtdPublisher):
    def on_start(self):
        self.pdict = None
        stream_id = self.CFG.get_safe('process.stream_id')
        pubsub_cli = PubsubManagementServiceProcessClient(process=self)
        try: 
            stream_def = pubsub_cli.read_stream_definition(stream_id=stream_id)
            self.pdict = ParameterDictionary.load(stream_def.parameter_dictionary)
        except NotFound:
            self.pdict = get_param_dict('ctd_parsed_param_dict')
        super(BetterDataProducer,self).on_start()

    def publish_loop(self):
        t_i = 0
        while not self.finished.is_set():
            rdt = RecordDictionaryTool(param_dictionary=self.pdict)
            rdt['time']         = numpy.arange(10) + t_i*10
            rdt['temp']         = numpy.random.random(10) * 10
            rdt['lat']          = numpy.array([0] * 10)
            rdt['lon']          = numpy.array([0] * 10)
            rdt['conductivity'] = numpy.random.random(10) * 10
            rdt['binary']         = numpy.array(['hi'] * 10, dtype='object')
            
            self.publish(rdt.to_granule())
            gevent.sleep(self.interval)
            t_i += 1



class ExampleDataProducer(SimpleCtdPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """


    #overriding trigger function here to use new granule
    def publish_loop(self):

        #@todo - add lots of comments in here
        while not self.finished.is_set():

            length = 10

            #Explicitly make these numpy arrays...
            c = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])
            t = numpy.array([random.uniform(-1.7, 21.0) for i in xrange(length)])
            p = numpy.array([random.lognormvariate(1,2) for i in xrange(length)])
            lat = numpy.array([random.uniform(-90.0, 90.0) for i in xrange(length)])
            lon = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)])
            tvar = numpy.array([self.last_time + i for i in xrange(1,length+1)])
            self.last_time = max(tvar)

            parameter_dictionary = self._create_parameter()
            rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

            # This is an example of using groups it is not a normative statement about how to use groups



            rdt['temp'] = t
            rdt['conductivity'] = c
            rdt['pressure'] = p

            #add a value sequence of raw bytes - not sure the type below is correct?
            with open('/dev/urandom','r') as rand:
                rdt['raw_fixed'] = numpy.array([rand.read(32) for i in xrange(length)], dtype='a32')

            #add a value sequence of raw bytes - not sure the type below is correct?
            with open('/dev/urandom','r') as rand:
                rdt['raw_blob'] = numpy.array([rand.read(random.randint(1,40)) for i in xrange(length)], dtype=object)



            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon


            g = rdt.to_granule()

            log.info('Sending %d values!' % length)
            if isinstance(g,Granule):
                self.publish(g)

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

        raw_fixed_ctxt = ParameterContext('raw_fixed', param_type=QuantityType(value_encoding=numpy.float32))
        raw_fixed_ctxt.uom = 'unknown'
        raw_fixed_ctxt.fill_value = 0e0
        pdict.add_context(raw_fixed_ctxt)

        raw_blob_ctxt = ParameterContext('raw_blob', param_type=QuantityType(value_encoding=numpy.float32))
        raw_blob_ctxt.uom = 'unknown'
        raw_blob_ctxt.fill_value = 0e0
        pdict.add_context(raw_blob_ctxt)

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
