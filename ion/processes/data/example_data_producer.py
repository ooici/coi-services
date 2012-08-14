# New Stream and Granule Stuff....

'''
See also: ion/processes/data/stream_granule_logger.py for an example with subscription!

To Run:
bin/pycc --rel res/deploy/r2dm.yml
### In the shell...

# create a stream id and pass it in...
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
pmsc = PubsubManagementServiceClient(node=cc.node)
stream_id = pmsc.create_stream(name='pfoo')
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_data_producer',cls='ExampleDataProducer',config={'process':{'stream_id':stream_id}})


'''

# Inherit some old machinery for this example
from interface.objects import Granule
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher

### For new granule and stream interface
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, build_granule
from pyon.public import log
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from ion.services.dm.utility.granule_utils import CoverageCraft

import numpy
import random
import time
import gevent

class BetterDataProducer(SimpleCtdPublisher):
    def _trigger_func(self, stream_id):
        t_i = 0
        while not self.finished.is_set():

            length                    = 10
            black_box                 = CoverageCraft()
            black_box.rdt['time']         = numpy.arange(10) + t_i*10
            black_box.rdt['temp']         = numpy.random.random(10) * 10
            black_box.rdt['lat']          = numpy.array([0] * 10)
            black_box.rdt['lon']          = numpy.array([0] * 10)
            black_box.rdt['depth']        = numpy.array([0] * 10)
            black_box.rdt['conductivity'] = numpy.random.random(10) * 10
            black_box.rdt['data']         = numpy.random.randint(0,255,10) # Simulates random bytes
            
            black_box.sync_with_granule()
            granule = black_box.to_granule()

            self.publish(granule)
            gevent.sleep(self.interval)
            t_i += 1



class ExampleDataProducer(SimpleCtdPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """


    #overriding trigger function here to use new granule
    def _trigger_func(self, stream_id):

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


            g = build_granule(data_producer_id=stream_id, param_dictionary=parameter_dictionary, record_dictionary=rdt)

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

        raw_fixed_ctxt = ParameterContext('raw_fixed', param_type='unknown')
        raw_fixed_ctxt.uom = 'unknown'
        raw_fixed_ctxt.fill_value = 0e0
        pdict.add_context(raw_fixed_ctxt)

        raw_blob_ctxt = ParameterContext('raw_blob', param_type='unknown')
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

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=numpy.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict