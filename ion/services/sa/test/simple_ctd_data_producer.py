# Inherit some old machinery for this example
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.public import log
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

import numpy, gevent
import random
import time

class SimpleCtdDataProducer(SimpleCtdPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """
    def on_start(self):
        super(SimpleCtdDataProducer,self).on_start()

    def on_quit(self):
        super(SimpleCtdDataProducer,self).on_quit()

    #overriding trigger function here to use new granule
    def _trigger_func(self, stream_id):
        log.debug("SimpleCtdDataProducer:_trigger_func ")

        parameter_dictionary = self._create_parameter()
        rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)
#        rdt0 = RecordDictionaryTool(param_dictionary=parameter_dictionary)
#        rdt1 = RecordDictionaryTool(param_dictionary=parameter_dictionary)


        #@todo - add lots of comments in here

        # The base SimpleCtdPublisher provides a gevent Event that indicates when the process is being
        # shut down. We can use a simple pattern here to accomplish both a safe shutdown of this loop
        # when the process shuts down *AND* do the timeout between loops in a very safe/efficient fashion.
        #
        # By using this instead of a sleep in the loop itself, we can immediatly interrupt this loop when
        # the process is being shut down instead of having to wait for the sleep to terminate.
        while not self.finished.wait(timeout=2):

            length = 10

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


            #todo: use only flat dicts for now, may change later...
#            rdt['coordinates'] = rdt0
#            rdt['data'] = rdt1

#            log.debug("SimpleCtdDataProducer: logging published Record Dictionary:\n %s", rdt.pretty_print())
#            log.debug("SimpleCtdDataProducer: logging published Record Dictionary:\n %s", rdt)

            g = build_granule(data_producer_id=stream_id, param_dictionary=parameter_dictionary, record_dictionary=rdt)

            log.debug('SimpleCtdDataProducer: Sending %d values!' % length)
            self.publisher.publish(g)

    def _create_parameter(self):

        pdict = ParameterDictionary()

        pdict = self._add_location_time_ctxt(pdict)

        pres_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=numpy.float32))
        pres_ctxt.uom = 'Pascal'
        pres_ctxt.fill_value = 0x0
        pdict.add_context(pres_ctxt)

        cond_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=numpy.float32))
        cond_ctxt.uom = 'unknown'
        cond_ctxt.fill_value = 0e0
        pdict.add_context(cond_ctxt)

        temp_ctxt = ParameterContext('temp', param_type=QuantityType(value_encoding=np.float32))
        temp_ctxt.uom = 'degree_Celsius'
        temp_ctxt.fill_value = 0e0
        pdict.add_context(temp_ctxt)

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

