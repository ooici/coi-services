# Inherit some old machinery for this example
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.public import log
from ion.services.dm.utility.granule_utils import CoverageCraft
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

import numpy, gevent
import random
import time

craft = CoverageCraft
sdom, tdom = craft.create_domains()
sdom = sdom.dump()
tdom = tdom.dump()
parameter_dictionary = craft.create_parameters()

class SimpleCtdDataProducer(SimpleCtdPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """
    def on_start(self):
        super(SimpleCtdDataProducer,self).on_start()

    #overriding trigger function here to use new granule
    def _trigger_func(self, stream_id):
        log.debug("SimpleCtdDataProducer:_trigger_func ")


        rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)
#        rdt0 = RecordDictionaryTool(param_dictionary=parameter_dictionary)
#        rdt1 = RecordDictionaryTool(param_dictionary=parameter_dictionary)


        #@todo - add lots of comments in here
        while True:

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

            time.sleep(2.0)
  