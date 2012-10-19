# Inherit some old machinery for this example
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ooi.logging import log
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.util.parameter_yaml_IO import get_param_dict

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

        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')
        rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

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
            rdt['temp'] = t
            rdt['conductivity'] = c
            rdt['pressure'] = p

            g = rdt.to_granule()
            log.debug('SimpleCtdDataProducer: Sending %d values!' % length)
            self.publisher.publish(g)


