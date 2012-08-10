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
from ion.services.dm.utility.granule_utils import RecordDictionaryTool, build_granule, CoverageCraft
from pyon.public import log
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

import numpy
import random
import time
import gevent

craft = CoverageCraft
sdom, tdom = craft.create_domains()
sdom = sdom.dump()
tdom = tdom.dump()
parameter_dictionary = craft.create_parameters()

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
            if(isinstance(g,Granule)):
                self.publish(g)

            gevent.sleep(self.interval)
