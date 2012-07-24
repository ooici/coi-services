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
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.public import log

import numpy
import random
import time
import gevent

### Taxonomies are defined before hand out of band... somehow.
tx = TaxyTool()
tx.add_taxonomy_set('temp','long name for temp')
tx.add_taxonomy_set('cond','long name for cond')
tx.add_taxonomy_set('lat','long name for latitude')
tx.add_taxonomy_set('lon','long name for longitude')
tx.add_taxonomy_set('pres','long name for pres')
tx.add_taxonomy_set('time','long name for time')
# This is an example of using groups it is not a normative statement about how to use groups
tx.add_taxonomy_set('group1','This group contains coordinates...')
tx.add_taxonomy_set('group0','This group contains data...')

tx.add_taxonomy_set('raw_fixed','Fixed length bytes in an array of records')
tx.add_taxonomy_set('raw_blob','Unlimited length bytes in an array')


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
            rdt = RecordDictionaryTool(taxonomy=tx)

            # This is an example of using groups it is not a normative statement about how to use groups



            rdt['temp'] = t
            rdt['cond'] = c
            rdt['pres'] = p

            #add a value sequence of raw bytes - not sure the type below is correct?
            with open('/dev/urandom','r') as rand:
                rdt['raw_fixed'] = numpy.array([rand.read(32) for i in xrange(length)], dtype='a32')

            #add a value sequence of raw bytes - not sure the type below is correct?
            with open('/dev/urandom','r') as rand:
                rdt['raw_blob'] = numpy.array([rand.read(random.randint(1,40)) for i in xrange(length)], dtype=object)



            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon


            g = build_granule(data_producer_id=stream_id, taxonomy=tx, record_dictionary=rdt)

            log.info('Sending %d values!' % length)
            if(isinstance(g,Granule)):
                self.publish(g)

            gevent.sleep(self.interval)
