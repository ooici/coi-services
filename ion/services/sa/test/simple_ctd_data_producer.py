# Inherit some old machinery for this example
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher

### For new granule and stream interface
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.public import log

import numpy
import random
import time


### Taxonomies are defined before hand out of band... somehow.
tx = TaxyTool()
tx.add_taxonomy_set('temp','long name for temp')
tx.add_taxonomy_set('cond','long name for cond')
tx.add_taxonomy_set('lat','long name for latitude')
tx.add_taxonomy_set('lon','long name for longitude')
tx.add_taxonomy_set('pres','long name for pres')
tx.add_taxonomy_set('time','long name for time')
tx.add_taxonomy_set('height','long name for height')
# This is an example of using groups it is not a normative statement about how to use groups
tx.add_taxonomy_set('coordinates','This group contains coordinates...')
tx.add_taxonomy_set('data','This group contains data...')


class SimpleCtdDataProducer(SimpleCtdPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """


    #overriding trigger function here to use new granule
    def _trigger_func(self, stream_id):
        log.debug("SimpleCtdDataProducer:_trigger_func ")

        rdt = RecordDictionaryTool(taxonomy=tx)
        rdt0 = RecordDictionaryTool(taxonomy=tx)
        rdt1 = RecordDictionaryTool(taxonomy=tx)


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

            rdt0['time'] = tvar
            rdt0['lat'] = lat
            rdt0['lon'] = lon
            rdt0['height'] = h
            rdt1['temp'] = t
            rdt1['cond'] = c
            rdt1['pres'] = p

            rdt['coordinates'] = rdt0
            rdt['data'] = rdt1

            log.debug("SimpleCtdDataProducer: logging published Record Dictionary:\n %s", rdt.pretty_print())

            g = build_granule(data_producer_id=stream_id, taxonomy=tx, record_dictionary=rdt)

            log.debug('SimpleCtdDataProducer: Sending %d values!' % length)
            self.publisher.publish(g)

            time.sleep(2.0)
  