# New Transform Data Producer

'''

from pyon.util.containers import DotDict
config = DotDict()
config.process.exchange_point = 'test'
config.process.out_stream_id = 'out_stream_id'
pid = cc.spawn_process('test','ion.processes.data.example_data_producer_a', 'ExampleDataProducer', config)

'''

# Inherit some old machinery for this example
from interface.objects import Granule
from pyon.ion.transforma import TransformStreamPublisher, TransformAlgorithm

### For new granule and stream interface
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.public import log

from gevent.greenlet import Greenlet

import numpy
import random
import time

class ExampleDataProducer(TransformStreamPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """

    def init(self):
        ### Taxonomies are defined before hand out of band... somehow.
        self._tx = TaxyTool()
        self._tx.add_taxonomy_set('temp','long name for temp')
        self._tx.add_taxonomy_set('cond','long name for cond')
        self._tx.add_taxonomy_set('lat','long name for latitude')
        self._tx.add_taxonomy_set('lon','long name for longitude')
        self._tx.add_taxonomy_set('pres','long name for pres')
        self._tx.add_taxonomy_set('time','long name for time')

    def on_start(self):
        super(ExampleDataProducer, self).on_start()

        stream_id = self.CFG.process.out_stream_id

        g = Greenlet(self._trigger_func, stream_id)
        log.debug('Starting publisher thread for simple ctd data.')
        g.start()
        log.warn('Publisher Greenlet started in "%s"' % self.__class__.__name__)
        self.greenlet_queue = []
        self.greenlet_queue.append(g)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(ExampleDataProducer,self).on_quit()

    def _trigger_func(self, stream_id):

        self.last_time = 0

        #@todo - add lots of comments in here
        while True:

            length = 10

            #Explicitly make these numpy arrays...
            c = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)])

            t = numpy.array([random.uniform(-1.7, 21.0) for i in xrange(length)])

            p = numpy.array([random.lognormvariate(1,2) for i in xrange(length)])

            lat = numpy.array([random.uniform(-90.0, 90.0) for i in xrange(length)])

            lon = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)])

            tvar = numpy.array([self.last_time + i for i in xrange(1,length+1)])

            self.last_time = max(tvar)

            rdt = RecordDictionaryTool(taxonomy=self._tx)

            rdt['temp'] = ExampleDataProducer_algorithm.execute(t)
            rdt['cond'] = ExampleDataProducer_algorithm.execute(c)
            rdt['pres'] = ExampleDataProducer_algorithm.execute(p)
            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon

            log.info("logging published Record Dictionary:\n %s", rdt.pretty_print())

            g = build_granule(data_producer_id=stream_id, taxonomy=self._tx, record_dictionary=rdt)

            log.warn('Sending %d values!' % length)
            if(isinstance(g, Granule)):
                self.publish(g, stream_id)

            time.sleep(2.0)

class ExampleDataProducer_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return args[0]
