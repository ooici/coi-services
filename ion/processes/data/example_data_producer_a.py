# New Transform Data Producer

'''

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.util.containers import DotDict
pmsc = PubsubManagementServiceClient(node=cc.node)

#create the stream to publish the data on
out_stream_id = pmsc.create_stream(name='out_stream')

config = DotDict()
config.process.exchange_point = 'test_xp'
config.process.out_stream_id = out_stream_id
#spawn the producer process. This will create the exchange point given in the config, and publish to out_stream
pid = cc.spawn_process(name='test', module='ion.processes.data.example_data_producer_a', cls='ExampleDataProducer', config=config)

'''

from interface.objects import Granule
from pyon.ion.transforma import TransformStreamPublisher, TransformAlgorithm

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule
from pyon.public import log

from gevent.greenlet import Greenlet

from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

import numpy
import random
import time
import gevent

class ExampleDataProducer(TransformStreamPublisher):
    """
    This Example process inherits some business logic from the above example.

    It is not infrastructure - it is a demonstration of the infrastructure applied to an example.
    """

    def on_start(self):
        super(ExampleDataProducer, self).on_start()

        stream_id = self.CFG.process.out_stream_id

        g = Greenlet(self._trigger_func, stream_id)
        log.debug('Starting publisher thread for simple ctd data.')
        g.start()
        log.info('Publisher Greenlet started in "%s"' % self.__class__.__name__)
        self.greenlet_queue = []
        self.greenlet_queue.append(g)

        self.finished = gevent.event.Event()

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(ExampleDataProducer,self).on_quit()

    def _trigger_func(self, stream_id):

        self.last_time = 0

        parameter_dictionary = self._create_parameter()

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

            rdt['temp'] = t # ExampleDataProducer_algorithm.execute(t)
            rdt['conductivity'] = c # ExampleDataProducer_algorithm.execute(c)
            rdt['pressure'] = p # ExampleDataProducer_algorithm.execute(p)
            rdt['time'] = tvar
            rdt['lat'] = lat
            rdt['lon'] = lon

            log.info("logging published Record Dictionary:\n %s", rdt.pretty_print())

            g = build_granule(data_producer_id=stream_id, param_dictionary=parameter_dictionary, record_dictionary=rdt)

            log.info('Sending %d values!' % length)
            if(isinstance(g, Granule)):
                self.publish(g, stream_id)

            time.sleep(2.0)

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

        depth_ctxt = ParameterContext('depth', param_type=QuantityType(value_encoding=numpy.float32))
        depth_ctxt.reference_frame = AxisTypeEnum.HEIGHT
        depth_ctxt.uom = 'meters'
        depth_ctxt.fill_value = 0e0
        pdict.add_context(depth_ctxt)

        return pdict
