'''
@author Tim Giguere
@file ion/processes/data/transforms/ctd/ctd_L0_all_a.py
@description Uses new transform classes to parse CTD data into L0 streams
'''

import re

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.ion.granule import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe
from pyon.public import log

#Here's an example on how to use the CTD Transforms. ExampleDataProducer publishes data granules to 'out_stream'. ctd_L0_all
#receives the granule and publishes each part to streams 'conductivity', 'temperature', and 'pressure'. Finally
#ExampleDataReceiver receives the granules published by ctd_L0_all and prints the data values to the screen.

#from pyon.util.containers import DotDict
#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)
#
#c_stream_id = pmsc.create_stream(name='conductivity')
#t_stream_id = pmsc.create_stream(name='temperature')
#p_stream_id = pmsc.create_stream(name='pressure')
#
#out_stream_id = pmsc.create_stream(name='out_stream')
#
#config = DotDict()
#config.process.exchange_point = 'test_xp'
#config.process.out_stream_id = out_stream_id
#pid = cc.spawn_process(name='test', module='ion.processes.data.example_data_producer_a', cls='ExampleDataProducer', config=config)
#
#config = DotDict()
#config.process.queue_name = 'test_queue'
#config.process.exchange_point = 'output_xp'
#config.process.publish_streams.conductivity = c_stream_id
#config.process.publish_streams.pressure = p_stream_id
#config.process.publish_streams.temperature = t_stream_id
#pid2 = cc.spawn_process(name='ctd_test', module='ion.processes.data.transforms.ctd.ctd_L0_all_a', cls='ctd_L0_all', config=config)
#
#xn = cc.ex_manager.create_xn_queue('test_queue')
#xp = cc.ex_manager.create_xp('test_xp')
#xn.bind(out_stream_id + '.data', xp)
#
#config = DotDict()
#config.process.queue_name = 'output_queue'
#pid3 = cc.spawn_process(name='receiver_test', module='ion.processes.data.example_data_receiver_a', cls='ExampleDataReceiver', config=config)
#
#xn2 = cc.ex_manager.create_xn_queue('output_queue')
#xp2 = cc.ex_manager.create_xp('output_xp')
#xn2.bind(c_stream_id + '.data', xp2)   #bind the queue to the stream you want to listen to

class ctd_L0_all(TransformDataProcess):

    def __init__(self):
        self.cond_stream = self.CFG.process.publish_streams.conductivity
        self.temp_stream = self.CFG.process.publish_streams.temperature
        self.pres_stream = self.CFG.process.publish_streams.pressure
        super(ctd_L0_all, self).__init__()

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def recv_packet(self, msg, stream_route, stream_id):
        if msg == {}:
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')
        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time')

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['cond'] = ctd_L0_algorithm.execute(conductivity)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.cond_stream)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['pres'] = ctd_L0_algorithm.execute(pressure)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.pres_stream)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['temp'] = ctd_L0_algorithm.execute(temperature)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.temp_stream)

class ctd_L0_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return args[0]
