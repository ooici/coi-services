'''
@author Tim Giguere
@file ion/processes/data/transforms/ctd/ctd_L1_conductivity_a.py
@description Uses new transform classes to parse data into L1 product for conductivity
'''

import re

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.ion.granule import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe
from pyon.public import log

#from pyon.util.containers import DotDict
#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)
#
#c_stream_id = pmsc.create_stream(name='conductivity')
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
#pid2 = cc.spawn_process(name='ctd_test', module='ion.processes.data.transforms.ctd.ctd_L1_conductivity_a', cls='ctd_L1_conductivity', config=config)
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
#xn2.bind(c_stream_id + '.data', xp2)


class ctd_L1_conductivity(TransformDataProcess):

    def __init__(self):
        self.cond_stream = self.CFG.process.publish_streams.conductivity
        super(ctd_L1_conductivity, self).__init__()

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def recv_packet(self, msg,stream_route,stream_id):
        if msg == {}:
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')
        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time')

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['cond'] = ctd_L1_pressure_algorithm.execute(conductivity)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L1_conductivity', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.cond_stream)

class ctd_L1_pressure_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return (args[0] / 100000.0) - 0.5
