'''
@author Tim Giguere
@file ion/processes/data/transforms/ctd/ctd_L2_salinity_a.py
@description Uses new transform classes to parse data into L2 product for salinity
'''

import re

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.ion.granule import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe
from pyon.public import log
from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

#from pyon.util.containers import DotDict
#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)
#
#s_stream_id = pmsc.create_stream(name='salinity')
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
#config.process.publish_streams.salinity = s_stream_id
#pid2 = cc.spawn_process(name='ctd_test', module='ion.processes.data.transforms.ctd.ctd_L2_salinity_a', cls='ctd_L2_salinity', config=config)
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
#xn2.bind(s_stream_id + '.data', xp2)


class ctd_L2_salinity(TransformDataProcess):

    def init(self):
        self._tx = TaxyTool()
        self._tx.add_taxonomy_set('sal','long name for salinity')
        self._tx.add_taxonomy_set('lat','long name for latitude')
        self._tx.add_taxonomy_set('lon','long name for longitude')
        self._tx.add_taxonomy_set('time','long name for time')

    def __init__(self):
        self.sal_stream = self.CFG.process.publish_streams.salinity
        super(ctd_L2_salinity, self).__init__()

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
        rdt2['sal'] = ctd_L2_salinity_algorithm.execute(conductivity, pressure, temperature)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L2_salinity', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.sal_stream)

class ctd_L2_salinity_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        print args[0], args[1], args[2]
        cond = args[0]
        pres = args[1]
        temp = args[2]
        return SP_from_cndr(r=cond/cte.C3515, t=temp, p=pres)
