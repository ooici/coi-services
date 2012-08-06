'''
@author Tim Giguere
@file ion/processes/data/transforms/ctd/ctd_L2_density_a.py
@description Uses new transform classes to parse data into L2 product for density
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
#d_stream_id = pmsc.create_stream(name='density')
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
#config.process.publish_streams.density = d_stream_id
#pid2 = cc.spawn_process(name='ctd_test', module='ion.processes.data.transforms.ctd.ctd_L2_density_a', cls='ctd_L2_density', config=config)
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
#xn2.bind(d_stream_id + '.data', xp2)
class ctd_L2_density(TransformDataProcess):

    def init(self):
        self._tx = TaxyTool()
        self._tx.add_taxonomy_set('dens','long name for density')
        self._tx.add_taxonomy_set('lat','long name for latitude')
        self._tx.add_taxonomy_set('lon','long name for longitude')
        self._tx.add_taxonomy_set('time','long name for time')

    def __init__(self):
        self.dens_stream = self.CFG.process.publish_streams.density
        super(ctd_L2_density, self).__init__()

    def recv_packet(self, msg, headers):
        log.warn('ctd_L2_desnity.recv_packet: {0}'.format(msg))
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def receive_msg(self, msg, stream_id):
        if msg == {}:
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')

        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time')

        rdt2 = RecordDictionaryTool(self._tx)
        rdt2['dens'] = ctd_L2_density_algorithm.execute(conductivity, pressure, temperature, longitude, latitude)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L2_density', record_dictionary=rdt2, taxonomy=rdt2._tx)
        self.publish(msg=g, stream_id=self.dens_stream)

class ctd_L2_density_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        print args[0], args[1], args[2], args[3], args[4]
        cond = args[0]
        pres = args[1]
        temp = args[2]
        lon = args[3]
        lat = args[4]
        sp = SP_from_cndr(r=cond/cte.C3515, t=temp, p=pres)
        sa = SA_from_SP(sp, pres, lon, lat)
        return rho(sa, temp, pres)