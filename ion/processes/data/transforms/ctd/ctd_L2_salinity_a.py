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

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)

#stream_id = pmsc.create_stream(name='pfoo')
#pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_data_producer',cls='ExampleDataProducer',config={'process':{'stream_id':stream_id}})

#s_stream_id = pmsc.create_stream(name='salinity')
#cc.spawn_process('l2_transform', 'ion.processes.data.transforms.ctd.ctd_L2_salinity_a','ctd_L2_salinity', config={'process':{'publish_streams':{'density':s_stream_id}, 'subscriber_streams':{'stream_id':stream_id} } })

class ctd_L2_salinity(TransformDataProcess):

    def init(self):
        self._tx = TaxyTool()
        self._tx.add_taxonomy_set('sal','long name for salinity')
        self._tx.add_taxonomy_set('lat','long name for latitude')
        self._tx.add_taxonomy_set('lon','long name for longitude')
        self._tx.add_taxonomy_set('time','long name for time')

    def recv_packet(self, msg, headers):
        log.warn('ctd_L2_salinity.recv_packet: {0}'.format(msg))
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

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['sal'] = ctd_L2_salinity_algorithm.execute(conductivity, pressure, temperature)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L2_salinity', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(msg=g, stream_id=self.salinity)

class ctd_L2_salinity_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        print args[0], args[1], args[2]
        cond = args[0]
        pres = args[1]
        temp = args[2]
        return SP_from_cndr(r=cond/cte.C3515, t=temp, p=pres)