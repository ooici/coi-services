'''
@author Tim Giguere
@file ion/processes/data/transforms/ctd/ctd_L1_pressure_a.py
@description Uses new transform classes to parse data into L1 product for pressure
'''

import re

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.ion.granule import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe
from pyon.public import log

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)

#stream_id = pmsc.create_stream(name='pfoo')
#pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_data_producer',cls='ExampleDataProducer',config={'process':{'stream_id':stream_id}})

#c_stream_id = pmsc.create_stream(name='conductivity')
#t_stream_id = pmsc.create_stream(name='temperature')
#p_stream_id = pmsc.create_stream(name='pressure')
#cc.spawn_process('l0_transform', 'ion.processes.data.transforms.ctd.ctd_L1_pressure_a','ctd_L1_pressure', config={'process':{'publish_streams':{'conductivity':c_stream_id, 'temperature':t_stream_id, 'pressure': p_stream_id }, 'subscriber_streams':{'stream_id':stream_id} } })

class ctd_L1_pressure(TransformDataProcess):

    def recv_packet(self, msg, headers):
        log.warn('ctd_L1_pressure.recv_packet: {0}'.format(msg))
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        self.publisher.publish(msg=msg, stream_id=stream_id)

    def receive_msg(self, msg, stream_id):
        if msg == {}:
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)

        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')
        longitude = get_safe(rdt, 'lon') # psd.get_values('longitude')
        latitude = get_safe(rdt, 'lat')  #psd.get_values('latitude')
        time = get_safe(rdt, 'time')

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['pres'] = ctd_L1_conductivity_algorithm.execute(pressure)
        rdt2['lat'] = latitude
        rdt2['lon'] = longitude
        rdt2['time'] = time

        g = build_granule(data_producer_id='ctd_L1_pressure', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(msg=g, stream_id=self.pressure)

class ctd_L1_conductivity_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        print args[0]
        return args[0]