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

#from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
#pmsc = PubsubManagementServiceClient(node=cc.node)

#stream_id = pmsc.create_stream(name='pfoo')
#pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_data_producer',cls='ExampleDataProducer',config={'process':{'stream_id':stream_id}})

#c_stream_id = pmsc.create_stream(name='conductivity')
#t_stream_id = pmsc.create_stream(name='temperature')
#p_stream_id = pmsc.create_stream(name='pressure')
#cc.spawn_process('l0_transform', 'ion.processes.data.transforms.ctd.ctd_L0_all_a','ctd_L0_all', config={'process':{'publish_streams':{'conductivity':c_stream_id, 'temperature':t_stream_id, 'pressure': p_stream_id }, 'subscriber_streams':{'stream_id':stream_id} } })

class ctd_L0_all(TransformDataProcess):

    def recv_packet(self, msg, headers):
        log.warn('ctd_L0_all.recv_packet: {0}'.format(msg))
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)

        log.warn('ctd_L0_all:{0}'.format(msg))

        self.receive_msg(msg, stream_id)

    def publish(self, msg, stream_id):
        #for publisher in self.publishers:
        #    publisher.publish(msg=msg, headers=headers)
        self.publisher.publish(msg=msg, to_name=self.exchange_point.create_route('%s.data' % stream_id))

    def receive_msg(self, msg, stream_id):
        if msg == {}:
            return

        rdt = RecordDictionaryTool.load_from_granule(msg)

        conductivity = get_safe(rdt, 'cond') #psd.get_values('conductivity')
        pressure = get_safe(rdt, 'pres') #psd.get_values('pressure')
        temperature = get_safe(rdt, 'temp') #psd.get_values('temperature')

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['cond'] = ctd_L0_algorithm.execute(conductivity)

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(msg=g, stream_id=self.conductivity)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['pres'] = ctd_L0_algorithm.execute(pressure)

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(msg=g, stream_id=self.pressure)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['temp'] = ctd_L0_algorithm.execute(temperature)

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(msg=g, stream_id=self.temperature)

class ctd_L0_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        print args[0]
        return args[0]