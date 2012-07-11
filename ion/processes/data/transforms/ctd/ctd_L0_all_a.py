
import re

from pyon.ion.transforma import TransformDataProcess, TransformAlgorithm
from pyon.ion.granule import RecordDictionaryTool
from pyon.ion.granule.granule import build_granule
from pyon.util.containers import get_safe

class ctd_L0_all(TransformDataProcess):

    def recv_packet(self, msg, headers):
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def publish(self, msg, headers):
        for publisher in self.publishers:
            publisher.publish(msg=msg)

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
        self.publish(g, None)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['pres'] = ctd_L0_algorithm.execute(pressure)

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(g, None)

        rdt2 = RecordDictionaryTool(rdt._tx)
        rdt2['temp'] = ctd_L0_algorithm.execute(temperature)

        g = build_granule(data_producer_id='ctd_L0_all', record_dictionary=rdt2, taxonomy=-rdt2._tx)
        self.publish(g, None)


class ctd_L0_algorithm(TransformAlgorithm):

    @staticmethod
    def execute(*args, **kwargs):
        return args[0]