'''
@author Stephen Henrie
@description Example Transform to double salinity
'''
from ion.core.process.transform import TransformDataProcess
from ion.core.function.transform_function import SimpleGranuleTransformFunction
### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.containers import get_safe
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

class SalinityDoubler(TransformDataProcess):


    def on_start(self):

        pubsub = PubsubManagementServiceProcessClient(process=self)
        if self.CFG.process.publish_streams.has_key('salinity'):
            self.sal_stream = self.CFG.process.publish_streams.salinity
        elif self.CFG.process.publish_streams.has_key('output'):
            self.sal_stream = self.CFG.process.publish_streams.output
            self.salinity = self.output
        self.CFG.process.stream_id = self.sal_stream
        self.stream_id = self.sal_stream
        self.stream_def = pubsub.read_stream_definition(stream_id=self.sal_stream)

        super(SalinityDoubler, self).on_start()

    def publish(self, msg, stream_id=''):
        self.publisher.publish(msg)

    def recv_packet(self, granule, stream_route, stream_id):
        """
        Example process to double the salinity value
        """
        g = ctd_L2_salinity_algorithm.execute(granule, params=self.stream_def._id)
        g.data_producer_id = self.id

        self.publish(g)

        return g

class ctd_L2_salinity_algorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        stream_def_id = params
        rdt = RecordDictionaryTool.load_from_granule(input)

        salinity = get_safe(rdt, 'salinity')

        longitude = get_safe(rdt, 'lon')
        latitude = get_safe(rdt, 'lat')
        time = get_safe(rdt, 'time')

        root_rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)

        root_rdt['salinity'] = 2 * salinity
        root_rdt['time'] = time
        root_rdt['lat'] = latitude
        root_rdt['lon'] = longitude

        g = root_rdt.to_granule()
        return g

