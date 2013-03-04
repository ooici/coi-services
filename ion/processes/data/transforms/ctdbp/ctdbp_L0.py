'''
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctdbp/ctdbp_L0.py
@description Transforms CTD parsed data into L0 streams
'''

from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import MultiGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

class CTDBP_L0_all(TransformDataProcess):
    """
    L0 listens to the parsed and just pulls C, T, & Pressure from the parsed and puts it onto the L0 stream.
    """
    output_bindings = ['L0_stream']
    def on_start(self):
        super(CTDBP_L0_all, self).on_start()

        if not self.CFG.process.publish_streams.has_key('L0_stream'):
            raise BadRequest("For CTD transforms, please send the stream_id for the L0_stream using "
                             "a special keyword (L0_stream)")
        self.L0_stream = self.CFG.process.publish_streams.L0_stream

        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_def_L0 = pubsub.read_stream_definition(stream_id=self.L0_stream)

        self.params = {}
        self.params['L0_stream'] = self.stream_def_L0._id

    def recv_packet(self, packet,stream_route, stream_id):
        """Processes incoming data!!!!
            @param packet granule
            @param stream_route StreamRoute
            @param stream_id str
        """
        if packet == {}:
            return
        granules = ctd_L0_algorithm.execute([packet], params=self.params)
        for granule in granules:
            self.conductivity.publish(msg = granule['conductivity'])
            self.temperature.publish(msg = granule['temp'])
            self.pressure.publish(msg = granule['pressure'])

class ctd_L0_algorithm(MultiGranuleTransformFunction):

    @staticmethod
    @MultiGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        '''
        @param input granule
        @retval result_list list of dictionaries containing granules as values
        '''

        result_list = []
        for x in input:
            rdt = RecordDictionaryTool.load_from_granule(x)

            conductivity = rdt['conductivity']
            pressure = rdt['pressure']
            temperature = rdt['temp']
            time = rdt['time']

            result = {}

            # build the granules for conductivity, temperature and pressure
            result['conductivity'] = ctd_L0_algorithm._build_granule(   stream_definition_id= params['conductivity'],
                field_name= 'conductivity',
                time=time,
                value= conductivity)

            result['temp'] = ctd_L0_algorithm._build_granule(stream_definition_id= params['temperature'],
                field_name= 'temp',
                time=time,
                value= temperature)

            result['pressure'] = ctd_L0_algorithm._build_granule(stream_definition_id= params['pressure'],
                field_name= 'pressure',
                time=time,
                value= pressure)

            result_list.append(result)

        return result_list

    @staticmethod
    def _build_granule(stream_definition_id=None, field_name='', value=None, time=None):
        '''
        @param param_dictionary ParameterDictionary
        @param field_name str
        @param value numpy.array

        @retval Granule
        '''
        root_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        root_rdt[field_name] = value
        root_rdt['time'] = time

        return root_rdt.to_granule()

