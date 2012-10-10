'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
'''

from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import MultiGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class ctd_L0_all(TransformDataProcess):
    """Model for a TransformDataProcess

    """
    output_bindings = ['conductivity', 'pressure', 'temperature']
    def on_start(self):
        super(ctd_L0_all, self).on_start()

        config_streams = self.CFG.process.publish_streams
        requirement = config_streams.has_key('conductivity') \
                        and config_streams.has_key('pressure') \
                        and config_streams.has_key('temperature')

        if not requirement:
            raise BadRequest("For CTD transforms, please send the stream_ids for conductivity, temperature, pressure"
                                 " using special keywords (ex: conductivity) for each instead of just \'output \'")

        self.cond_stream = self.CFG.process.publish_streams.conductivity
        self.pres_stream = self.CFG.process.publish_streams.pressure
        self.temp_stream = self.CFG.process.publish_streams.temperature

        # Read the parameter dicts from the stream defs of the streams
        self._get_param_dicts_from_streams()

    def _get_param_dicts_from_streams(self):

        pubsub = PubsubManagementServiceProcessClient(process=self)

        self.stream_def_cond = pubsub.read_stream_definition(stream_id=self.cond_stream)
        self.stream_def_pres = pubsub.read_stream_definition(stream_id=self.pres_stream)
        self.stream_def_temp = pubsub.read_stream_definition(stream_id=self.temp_stream)

        self.params = {}
        self.params['conductivity'] = self.stream_def_cond._id
        self.params['pressure'] = self.stream_def_pres._id
        self.params['temperature'] = self.stream_def_temp._id

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

            result = {}

            # build the granules for conductivity, temperature and pressure
            result['conductivity'] = ctd_L0_algorithm._build_granule(   stream_definition_id= params['conductivity'],
                                                                        field_name= 'conductivity',
                                                                        value= conductivity)

            result['temp'] = ctd_L0_algorithm._build_granule(stream_definition_id= params['temperature'],
                                                           field_name= 'temp',
                                                           value= temperature)

            result['pressure'] = ctd_L0_algorithm._build_granule(stream_definition_id= params['pressure'],
                                                                 field_name= 'pressure',
                                                                 value= pressure)

            result_list.append(result)

        return result_list

    @staticmethod
    def _build_granule(stream_definition_id=None, field_name='', value=None):
        '''
        @param param_dictionary ParameterDictionary
        @param field_name str
        @param value numpy.array

        @retval Granule
        '''
        root_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        root_rdt[field_name] = value

        return root_rdt.to_granule()

