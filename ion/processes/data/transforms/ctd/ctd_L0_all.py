'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L0_all.py
@description Transforms CTD parsed data into L0 streams
'''

from ion.core.process.transform import TransformDataProcess
from pyon.service.service import BaseService
from pyon.core.exception import BadRequest
from pyon.public import IonObject, RT, log
from pyon.util.arg_check import validate_true
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from pyon.net.endpoint import Publisher
from pyon.core.bootstrap import get_sys_name
from pyon.net.transport import NameTrio

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import MultiGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class ctd_L0_all(TransformDataProcess):
    """Model for a TransformDataProcess

    """
    def on_start(self):
        super(ctd_L0_all, self).on_start()
        config_streams = self.CFG.process.publish_streams
        requirement = config_streams.has_key('conductivity') \
                        and config_streams.has_key('pressure') \
                        and config_streams.has_key('temperature')

        if not requirement:
            raise AssertionError("For CTD transforms, please send the stream_ids for conductivity, temperature, pressure"
                                 " using special keywords (ex: conductivity) for each instead of just \'output \'")

        self.cond_stream = self.CFG.process.publish_streams.conductivity
        self.pres_stream = self.CFG.process.publish_streams.pressure
        self.temp_stream = self.CFG.process.publish_streams.temperature

        # Read the parameter dicts from the stream defs of the streams
        self._get_param_dicts_from_streams()

    def _get_param_dicts_from_streams(self):

        pubsub = PubsubManagementServiceClient()

        stream_def_cond = pubsub.read_stream_definition(stream_id=self.cond_stream)
        pdict = stream_def_cond.parameter_dictionary
        cond_pdict = ParameterDictionary.load(pdict)

        stream_def_pres = pubsub.read_stream_definition(stream_id=self.pres_stream)
        pdict = stream_def_pres.parameter_dictionary
        pres_pdict = ParameterDictionary.load(pdict)

        stream_def_temp = pubsub.read_stream_definition(stream_id=self.temp_stream)
        pdict = stream_def_temp.parameter_dictionary
        temp_pdict = ParameterDictionary.load(pdict)

        self.params = {}
        self.params['conductivity'] = cond_pdict
        self.params['pressure'] = pres_pdict
        self.params['temperature'] = temp_pdict

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
            result['conductivity'] = ctd_L0_algorithm._build_granule(   param_dictionary= params['conductivity'],
                                                                        field_name= 'conductivity',
                                                                        value= conductivity)

            result['temp'] = ctd_L0_algorithm._build_granule(param_dictionary= params['temperature'],
                                                           field_name= 'temp',
                                                           value= temperature)

            result['pressure'] = ctd_L0_algorithm._build_granule(param_dictionary= params['pressure'],
                                                                 field_name= 'pressure',
                                                                 value= pressure)

            result_list.append(result)

        return result_list

    @staticmethod
    def _build_granule(param_dictionary=None, field_name='', value=None):
        '''
        @param param_dictionary ParameterDictionary
        @param field_name str
        @param value numpy.array

        @retval Granule
        '''
        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)
        root_rdt[field_name] = value

        return root_rdt.to_granule()

