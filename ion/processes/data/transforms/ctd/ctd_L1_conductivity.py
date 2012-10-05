
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_conductivity.py
@description Transforms CTD parsed data into L1 product for conductivity
'''

from pyon.ion.transforma import TransformDataProcess
from pyon.public import log
import numpy as np

### For new granule and stream interface
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDL1ConductivityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''

    def on_start(self):
        super(CTDL1ConductivityTransform, self).on_start()
        pubsub = PubsubManagementServiceClient()

        if not self.CFG.process.publish_streams.has_key('conductivity'):
            raise AssertionError("For CTD transforms, please send the stream_id using "
                                 "a special keyword (ex: conductivity)")
        self.cond_stream = self.CFG.process.publish_streams.conductivity

        # Read the parameter dict from the stream def of the stream
        stream_definition = pubsub.read_stream_definition(stream_id=self.cond_stream)
        pdict = stream_definition.parameter_dictionary
        self.cond_pdict = ParameterDictionary.load(pdict)

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """
        if packet == {}:
            returns

        granule = CTDL1ConductivityTransformAlgorithm.execute(packet, params=self.cond_pdict)
        self.conductivity.publish(msg=granule)

class CTDL1ConductivityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        '''
        @param input Granule
        @retval result Granule
        '''

        rdt = RecordDictionaryTool.load_from_granule(input)
        conductivity = rdt['conductivity']

        # create parameter settings
        cond_value = (conductivity / 100000.0) - 0.5
        # build the granule for conductivity
        result = CTDL1ConductivityTransformAlgorithm._build_granule(param_dictionary = params,
                                                                    field_name ='conductivity',
                                                                    value=cond_value)
        return result

    @staticmethod
    def _build_granule(param_dictionary=None, field_name='', value=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)
        root_rdt[field_name] = value
        log.debug("CTDL1ConductivityTransform:_build_granule_settings: logging published Record Dictionary:\n %s", str(root_rdt.pretty_print()))

        return root_rdt.to_granule()
