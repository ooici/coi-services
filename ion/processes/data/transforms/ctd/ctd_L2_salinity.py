
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L2_salinity.py
@description Transforms CTD parsed data into L2 product for salinity
'''

from pyon.ion.transforma import TransformDataProcess
from pyon.public import log
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

from seawater.gibbs import SP_from_cndr
from seawater.gibbs import cte

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class SalinityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''

    def on_start(self):
        super(SalinityTransform, self).on_start()
        if not self.CFG.process.publish_streams.has_key('salinity'):
            raise AssertionError("For CTD transforms, please send the stream_id "
                                 "using a special keyword (ex: salinity)")

        self.sal_stream = self.CFG.process.publish_streams.salinity

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceClient()
        stream_definition = pubsub.read_stream_definition(stream_id=self.sal_stream)
        pdict = stream_definition.parameter_dictionary
        self.sal_pdict = ParameterDictionary.load(pdict)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        log.info('Received incoming packet')

        if packet == {}:
            return

        granule = CTDL2SalinityTransformAlgorithm.execute(packet, params=self.sal_pdict)
        self.salinity.publish(msg=granule)


class CTDL2SalinityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)

        conductivity = rdt['conductivity']
        pressure = rdt['pressure']
        temperature = rdt['temp']

        sal_value = SP_from_cndr(r=conductivity/cte.C3515, t=temperature, p=pressure)
        # build the granule for salinity
        result = CTDL2SalinityTransformAlgorithm._build_granule(param_dictionary=params,
                                                                        field_name='salinity',
                                                                        value=sal_value)

        return result

    @staticmethod
    def _build_granule(param_dictionary=None, field_name='', value=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)
        root_rdt[field_name] = value
        return root_rdt.to_granule()
