
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_temperature.py
@description Transforms CTD parsed data into L1 product for temperature
'''

from pyon.ion.transforma import TransformDataProcess
from pyon.public import log
import numpy as np

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDL1TemperatureTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    def on_start(self):
        super(CTDL1TemperatureTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('temperature'):
            raise AssertionError("For CTD transforms, please send the stream_id using a "
                                 "special keyword (ex: temperature)")

        self.temp_stream = self.CFG.process.publish_streams.temperature

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceClient()
        stream_definition = pubsub.read_stream_definition(stream_id=self.temp_stream)
        pdict = stream_definition.parameter_dictionary
        self.temp_pdict = ParameterDictionary.load(pdict)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return
        granule = CTDL1TemperatureTransformAlgorithm.execute(packet, params=self.temp_pdict)
        self.temperature.publish(msg=granule)

class CTDL1TemperatureTransformAlgorithm(SimpleGranuleTransformFunction):
    '''
    The L1 temperature data product algorithm takes the L0 temperature data product and converts it into Celsius.
    Once the hexadecimal string is converted to decimal, only scaling (dividing by a factor and adding an offset) is
    required to produce the correct decimal representation of the data in Celsius.
    The scaling function differs by CTD make/model as described below.
        SBE 37IM, Output Format 0
        1) Standard conversion from 5-character hex string (Thex) to decimal (tdec)
        2) Scaling: T [C] = (tdec / 10,000) - 10
    '''

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        temperature = rdt['temp']
        temp_value = (temperature / 100000.0) - 10

        #build the granule for temperature
        result = CTDL1TemperatureTransformAlgorithm._build_granule_settings(param_dictionary = params,
                                                                            field_name ='temp',
                                                                            value=temp_value)
        return result

    @staticmethod
    def _build_granule_settings(param_dictionary=None, field_name='', value=None):

        root_rdt = RecordDictionaryTool(param_dictionary=param_dictionary)

        root_rdt[field_name] = value
        return root_rdt.to_granule()
