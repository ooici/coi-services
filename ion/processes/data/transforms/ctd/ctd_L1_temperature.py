
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_temperature.py
@description Transforms CTD parsed data into L1 product for temperature
'''
from pyon.core.exception import BadRequest
from ion.core.process.transform import TransformDataProcess

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDL1TemperatureTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    output_bindings = ['temperature']

    def on_start(self):
        super(CTDL1TemperatureTransform, self).on_start()

        self.temp_stream = self.CFG.process.publish_streams.values()[0]

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.temp_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Processes incoming data!!!!
        """
        if packet == {}:
            return
        granule = CTDL1TemperatureTransformAlgorithm.execute(packet, params=self.stream_definition._id)
        self.publisher.publish(msg=granule)

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
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        temperature = rdt['temp']
        temp_value = (temperature / 10000.0) - 10

        for key, value in rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['temp'] = temp_value

        return out_rdt.to_granule()
