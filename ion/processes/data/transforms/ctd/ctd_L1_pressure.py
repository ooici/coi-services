
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_pressure.py
@description Transforms CTD parsed data into L1 product for pressure
'''

from pyon.ion.transforma import TransformDataProcess
from pyon.core.exception import BadRequest
### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDL1PressureTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''

    def on_start(self):
        super(CTDL1PressureTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('pressure'):
            raise BadRequest("For CTD transforms, please send the stream_id using "
                                 "a special keyword (ex: pressure)")

        self.pres_stream = self.CFG.process.publish_streams.pressure

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.pres_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """

        if packet == {}:
            return
        granule = CTDL1PressureTransformAlgorithm.execute(packet, params=self.stream_definition._id)
        self.pressure.publish(msg=granule)


class CTDL1PressureTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        pressure = rdt['pressure']
        pres_value = (pressure / 100.0) + 0.5

        # build the granule for pressure
        result = CTDL1PressureTransformAlgorithm._build_granule(stream_definition_id = params,
            field_name ='pressure',
            value=pres_value)
        return result

    @staticmethod
    def _build_granule(stream_definition_id=None, field_name='', value=None):

        root_rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)
        root_rdt[field_name] = value
        return root_rdt.to_granule()
