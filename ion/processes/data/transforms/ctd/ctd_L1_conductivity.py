
'''
@author MManning
@file ion/processes/data/transforms/ctd/ctd_L1_conductivity.py
@description Transforms CTD parsed data into L1 product for conductivity
'''

from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction

# For usage: please refer to the integration tests in
# ion/processes/data/transforms/ctd/test/test_ctd_transforms.py

class CTDL1ConductivityTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the conductivity value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.
    '''
    output_bindings = ['conductivity']

    def on_start(self):
        super(CTDL1ConductivityTransform, self).on_start()

        self.cond_stream = self.CFG.process.publish_streams.values()[0]

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.cond_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """
        if packet == {}:
            return

        granule = CTDL1ConductivityTransformAlgorithm.execute(packet, params=self.stream_definition._id)
        self.publisher.publish(msg=granule)

class CTDL1ConductivityTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        conductivity = rdt['conductivity']
        cond_value = (conductivity / 100000.0) - 0.5

        for key, value in rdt.iteritems():
            if key in out_rdt:
                out_rdt[key] = value[:]

        # Update the conductivity values
        out_rdt['conductivity'] = cond_value

        # build the granule for conductivity
        return out_rdt.to_granule()
