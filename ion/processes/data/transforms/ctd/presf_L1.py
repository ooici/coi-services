'''
@author Swarbhanu Chatterjee
@file ion/processes/data/transforms/ctd/presf_L0_splitter.py
@description The transform takes the DPD8_out_L0_pressure stream as input, uses the scaling factor below on the pressure
param and outputs the DPD8_out_L1_pressure stream (will create this streamdef shortly) with a pressure  param and
supporting params.
'''

from ion.core.process.transform import TransformDataProcess
from pyon.core.exception import BadRequest
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient


class PresfL1Transform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input from a CTD, extracts the pressure value and scales it according to
    the defined algorithm. If the transform
    has an output_stream it will publish the output on the output stream.

    '''
    output_bindings = ['seafloor_pressure']

    def on_start(self):
        super(PresfL1Transform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('seafloor_pressure'):
            raise BadRequest("For the PresfL1Transform, please send the stream_id using "
                             "a special keyword (ex: seafloor_pressure)")

        self.pres_stream = self.CFG.process.publish_streams.seafloor_pressure

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.pres_stream)

    def recv_packet(self, packet, stream_route, stream_id):
        """Processes incoming data!!!!
        """

        if packet == {}:
            return
        granule = PresfL1TransformAlgorithm.execute(packet, params=self.stream_definition._id)
        self.seafloor_pressure.publish(msg=granule)


class PresfL1TransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = RecordDictionaryTool(stream_definition_id=params)

        absolute_pressure = rdt['absolute_pressure']
        seafloor_pressure = absolute_pressure * 0.689475728

        for key, value in rdt.iteritems():

            cond = key=='time' or key=='port_timestamp' or key=='driver_timestamp'\
                    or  key=='internal_timestamp' or key=='preferred_timestamp' or key=='timestamp'\
                    or key=='lat' or key=='lon' or key=='absolute_pressure'

            if cond and key in out_rdt:
                out_rdt[key] = value[:]

        out_rdt['seafloor_pressure'] = seafloor_pressure

        return out_rdt.to_granule()
