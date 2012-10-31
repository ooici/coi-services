#!/usr/bin/env python

'''
@brief The EventInStreamOutTransform transform listens to an event and publishes a stream

@author Swarbhanu Chatterjee
'''
from ion.core.process.transform import TransformEventListener, TransformDataProcess, TransformStreamPublisher
from pyon.util.log import log
from pyon.util.containers import get_safe
from pyon.core.exception import BadRequest
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

class EventInStreamOutTransform(TransformEventListener, TransformDataProcess):

    def on_start(self):
        super(EventTriggeredTransform_A, self).on_start()

        self.awake = False

        if not self.CFG.process.publish_streams.has_key('conductivity'):
            raise BadRequest("For event triggered transform, please send the stream_id "
                             "using the special keyword, conductivity")

        self.stream_dict = self.CFG.process.publish_streams
        self.stream_definitions = DotDict()

        for binding, stream_id in self.stream_dict:
            # Read the parameter dict from the stream def of the stream
            pubsub = PubsubManagementServiceProcessClient(process=self)
            stream_def = pubsub.read_stream_definition(stream_id=stream_id)
            self.stream_definitions.binding = stream_def._id

    def on_quit(self):
        """
        Stop the subscriber
        """
        super(EventTriggeredTransform_A, self).on_quit()

    def process_event(self, msg, headers):
        """
        Wake up the transform
        """
        self.awake = True

    def recv_packet(self, packet, stream_route, stream_id):
        """
        Only when the transform has been woken up does it process the received packets
        according to its algorithm and the result is published
        """

        if self.awake: # if the transform is awake
            if packet == {}:
                return

            granule = TransformAlgorithm.execute(packet, params=self.stream_definitions)
            self.conductivity.publish(msg=granule)
        else:
            return # the transform does not do anything with received packets when it is not yet awake

class TransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        """
        @param input Granule
        @retval result Granule
        """

        rdt = RecordDictionaryTool.load_from_granule(input)
        out_rdt = None

        for binding, stream_def_id in self.stream_definitions:
            if rdt.has_key(binding):
                in_value = rdt[binding]
                out_rdt = RecordDictionaryTool(stream_definition_id=stream_def_id)
                if binding in out_rdt:
                    value = in_value[:]
                    out_rdt[binding] = value * 2

        # build the granule for conductivity
        return out_rdt.to_granule()

