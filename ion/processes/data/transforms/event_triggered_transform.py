#!/usr/bin/env python

'''
@brief The EventTriggeredTransform transform listens for a particular kind of event. When it receives the event,
it goes into a woken up state. When it is awake it processes data according to its transform algorithm.

@author Swarbhanu Chatterjee
'''
from ion.core.process.transform import TransformEventListener, TransformDataProcess, TransformStreamPublisher
from pyon.util.log import log
from pyon.util.containers import get_safe
from pyon.core.exception import BadRequest
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

class EventTriggeredTransform_A(TransformEventListener, TransformDataProcess):

    def on_start(self):
        super(EventTriggeredTransform_A, self).on_start()

        self.awake = False

        if not self.CFG.process.publish_streams.has_key('conductivity'):
            raise BadRequest("For event triggered transform, please send the stream_id "
                             "using the special keyword, conductivity")

        self.cond_stream = self.CFG.process.publish_streams.conductivity

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.cond_stream)

    def on_quit(self):
        '''
        Stop the subscriber
        '''
        super(EventTriggeredTransform_A, self).on_quit()

    def process_event(self, msg, headers):
        '''
        Wake up the transform
        '''

        self.awake = True

    def recv_packet(self, packet, stream_route, stream_id):
        '''
        Only when the transform has been worken up does it process the received packets
        according to its algorithm and the result is published
        '''

        if self.awake: # if the transform is awake
            if packet == {}:
                return

            granule = TransformAlgorithm.execute(packet, params=self.stream_definition._id)
            self.conductivity.publish(msg=granule)
        else:
            return # the transform does not do anything with received packets when it is not yet awake

class TransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        '''
        @param input Granule
        @retval result Granule
        '''

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

class EventTriggeredTransform_B(TransformEventListener, TransformStreamPublisher):

    def on_start(self):
        super(EventTriggeredTransform_B, self).on_start()

        self.awake = False

        if not self.CFG.process.publish_streams.has_key('output'):
            raise BadRequest("For event triggered transform, please send the stream_id "
                             "using the special keyword, output")

        self.output = self.CFG.process.publish_streams.output

        # Read the parameter dict from the stream def of the stream
        pubsub = PubsubManagementServiceProcessClient(process=self)
        self.stream_definition = pubsub.read_stream_definition(stream_id=self.output)

    def on_quit(self):
        '''
        Stop the subscriber
        '''
        super(EventTriggeredTransform_B, self).on_quit()

    def process_event(self, msg, headers):
        '''
        Wake up the transform
        '''

        self.awake = True
        log.debug("awake!!!")

    def publish(self, msg, to_name):
        '''
        Publish on a stream
        '''

        if self.awake:
            self.publisher.publish(msg=msg)