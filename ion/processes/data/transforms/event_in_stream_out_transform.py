#!/usr/bin/env python

"""
@brief The EventInStreamOutTransform transform listens to an event and publishes a stream containing data in the event
message

@author Swarbhanu Chatterjee
"""
from ion.core.process.transform import TransformEventListener, TransformStreamPublisher
from pyon.util.log import log
from pyon.util.containers import get_safe
from pyon.core.exception import BadRequest
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool

class EventInStreamOutTransform(TransformEventListener, TransformStreamPublisher):

    def on_start(self):
        super(EventInStreamOutTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('output'):
            raise BadRequest("For event triggered transform, please send the stream_id "
                             "using the special keyword, output")

        self.output = self.CFG.process.publish_streams.output

    def process_event(self, msg, headers):
        """
        Process the event and publish a message
        """
        out_message = '<'
        for name, value in msg.__dict___.iteritems():
            out_message += "%s : %s," %  (name, value)
        out_message += '>'

        self.publish(out_message)

    def publish(self, msg, to_name):
        """
        Publish on a stream
        """
        self.output.publish(msg=msg)