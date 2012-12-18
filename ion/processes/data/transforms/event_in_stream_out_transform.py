#!/usr/bin/env python

"""
@brief The EventToStreamTransform transform listens to an event and publishes a stream containing data in the event
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

class EventToStreamTransform(TransformEventListener, TransformStreamPublisher):

    def on_start(self):
        super(EventToStreamTransform, self).on_start()

        if not self.CFG.process.publish_streams.has_key('output'):
            raise BadRequest("For event-in/stream-out transform, please send the stream_id "
                             "using the special keyword, output")
        self.variables = self.CFG.process.variables or []

    def process_event(self, msg, headers):
        """
        Process the event and publish a message
        """
        log.debug("got the event: %s" % msg)
        out_message = '<'
        for variable in self.variables:
            log.debug("for variable::: %s" % variable)
            out_message += "%s : %s," %  (variable, getattr(msg,variable))
        out_message += '>'

        self.publish(out_message, None)

    def publish(self, msg, to_name):
        """
        Publish on a stream
        """
        log.debug("came here to publish! msg = %s" % msg)
        self.publisher.publish(msg=msg)