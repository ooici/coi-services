#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.ion.transforma import TransformEventListener, TransformStreamListener, TransformAlgorithm
from pyon.util.log import log
from ion.services.dm.utility.query_language import QueryLanguage
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher
from pyon.ion.stream import SimpleStreamSubscriber

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
import operator

class EventAlertTransform(TransformEventListener):

    def on_start(self):
        log.warn('EventAlertTransform.on_start()')
        super(EventAlertTransform, self).on_start()

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # get the algorithm to use
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        self.max_count = self.CFG.get_safe('process.max_count', 1)
        self.time_window = self.CFG.get_safe('process.time_window', 0)

        self.counter = 0
        self.event_times = []

        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher()

    def process_event(self, msg, headers):
        '''
        The callback method.
        If the events satisfy the criteria supplied through the algorithm object, publish an alert event.
        '''

        self.counter += 1

        log.warning("message we get here: %s" % msg)
        self.event_times.append(msg.ts_created)

        if self.counter == self.max_count:

            time_diff = self.event_times[self.max_count - 1] - self.event_times[0]

            if time_diff <= self.time_window:
                log.warning("inside the if statement")

                self.publish()
                self.counter = 0
                self.event_times = []

    def publish(self):

        # publish an alert event
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="EventAlertTransform",
                                            description= "An alert event being published.")
        log.warning("published the event!")

class StreamAlertTransform(TransformStreamListener):

    def on_start(self):
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)

        log.warning("queue_name: %s" % self.queue_name)


        self.subscriber = SimpleStreamSubscriber.new_subscriber(self.container, self.queue_name, self.recv_packet)
        self.subscriber.start()




        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher()

    def recv_packet(self, msg, headers):
        '''
        The callback method.
        If the events satisfy the criteria supplied through the algorithm object, publish an alert event.
        '''

        self.publish()

#        message = msg
#
#        if _algorithm_evaluation(message):
#            self.publish()

    def publish(self):

        # publish an alert event
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="StreamAlertTransform",
                                            description= "An alert event being published.")

    def _extract_parameters_from_stream(self, field_names ):

        fields = []

        #todo implement this method according to use cases

#        for field_name in field_names:
#            fields.append(  )

        log.warning("in stream alert recv_packet method, got the following fields: %s" % fields)

        return fields

    def _algorithm_evaluation(self, message):

        assert(isinstance(message, str), "the input message here should be a string")

        cond = False




        return cond




