#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.ion.transforma import TransformEventListener, TransformStreamListener, TransformAlgorithm
from pyon.util.log import log
from pyon.event.event import EventPublisher

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
        If the events satisfy the criteria, publish an alert event.
        '''

        self.counter += 1

        self.event_times.append(msg.ts_created)

        if self.counter == self.max_count:

            time_diff = self.event_times[self.max_count - 1] - self.event_times[0]

            if time_diff <= self.time_window:

                self.publish()
                self.counter = 0
                self.event_times = []

    def publish(self):

        #-------------------------------------------------------------------------------------
        # publish an alert event
        #-------------------------------------------------------------------------------------
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="EventAlertTransform",
                                            description= "An alert event being published.")

class StreamAlertTransform(TransformStreamListener):

    def on_start(self):
        self.value = self.CFG.get_safe('process.value', 0)


        # Create the publisher that will publish the Alert message
        self.event_publisher = EventPublisher()

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method.
        If the events satisfy the criteria, publish an alert event.
        '''

        value = self._extract_parameters_from_stream(msg, "VALUE")

        if msg.find("PUBLISH") > -1 and (value < self.value):
            self.publish()

    def publish(self):
        '''
        Publish an alert event
        '''
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="StreamAlertTransform",
                                            description= "An alert event being published.")

    def _extract_parameters_from_stream(self, msg, field ):

        tokens = msg.split(" ")

        try:
            for token in tokens:
                token = token.strip()
                if token == '=':
                    i = tokens.index(token)
                    if tokens[i-1] == field:
                        return int(tokens[i+1].strip())
        except IndexError:
            log.warning("Could not extract value from the message. Please check its format.")

        return self.value



