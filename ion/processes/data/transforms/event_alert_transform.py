#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.util.log import log
from pyon.event.event import EventPublisher, EventSubscriber
from ion.core.process.transform import TransformEventListener, TransformStreamListener, TransformEventPublisher
import gevent
from gevent import queue

class EventAlertTransform(TransformEventListener):

    def on_start(self):
        log.warn('EventAlertTransform.on_start()')
        super(EventAlertTransform, self).on_start()

        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # get the algorithm to use
        #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        self.timer_origin = self.CFG.get_safe('process.timer_origin', 'Interval Timer')
        self.instrument_origin = self.CFG.get_safe('process.instrument_origin', '')

        self.counter = 0
        self.event_times = []

        #-------------------------------------------------------------------------------------
        # Set up a listener for instrument events
        #-------------------------------------------------------------------------------------

        self.instrument_event_queue = gevent.queue.Queue()

        def instrument_event_received(message, headers):
            log.debug("EventAlertTransform received an instrument event here::: %s" % message)
            self.instrument_event_queue.put(message)

        self.instrument_event_subscriber = EventSubscriber(origin = self.instrument_origin,
                                                        callback=instrument_event_received)

        self.instrument_event_subscriber.start()

        #-------------------------------------------------------------------------------------
        # Create the publisher that will publish the Alert message
        #-------------------------------------------------------------------------------------

        self.event_publisher = EventPublisher()

    def on_quit(self):
        self.instrument_event_subscriber.stop()
        super(EventAlertTransform, self).on_quit()

    def process_event(self, msg, headers):
        '''
        The callback method.
        If the events satisfy the criteria, publish an alert event.
        '''

        if self.instrument_event_queue.empty():
            log.debug("no event received from the instrument. Publishing an alarm event!")
            self.publish()
        else:
            log.debug("Events were received from the instrument in between timer events. Instrument working normally.")
            self.instrument_event_queue.queue.clear()


    def publish(self):

        #-------------------------------------------------------------------------------------
        # publish an alert event
        #-------------------------------------------------------------------------------------
        self.event_publisher.publish_event( event_type= "DeviceEvent",
                                            origin="EventAlertTransform",
                                            description= "An alert event being published.")

class StreamAlertTransform(TransformStreamListener, TransformEventPublisher):

    def on_start(self):
        super(StreamAlertTransform,self).on_start()
        self.value = self.CFG.get_safe('process.value', 0)

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method.
        If the events satisfy the criteria, publish an alert event.
        '''
        log.debug('StreamAlertTransform got an incoming packet!')

        value = self._extract_parameters_from_stream(msg, "VALUE")

        if msg.find("PUBLISH") > -1 and (value < self.value):
            self.publish()

    def publish(self):
        '''
        Publish an alert event
        '''
        self.publisher.publish_event(origin="StreamAlertTransform",
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



class DemoStreamAlertTransform(TransformStreamListener, TransformEventListener, TransformEventPublisher):

    def on_start(self):
        super(DemoStreamAlertTransform,self).on_start()
        self.value = self.CFG.get_safe('process.value', 0)

        self.granules = []

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method.
        If the events satisfy the criteria, publish an alert event.
        '''
        log.debug('StreamAlertTransform got an incoming packet!')

        value = self._extract_parameters_from_stream(msg, "VALUE")

        if msg.find("PUBLISH") > -1 and (value < self.value):
            self.publish(subtype='OUT_OF_RANGE')
        else:
            self.publish(subtype='IN_RANGE')


    def process_event(self, msg, headers):
        '''
        When timer events come, if no granule has arrived since the last timer event, publish an alarm
        '''

        if self.granules.empty():
            log.debug("No granule arrived since the last timer event. Publishing an alarm!!!")
            self.publish(subtype= 'NO_DATA')
        else:
            log.debug("Granules have arrived since the last timer event.")
            self.granules.clear()


    def publish(self, subtype = None):
        '''
        Publish an alert event
        '''
        self.publisher.publish_event(origin="DemoStreamAlertTransform",
            event_type = 'DeviceStatusEvent',
            sub_type = subtype,
            description= "Event to deliver the status of instrument.")


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

