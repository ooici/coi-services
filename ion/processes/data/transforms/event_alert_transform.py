#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.util.log import log
from pyon.util.arg_check import validate_is_instance, validate_true
from pyon.event.event import EventPublisher, EventSubscriber
from ion.core.process.transform import TransformEventListener, TransformStreamListener, TransformEventPublisher
from interface.objects import DeviceStatusType
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
        self.instrument_variable = self.CFG.get_safe('process.variable', 0)
        self.valid_values = self.CFG.get_safe('process.valid_values', [-200,200])
        validate_is_instance(self.valid_values, list)

        self.granules = []

    def _stringify_list(self, my_list = None):
        validate_true(len(my_list) == 2, "List has more than just the 2 variables to specify lower and upper limits")
        return "%s %s" % (my_list[0], my_list[1])

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method. For situations like bad or no data, publish an alert event.

        @param msg granule
        @param stream_route StreamRoute object
        @param stream_id str
        '''
        log.debug('StreamAlertTransform got an incoming packet!')
        good_values = self._check_values_in_granule(granule=msg, field=self.instrument_variable)

        if not good_values:
            # Data is out-of-range
            self.publish(subtype=self.instrument_variable, state= DeviceStatusType.OUT_OF_RANGE)
        else:
            # Data is in-range
            self.publish(subtype=self.instrument_variable, state=DeviceStatusType.OK)


    def process_event(self, msg, headers):
        '''
        When timer events come, if no granule has arrived since the last timer event, publish an alarm
        '''

        if self.granules.empty():
            log.debug("No granule arrived since the last timer event. Publishing an alarm!!!")
            self.publish(subtype=self.instrument_variable, state=DeviceStatusType.NO_DATA)
        else:
            log.debug("Granules have arrived since the last timer event.")
            self.granules.clear()


    def publish(self, subtype = None, state = None, value = None):
        '''
        Publish an alert event
        '''
        self.publisher.publish_event(origin="DemoStreamAlertTransform",
            event_type = 'DeviceStatusEvent',
            sub_type = subtype,
            state = state,
            value = value,
            valid_values = self.valid_values,
            description= "Event to deliver the status of instrument.")


    def _check_values_in_granule(self, granule = None, field = None):

        rdt = RecordDictionaryTool.load_from_granule(granule)

        value_array[:] = rdt[field]
        
        if value_array.min() < self.valid_values[0] or value_array.max() > self.valid_values[1]:
            return False
        else:
            return True



