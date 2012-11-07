#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.arg_check import validate_is_instance, validate_true
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from ion.core.process.transform import TransformEventListener, TransformStreamListener, TransformEventPublisher
from interface.objects import DeviceStatusType, DeviceStatusEvent, DeviceCommsEvent, DeviceCommsType
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

        if msg.origin == self.timer_origin:
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

    def __init__(self):
        super(DemoStreamAlertTransform,self).__init__()

        # the queue of granules that arrive in between two timer events
        self.granules = gevent.queue.Queue()
        self.instrument_variable_name = None
        self.timer_origin = None

    def on_start(self):
        super(DemoStreamAlertTransform,self).on_start()

        #-------------------------------------------------------------------------------------
        # Values that are passed in when the transform is launched
        #-------------------------------------------------------------------------------------
        self.instrument_variable_name = self.CFG.get_safe('process.variable_name', 'input_voltage')
        self.time_field_name = self.CFG.get_safe('process.time_field_name', 'preferred_timestamp')
        self.valid_values = self.CFG.get_safe('process.valid_values', [-200,200])
        self.timer_origin = self.CFG.get_safe('process.timer_origin', 'Interval Timer')

        # Check that valid_values is a list
        validate_is_instance(self.valid_values, list)



    def _stringify_list(self, my_list = None):
        validate_true(len(my_list) == 2, "List should have only 2 values to specify lower and upper limits")
        return "%s %s" % (my_list[0], my_list[1])

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method. For situations like bad or no data, publish an alert event.

        @param msg granule
        @param stream_route StreamRoute object
        @param stream_id str
        '''

        #-------------------------------------------------------------------------------------
        # Set up the config to use to pass info to the transform algorithm
        #-------------------------------------------------------------------------------------
        config = DotDict()
        config.valid_values = self.valid_values
        config.variable_name = self.instrument_variable_name
        config.time_field_name = self.instrument_variable_name

        log.debug("config:: %s" % config)
        log.debug('StreamAlertTransform got an incoming packet! :%s' % msg)

        #-------------------------------------------------------------------------------------
        # Store the granule received
        #-------------------------------------------------------------------------------------
        self.granules.put(msg)

        log.debug("num of granules received:::: %s " % self.granules.qsize())

        #-------------------------------------------------------------------------------------
        # Check for good and bad values in the granule
        #-------------------------------------------------------------------------------------
        bad_values, bad_value_times = AlertTransformAlgorithm.execute(msg, config = config)

        #-------------------------------------------------------------------------------------
        # If there are any bad values, publish an alert event for each of them, with information about their time stamp
        #-------------------------------------------------------------------------------------
        if bad_values:
            for bad_value, time_stamp in zip(bad_values, bad_value_times):
                # Create the event object
                event = DeviceStatusEvent(  origin = 'DemoStreamAlertTransform',
                                            sub_type = self.instrument_variable_name,
                                            value = bad_value,
                                            time_stamp = time_stamp,
                                            valid_values = self.valid_values,
                                            state = DeviceStatusType.OUT_OF_RANGE,
                                            description = "Event to deliver the status of instrument.")

                # Publish the event
                self.publisher._publish_event( event_msg = event,
                    origin=event.origin,
                    event_type = event.type_)

                log.debug("published a bad data event: %s" % bad_value)

    def process_event(self, msg, headers):
        """
        When timer events come, if no granule has arrived since the last timer event, publish an alarm
        """
        log.debug("got a timer event")

        log.debug("the number of granules that are stored::: %s" % self.granules.qsize())

        log.debug("message here!! %s" % msg)

        if msg.origin == self.timer_origin:
            if self.granules.qsize() == 0:
                log.debug("Granules have not arrived. Publishing an alarm event")
                # Create the event object
                event = DeviceCommsEvent( origin = 'DemoStreamAlertTransform',
                                        sub_type = self.instrument_variable_name,
                                        state=DeviceCommsType.DATA_DELIVERY_INTERRUPTION,
                                        description = "Event to deliver the communications status of the instrument.")
                # Publish the event
                self.publisher._publish_event( event_msg = event,
                                            origin=event.origin,
                                            event_type = event.type_)
                log.debug("event published~~~")
            else:
                log.debug("Granules have arrived since the last timer event.")
                self.granules.queue.clear()


class AlertTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        """
        Find if the input data has values, which are out of range

        @param input granule
        @param context parameter context
        @param config DotDict
        @param params list
        @param state
        @return bad_values, bad_value_times tuple of lists
        """

        log.debug("came here!!!!  %s" % input)
        log.debug("type of input::: %s" % type(input))

        rdt = RecordDictionaryTool.load_from_granule(input)

        # Retrieve the name used for the variable_name, the name used for timestamps and the range of valid values from the config
        valid_values = config.get_safe('valid_values', [-100,100])
        variable_name = config.get_safe('variable_name', 'input_voltage')
        time_field_name = config.get_safe('time_field_name', 'preferred_timestamp')

        # These variable_names will store the bad values and the timestamps of those values
        bad_values = []
        bad_value_times = []

        # retrieve the values and the times from the record dictionary
        values = rdt[variable_name][:]
        times = rdt[time_field_name][:]

        log.debug("got values from the granule: %s" % values)
        log.debug("got times from the granule: %s" % times)

        for val, t in zip(values, times):
            if val < valid_values[0] or val > valid_values[1]:
                bad_values.append(val)
                bad_value_times.append(t)

        log.debug("got bad_values: %s" % bad_values)
        log.debug("got bad_value_times: %s" % bad_value_times)

        # return the list of bad values and their timestamps
        return bad_values, bad_value_times




