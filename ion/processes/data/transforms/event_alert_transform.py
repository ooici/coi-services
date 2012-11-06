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

        #-------------------------------------------------------------------------------------
        # Values that are passed in when the transform is launched
        #-------------------------------------------------------------------------------------
        self.value = self.CFG.get_safe('process.value', 0)
        self.instrument_variable = self.CFG.get_safe('process.variable', 'input_voltage')
        self.time_variable = self.CFG.get_safe('process.time_variable', 'preferred_timestamp')

        valid_values = self.CFG.get_safe('process.valid_values', [-200,200])
        validate_is_instance(valid_values, list)

        #-------------------------------------------------------------------------------------
        # Set up the config to use to pass info to the transform algorithm
        #-------------------------------------------------------------------------------------
        self.config = DotDict()
        self.config.valid_values = valid_values
        self.config.variable = self.instrument_variable
        self.config.time_variable = self.instrument_variable

        #-------------------------------------------------------------------------------------
        # the list of granules that arrive in between two timer events
        #-------------------------------------------------------------------------------------
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
        bad_values, bad_value_times = AlertTransformAlgorithm.execute(input=msg, config = self.config)

        # If there are any bad values, publish an alert event for each of them, with information about their time stamp
        if bad_values:
            for bad_value, time_stamp in zip(bad_values, bad_value_times):
                log.debug("came here to publish device status event")
                self.publisher.publish(event_type= 'DeviceStatusEvent',
                            subtype=self.instrument_variable,
                            value= bad_value,
                            time_stamp= time_stamp,
                            state= DeviceStatusType.OUT_OF_RANGE,
                            description= "Event to deliver the status of instrument.")


    def process_event(self, msg, headers):
        """
        When timer events come, if no granule has arrived since the last timer event, publish an alarm
        """
        log.debug("got a timer event")

        if self.granules.empty():
            log.debug("No granule arrived since the last timer event. Publishing an alarm!!!")
            self.publisher.publish(event_type='DeviceCommsEvent',
                        subtype=self.instrument_variable,
                        state=DeviceCommsType.DATA_DELIVERY_INTERRUPTION,
                        description='Event to deliver the communications status of a device')
        else:
            log.debug("Granules have arrived since the last timer event.")
            self.granules.clear()


class AlertTransformAlgorithm(SimpleGranuleTransformFunction):

    @staticmethod
    @SimpleGranuleTransformFunction.validate_inputs
    def execute(input=None, context=None, config=None, params=None, state=None):
        """
        Find if the input data has values, which are out of range

        :param input granule:
        :param context parameter context:
        :param config DotDict:
        :param params list:
        :param state:
        :return bad_values, bad_value_times tuple of lists:
        """

        rdt = RecordDictionaryTool.load_from_granule(input)

        # Retrieve the name used for the variable, the name used for timestamps and the range of valid values from the config
        valid_values = config.get_safe('valid_values', [-100,100])
        variable = config.get_safe('variable', 'input_voltage')
        time_variable = config.get_safe('time_variable', 'preferred_timestamp')

        # These variables will store the bad values and the timestamps of those values
        bad_values = []
        bad_value_times = []

        # retrieve the values and the times from the record dictionary
        values = rdt[variable][:]
        times = rdt[time_variable][:]

        # check which values fall out of range
        while index < len(values):
            value = values[index]
            if value < valid_values[0] or value > valid_values[1]:
                bad_values.append(value)
                bad_value_times.append(times[index])

        # return the list of bad values and their timestamps
        return bad_values, bad_value_times




