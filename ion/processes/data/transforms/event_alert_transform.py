#!/usr/bin/env python

'''
@brief The EventAlertTransform listens to events and publishes alert messages when the events
        satisfy a condition. Its uses an algorithm to check the latter
@author Swarbhanu Chatterjee
'''
from pyon.util.log import log
from pyon.core.exception import NotFound
from pyon.util.containers import DotDict, get_ion_ts
from pyon.util.arg_check import validate_is_instance, validate_true
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.core.function.transform_function import SimpleGranuleTransformFunction
from ion.core.process.transform import TransformEventListener, TransformStreamListener, TransformEventPublisher
from interface.objects import DeviceStatusType, DeviceStatusEvent, DeviceCommsEvent, DeviceCommsType
from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
import gevent
from gevent import queue
import datetime, time

class EventAlertTransform(TransformEventListener):

    def on_start(self):
        log.debug('EventAlertTransform.on_start()')
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
            log.debug("Could not extract value from the message. Please check its format.")

        return self.value



class DemoStreamAlertTransform(TransformStreamListener, TransformEventListener, TransformEventPublisher):

    def __init__(self):
        super(DemoStreamAlertTransform,self).__init__()

        # the queue of granules that arrive in between two timer events
        self.granules = gevent.queue.Queue()
        self.instrument_variable_name = None
        self.timer_origin = None
        self.timer_interval = None
        self.count = 0
        self.timer_cleanup = (None, None)
        self.origin = ''

    def on_start(self):
        super(DemoStreamAlertTransform,self).on_start()

        #-------------------------------------------------------------------------------------
        # Values that are passed in when the transform is launched
        #-------------------------------------------------------------------------------------
        self.instrument_variable_name = self.CFG.get_safe('process.variable_name', 'input_voltage')
        self.time_field_name = self.CFG.get_safe('process.time_field_name', 'preferred_timestamp')
        self.valid_values = self.CFG.get_safe('process.valid_values', [-200,200])
        self.timer_origin = self.CFG.get_safe('process.timer_origin', 'Interval Timer')
        self.timer_interval = self.CFG.get_safe('process.timer_interval', 6)

        # Check that valid_values is a list
        validate_is_instance(self.valid_values, list)

        # Start the timer
        self.ssclient = SchedulerServiceProcessClient(node=self.container.node, process=self)
        id = self._create_interval_timer_with_end_time(timer_interval=self.timer_interval, end_time=-1)

    def on_quit(self):
        super(DemoStreamAlertTransform,self).on_quit()

        self.ssclient, sid = self.timer_cleanup
        DemoStreamAlertTransform._cleanup_timer(self.ssclient, sid)

    @staticmethod
    def _cleanup_timer(scheduler, schedule_id):
        """
        Do a friendly cancel of the scheduled event.
        If it fails, it's ok.
        """
        try:
            scheduler.cancel_timer(schedule_id)
        except:
            log.debug("Couldn't cancel")

    def now_utc(self):
        return time.mktime(datetime.datetime.utcnow().timetuple())

    def _create_interval_timer_with_end_time(self,timer_interval= None, end_time = None ):
        '''
        A convenience method to set up an interval timer with an end time
        '''
        self.timer_received_time = 0
        self.timer_interval = timer_interval

        start_time = self.now_utc()
        if not end_time:
            end_time = start_time + 2 * timer_interval + 1

        # Set up the interval timer. The scheduler will publish event with origin set as "Interval Timer"
        sid = self.ssclient.create_interval_timer(start_time="now" ,
            interval=self.timer_interval,
            end_time=end_time,
            event_origin="Interval Timer",
            event_subtype="")

        self.timer_cleanup =  (self.ssclient, sid)

        return sid

    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The callback method. For situations like bad or no data, publish an alert event.

        @param msg granule
        @param stream_route StreamRoute object
        @param stream_id str
        '''

        log.debug("DemoStreamAlertTransform received a packet!: %s" % msg)

        #-------------------------------------------------------------------------------------
        # Set up the config to use to pass info to the transform algorithm
        #-------------------------------------------------------------------------------------
        config = DotDict()
        config.valid_values = self.valid_values
        config.variable_name = self.instrument_variable_name
        config.time_field_name = self.time_field_name

        #-------------------------------------------------------------------------------------
        # Store the granule received
        #-------------------------------------------------------------------------------------
        self.granules.put(msg)

        #-------------------------------------------------------------------------------------
        # Check for good and bad values in the granule
        #-------------------------------------------------------------------------------------
        bad_values, bad_value_times, self.origin = AlertTransformAlgorithm.execute(msg, config = config)

        log.debug("DemoStreamAlertTransform got the origin of the event as: %s" % self.origin)

        #-------------------------------------------------------------------------------------
        # If there are any bad values, publish an alert event for the granule
        #-------------------------------------------------------------------------------------
        if bad_values:
            # Publish the event
            self.publisher.publish_event(
                event_type = 'DeviceStatusEvent',
                origin = self.origin,
                origin_type='PlatformDevice',
                sub_type = self.instrument_variable_name,
                values = bad_values,
                time_stamps = bad_value_times,
                valid_values = self.valid_values,
                state = DeviceStatusType.OUT_OF_RANGE,
                description = "Event to deliver the status of instrument."
            )

            log.debug("DemoStreamAlertTransform published a BAD DATA event")

    def process_event(self, msg, headers):
        """
        When timer events come, if no granule has arrived since the last timer event, publish an alarm
        """
        self.count += 1

        log.debug("Got a timer event with count: %s" % self.count )

        if msg.origin == self.timer_origin and self.origin:

            if self.granules.qsize() == 0:
                # Publish the event
                self.publisher.publish_event(
                    event_type = 'DeviceCommsEvent',
                    origin = self.origin,
                    origin_type='PlatformDevice',
                    sub_type = self.instrument_variable_name,
#                    time_stamp =int(time.time() + 2208988800),  # granules use NTP not unix
                    time_stamp = get_ion_ts(),
                    state=DeviceCommsType.DATA_DELIVERY_INTERRUPTION,
                    lapse_interval_seconds=self.timer_interval,
                    description = "Event to deliver the communications status of the instrument."
                )

                log.debug("DemoStreamAlertTransform published a NO DATA event")

            else:
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

        # Retrieve the name used for the variable_name, the name used for timestamps and the range of valid values from the config
        valid_values = config.get_safe('valid_values', [-100,100])
        variable_name = config.get_safe('variable_name', 'input_voltage')
        preferred_time = config.get_safe('time_field_name', 'preferred_timestamp')

        # Get the source of the granules which will be used to set the origin of the DeviceStatusEvent and DeviceCommsEvent events

        origin = input.data_producer_id

        if not origin:
            raise NotFound("The DemoStreamAlertTransform could not figure out the origin for DeviceStatusEvent. The data_producer_id attribute should be filled for the granules that are sent to it so that it can figure out the origin to use.")

        log.debug("The origin the demo transform is listening to is: %s" % origin)

        rdt = RecordDictionaryTool.load_from_granule(input)

        # These variable_names will store the bad values and the timestamps of those values
        bad_values = []
        bad_value_times = []

        # retrieve the values and the times from the record dictionary
        values = rdt[variable_name][:]

        time_names = rdt[preferred_time][:]

        log.debug("Values unravelled: %s" % values)
        log.debug("Time names unravelled: %s" % time_names)

        indexes = [l for l in xrange(len(time_names))]

        for val, index in zip(values, indexes):
            if val < valid_values[0] or val > valid_values[1]:
                bad_values.append(val)
                arr = rdt[time_names[index]]
                bad_value_times.append(arr[index])

        log.debug("Returning bad_values: %s, bad_value_times: %s and the origin: %s" % (bad_values, bad_value_times, origin))

        # return the list of bad values and their timestamps
        return bad_values, bad_value_times, origin




