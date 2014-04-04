"""
@package ion.agents.mission_executive
@file    ion/agents/mission_executive.py
@author  Bob Fratantonio
@brief   A class for the platform mission executive
"""

# import yaml
import calendar
import gevent
from gevent.event import AsyncResult
import time
from time import gmtime

from pyon.agent.agent import ResourceAgentClient
from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.common import BaseEnum
from pyon.event.event import EventSubscriber
from pyon.event.event import EventPublisher
from pyon.public import log
from pyon.util.breakpoint import breakpoint
from pyon.util.config import Config

from ion.agents.platform.platform_agent import PlatformAgentEvent
from ion.agents.platform.platform_agent import PlatformAgentState


from interface.objects import AgentCommand


class MissionEvents(BaseEnum):
    """
    Acceptable mission events.
    TODO: Define all possible events that a mission can respond to
    """
    #Shallow Profiler Events
    PROFILER_AT_CEILING = 'atceiling'
    PROFILER_AT_FLOOR = 'atfloor'
    PROFILER_AT_STEP = 'atstep'
    PROFILER_AT_DEPTH = 'atdepth'
    PROFILER_GO_TO_COMPLETE = 'gotocomplete'
    PROFILER_IDLE_TIMEOUT = 'idletimeout'
    PROFILER_SYSTEM_ERROR = 'systemerror'
    PROFILER_MISSION_COMPLETE = 'missioncomplete'


class MissionCommands(BaseEnum):
    """
    Acceptable mission commands and associated parameters
    """
    # General commands
    WAIT = 'wait'
    SAMPLE = 'sample'
    CALIBRATE = 'calibrate'

    # HD Camera commands
    ZOOM = 'zoom'
    PAN = 'pan'
    TILT = 'tilt'
    LIGHTS = 'lights'
    LASERS = 'lasers'

    # Shallow Profiler
    LOAD = 'loadmission'
    RUN = 'runmission'
    SET_ASCENT_SPEED = 'setascentspeed'
    SET_DESCENT_SPEED = 'setdescentspeed'
    SET_DEPTH_ALARM = 'setdepthalarm'
    GO_TO_DEPTH = 'gotodepth'
    GET_STATUS = 'getstatus'
    GET_LIMITS = 'getlimits'

    # Create dict of associations
    all_cmds = {
        CALIBRATE: [],
        SAMPLE: ['duration', 'units', 'interval'],
        WAIT: ['duration', 'units'],
        LASERS: ['power'],
        LIGHTS: ['power'],
        PAN: ['angle', 'rate'],
        TILT: ['angle', 'rate'],
        ZOOM: ['level'],
        LOAD: ['missionIndex'],
        RUN: ['missionIndex'],
        GO_TO_DEPTH: ['depth'],
        SET_ASCENT_SPEED: ['speed'],
        SET_DESCENT_SPEED: ['speed'],
        SET_DEPTH_ALARM: ['depth'],
        GET_LIMITS: [],
        GET_STATUS: [],
        }


class MissionLoader(object):
    """
    MissionLoader class is used to parse a mission file, check the mission logic
    and save the mission as a dict
    """

    mission_entries = []

    def add_entry(self, instrument_id='', start_time=0, duration=0,
                  num_loops=0, loop_value=0, event = {}, mission_cmds=[]):

        self.mission_entries.append({"instrument_id": instrument_id,
                                    "start_time": start_time,
                                    "duration": duration,
                                    "num_loops": num_loops,
                                    "loop_value": loop_value,
                                    "event": event,
                                    "mission_cmds": mission_cmds})

    def count_entries(self):
        return len(self.mission_entries)

    def sort_entries(self):
        self.mission_entries = sorted(self.mission_entries,  key=lambda k: k["start_time"])

    def get_entry_all(self, id_):
        return self.mission_entries[id_]

    def print_entry_all(self):
        log.debug(self.mission_entries)

    def delete_entry(self, id_):
        self.mission_entries.pop(id_)

    def calculate_next_interval(self, id_):
        current_time = time.time()
        start_time = self.mission_entries[id_]["start_time"]
        loop_duration = self.mission_entries[id_]["loop_duration"]

        if start_time < current_time:
            next_interval = start_time
            while next_interval < current_time:
                next_interval += loop_duration
            print "Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug("Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            print "Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time + loop_duration))
            log.debug("Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time + loop_duration)))
            return next_interval - current_time
        else:
            print "Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug("Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            print "Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
            log.debug("Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))
            return (start_time - current_time) + loop_duration

    def check_start_time(self, schedule, loop_duration):
        """
        Check mission start time
        """
        start_time_string = schedule['startTime']
        if not start_time_string or start_time_string.lower() == 'none':
            start_time = None
        else:
            try:
                # Get start time
                start_time = calendar.timegm(time.strptime(start_time_string, '%m/%d/%Y %H:%M:%S'))
                current_time = time.time()

                # Compare mission start time to current time
                if (current_time > start_time):
                    if loop_duration > 0:
                        nloops = int((current_time-start_time)/loop_duration)+1
                        start_time += nloops*loop_duration
                    else:
                        print "MissionLoader: validate_schedule: Start time has already elapsed"
                        log.debug("MissionLoader: validate_schedule: Start time has already elapsed")
                        # raise

            except ValueError:
                # log.error("MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))
                print "MissionLoader: validate_schedule: startTime format error: " + str(start_time_string)
                log.error("MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))

            print "Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(current_time))
            log.debug("Current time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(current_time)))
            print "Start time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
            log.debug("Start time is: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))

        return start_time

    def check_intersections(self, indices):
        """
        In the case of a single instrument with multiple missions,
        check missions for schedule intersections
        """
        start_times = []
        mission_durations = []
        loop_durations = []
        num_loops = []

        #Extract all the relevant info from the duplicate instrument missions
        for index in indices:
            if self.mission_entries[index]['start_time']:
                start_times.append(self.mission_entries[index]['start_time'])
                mission_durations.append(self.mission_entries[index]['duration'])
                loop_durations.append(self.mission_entries[index]['loop_duration'])
                num_loops.append(self.mission_entries[index]['num_loops'])

        if start_times:
            # Start times don't conflict with mission duration
            # Now check possible loop conflicts
            mission_all_times = []
            for n in range(len(start_times)):
                if num_loops[n] == -1:
                    #Checking the first 100 times should work
                    num_loops[n] = 100

                #Create list of mission start times and end times
                start = range(start_times[n], start_times[n] + (num_loops[n] * loop_durations[n]), loop_durations[n])
                end = [x + mission_durations[n] for x in start]
                mission_all_times.append(zip(start, end))

            # This only compares adjacent missions (only works for 2 missions)
            # TODO: Update logic to work for multiple instrument missions
            for n in range(1, len(start_times)):
                for sublist1 in mission_all_times[n-1]:
                    for sublist2 in mission_all_times[n]:
                        if (sublist1[0] >= sublist2[0] and sublist1[0] <= sublist2[1]) or (
                                sublist1[1] >= sublist2[0] and sublist1[1] <= sublist2[1]):
                            print 'Conflict: ' + str(sublist1) + str(sublist2)
                            log.error('Conflict: ' + str(sublist1) + str(sublist2))
                            raise Exception('Mission Error: Scheduling conflict')

        return True

    def verify_command_and_params(self, cmd='', params={}):
        """
        Verify that specified command is defined.
        """

        if cmd not in MissionCommands.all_cmds:
            raise Exception('Mission Error: %s Mission command not recognized' % cmd)
        for param in params:
            if param not in MissionCommands.all_cmds[cmd]:
                raise Exception('Mission Error: %s Mission parameter not recognized' % param)

    def calculate_loop_duration(self, schedule):
        """
        Calculate loop duration if given.
        """

        num_loops = schedule['loop']['quantity']
        loop_value = schedule['loop']['value']

        if type(loop_value) == str:
            # TODO: Check that event is valid
            pass
            #This is an event driven loop, check event cases
        elif loop_value == -1 or loop_value > 1:
            loop_units = schedule['loop']['units']
            if loop_units.lower() == 'days':
                loop_value *= 3600*24
            elif loop_units.lower() == 'hrs':
                loop_value *= 3600
            elif loop_units.lower() == 'mins':
                loop_value *= 60

        return loop_value, num_loops

    def calculate_mission_duration(self, mission_params={}):
        """
        Check the mission commands and parameters for duration
        """
        mission_duration = 0
        # Check mission duration
        # TODO: Add all commands that include time duration
        for index, items in enumerate(mission_params):
            #Calculate mission duration
            command = items['command'].lower()
            params = items['params']
            self.verify_command_and_params(command, params)
            if command == 'wait':
                    duration = params['duration']
                    units = params['units']
            elif command == 'sample':
                    duration = params['duration']
                    units = params['units']
            else:
                units = None
                duration = 0

            if units == 'days':
                duration *= 86400
            elif units == 'hrs':
                duration *= 3600
            elif units == 'mins':
                duration *= 60

            # For convenience convert time commands to seconds
            if units:
                mission_params[index]['params']['duration'] = duration
                mission_params[index]['params']['units'] = 'secs'

            mission_duration += duration

        return mission_duration

    def check_event(self, schedule):
        """
        Verify the mission event
        """
        event = schedule['event']
        event_id = event['eventID']
        parent_id = event['parentID']

        if not event_id:
            log.error('Mission event not specified')
            raise Exception('Mission event not specified')
        elif not parent_id:
            log.error('Mission event parentID not specified')
            raise Exception('Mission event parentID not specified')

        return event

    def validate_schedule(self, mission={}):
        """
        Check the mission parameters for scheduling conflicts
        """

        for current_mission in mission:
            # platform_id = current_mission['platformID']
            schedule = current_mission['schedule']
            mission_params = current_mission['missionParams']

            # instrument_name.append(current_mission['name'])
            instrument_id = current_mission['instrumentID']

            print instrument_id

            mission_duration = self.calculate_mission_duration(mission_params)
            loop_duration, num_loops = self.calculate_loop_duration(schedule)

            if (loop_duration and loop_duration < mission_duration):
                log.error('Mission File Error: Mission duration > scheduled loop duration')
                raise Exception('Mission Error: Mission duration greater than scheduled loop duration')

            start_time = self.check_start_time(schedule, loop_duration)

            if start_time:
                # Timed mission
                event = None
            else:
                # Event Driven Mission
                event = self.check_event(schedule)

            #Add mission entry
            self.add_entry(instrument_id, start_time, mission_duration,
                           num_loops, loop_duration, event, mission_params)

        print self.mission_entries

        #Sort mission entries by start time
        self.sort_entries()

        instrument_id = []
        for instrument in self.mission_entries:
            instrument_id.append(instrument['instrument_id'])

        #Return indices of duplicate instruments to check schedules
        indices = [i for i, x in enumerate(instrument_id) if instrument_id.count(x) > 1]

        #Now check timing schedule of duplicate instruments
        if len(indices) > 1:
            return self.check_intersections(indices)
        else:
            return True

    def load_mission_file(self, filename):
        """
        Load, parse, and check the mission
        """
        self.filename = filename

        print 'Parsing ' + filename.split('/')[-1]
        log.debug('Parsing ' + filename.split('/')[-1])

        mission_dict = Config([filename]).data

        # with open(filename) as f:
        #     mission_dict = yaml.safe_load(f)

        self.raw_mission = mission_dict['mission']

        return self.validate_schedule(self.raw_mission)


class MissionScheduler(object):
    """
    MissionScheduler takes care of the command/control and associated timing for a
    platform mission
    """

    def __init__(self, platform_agent_client = None, instrument_obj = None, mission = []):
        # TODO: Implement within the platform agent
        print 'Initialize Mission Scheduler'
        self.pa_client = platform_agent_client
        self.instruments = instrument_obj

        # Define max number of agent command retries
        self.max_attempts = 3

        # Initialize error events
        self.error_events_received = []

        # Initialize list of error event subscribers
        # self.error_event_subscriber = []

        # Should match the resource id in test_mission_executive.py
        self.profiler_resource_id = 'FakeID'

        # Start up the platform
        self.startup_platform()

        if mission:
            self.schedule(mission)
        else:
            log.error('Mission Scheduler Error: No mission')
            raise Exception('Mission Scheduler Error: No mission')

    def schedule(self, missions):
        """
        Set up gevent threads for each mission
        """
        self.threads = []
        for mission in missions:
            start_time = mission['start_time']

            # There are two types of mission schedules: timed and event
            if start_time:
                # Timed schedule
                start_in = start_time - time.time() if (time.time() < start_time) else 0
                print 'Mission start in ' + str(int(start_in)) + ' seconds'
                log.debug('Mission start in ' + str(int(start_in)) + ' seconds')
                self.threads.append(gevent.spawn_later(start_in, self.run_timed_mission, mission))
            else:
                # Event driven scheduler
                event_id = mission['event']['eventID']
                log.debug('Event driven mission started. Waiting for ' + event_id)
                print 'Event driven mission started. Waiting for ' + event_id
                self.threads.append(gevent.spawn(self.run_event_driven_mission, mission))

        gevent.joinall(self.threads)

    def parse_command(self, ia_client, cmd):
        """
        @param cmd          Mission command to be parsed
        """

        command = cmd['command'].lower()
        params = cmd['params']

        if command == 'wait':
            print ('Waiting ' + str(params['duration']) + ' Seconds ' +
                   time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            gevent.sleep(params['duration'])
            print 'Wait Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug('Wait Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))

        elif command == 'sample':

            self.send_command(ia_client, command, params)

            print ('Sampling ' + str(params['duration']) + ' Seconds ' +
                   time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            gevent.sleep(params['duration'])
            print 'Sample Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug('Sample Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))

            self.send_command(ia_client, 'stop')

        elif command == 'calibrate':

            print 'Calibating ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug('Calibating ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            self.send_command(ia_client, command)

            # gevent.sleep(params['duration'])
            print 'Calibrating Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            log.debug('Calibrating Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))

            self.send_command(ia_client, 'stop')

    def send_command(self, ia_client, command, params = []):
        """
        Send agent command
        @param command      Mission command to be parsed
        @param params       Optional mission command parameters
        """

        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

        attempt = 0
        state = ia_client.get_agent_state()

        if command == 'sample':
            # A sample command has a sample duration and frequency
            SBEparams = [SBE37Parameter.INTERVAL]
            reply = ia_client.get_resource(SBEparams)
            print 'Sample Interval= ' + str(reply[SBE37Parameter.INTERVAL])
            log.debug('Sample Interval= ' + str(reply[SBE37Parameter.INTERVAL]))

            if params and params['interval'] != reply[SBE37Parameter.INTERVAL]:
                ia_client.set_resource({SBE37Parameter.INTERVAL: params['interval']})
                reply = ia_client.get_resource(SBEparams)

            while (attempt < self.max_attempts) and (state != ResourceAgentState.STREAMING):
                attempt += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
                ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state
                log.debug(state)

        elif command == 'stop':
            while (attempt < self.max_attempts) and (state != ResourceAgentState.COMMAND):
                attempt += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
                ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state
                log.debug(state)

        elif command == 'calibrate':
            while (attempt < self.max_attempts) and (state != ResourceAgentState.CALIBRATE):
                attempt += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.CALIBRATE)
                ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state
                log.debug(state)

        elif command == 'idle':
            while (attempt < self.max_attempts) and (state != ResourceAgentState.UNINITIALIZED):
                attempt += 1
                cmd = AgentCommand(command=ResourceAgentEvent.RESET)
                ia_client.execute_agent(cmd)
                state = ia_client.get_agent_state()
                print state
                log.debug(state)

    def execute_mission_commands(self, ia_client, mission_cmds):
        """
        Loop through the mission commands sequentially
        """
        for cmd in mission_cmds:
            # Parse command and parameters
            try:
                self.parse_command(ia_client, cmd)
            finally:
                # Check for agent error events
                if len(self.error_events_received) > 0:
                    # Check the error
                    print 'Mission error dectected'
                    log.error('Mission error detected')
                    return False

        return True

    def run_timed_mission(self, mission):
        """
        Run a timed mission
        @param mission      Mission dictionary
        """
        instrument_id = mission['instrument_id']
        ia_client = self.instruments[instrument_id]
        self.check_preconditions(ia_client)

        start_time = mission['start_time']
        loop_running = True
        loop_count = 0

        # Master loop
        while loop_running:
            # Wake up instrument if necessary (may have timed out)
            # self.wake_up_instrument()

            loop_running = self.execute_mission_commands(ia_client, mission['mission_cmds'])

            if loop_running:
                # Commands have been executed - increment loop count
                loop_count += 1

                # Calculate next start time
                if (mission['num_loops']) > 0 and loop_count >= mission['num_loops']:
                    break

                start_time += mission['loop_value']
                print "Next Sequence starts at " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
                log.debug("Next Sequence starts at " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))

                # Wait until next start
                while (time.time() < start_time):
                    gevent.sleep(1)
            else:
                # TODO: Error handling
                print 'Mission error dectected'
                log.error('Mission error dectected')

        # At the end of a command loop, put instrument in idle
        # cmd = 'idle'
        # self.send_command(ia_client, cmd)
        # self.kill_mission()
        # self.shutdown_platform()

    def run_event_driven_mission(self, mission):
        """
        Run an event driven mission
        @param mission      Mission dictionary
        """
        from mi.core.instrument.instrument_driver import DriverEvent

        instrument_id = mission['instrument_id']
        ia_client = self.instruments[instrument_id]
        self.check_preconditions(ia_client)

        # Get the agent client for the device whos event needs monitoring
        parent_id = mission['event']['parentID']
        event_id = mission['event']['eventID']

        if parent_id in self.instruments:
            ia_event_client = self.instruments[parent_id]
        else:
            raise Exception('Parent ID unavailable')

        origin = ia_event_client.resource_id

        # Map event ID to something meaningful
        if event_id == 'stop':
            capture_event = dict(event_type='ResourceAgentCommandEvent',
                                 id=DriverEvent.STOP_AUTOSAMPLE)
        elif event_id == 'ShallowProfilerStep':
            capture_event = dict(event_type='ResourceAgentResourceStateEvent',
                                 id=MissionEvents.PROFILER_AT_STEP)
            origin = self.profiler_resource_id
        elif event_id == 'ShallowProfilerAtCeiling':
            capture_event = dict(event_type='ResourceAgentResourceStateEvent',
                                 id=MissionEvents.PROFILER_AT_CEILING)
            origin = self.profiler_resource_id

        #-------------------------------------------------------------------------------------
        # Set up the subscriber to catch the mission event
        #-------------------------------------------------------------------------------------
        def callback_for_mission_events(event, *args, **kwargs):

            # Check which type of event is being monitored
            # if capture_event['event_type'] == 'ResourceAgentCommandEvent':
            for attr in dir(event):
                # An event was captured. Check that it is the correct event
                if event[attr] == capture_event['id']:
                    # breakpoint(locals(), globals())
                    # Execute the mission
                    success = self.execute_mission_commands(ia_client, mission['mission_cmds'])

                    if not success:
                        raise Exception('Mission Error')

        self.mission_event_subscriber = EventSubscriber(event_type=capture_event['event_type'],
                                                        origin=origin,
                                                        callback=callback_for_mission_events)

        self.mission_event_subscriber.start()

    def check_preconditions(self, ia_client):
        """
        Mission precondition checks
        """
        self.start_error_event_subscriber(ia_client)

    def check_postconditions(self, ia_client):
        """
        Mission postcondition checks
        """
        self.stop_error_event_subscriber(ia_client)

    def startup_platform(self):
        """
        Verify platform is up and running in the MISSION_COMMAND state
        # TODO Error handling if attempt maxes out
        """
        from pyon.public import CFG
        self.receive_timeout = CFG.endpoint.receive.timeout

        state = self.pa_client.get_agent_state()

        if state != PlatformAgentState.COMMAND:
            # Initialize platform
            if state == PlatformAgentState.UNINITIALIZED:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.INACTIVE):
                    attempt += 1
                    self.platform_inactive_state()
                    state = self.pa_client.get_agent_state()

            # Go active
            if state == PlatformAgentState.INACTIVE:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.IDLE):
                    attempt += 1
                    self.platform_idle_state()
                    state = self.pa_client.get_agent_state()

            # Run
            if state == PlatformAgentState.IDLE:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.COMMAND):
                    attempt += 1
                    self.platform_command_state()
                    state = self.pa_client.get_agent_state()

            # # Run Mission
            # if state == PlatformAgentState.COMMAND:
            #     attempt = 0
            #     while (attempt < self.max_attempts and state != PlatformAgentState.MISSION_COMMAND):
            #         attempt += 1
            #         self.platform_mission_running_state()
            #         state = self.pa_client.get_agent_state()

    #-------------------------------------------------------------------------------------
    # Platform commands
    #-------------------------------------------------------------------------------------

    def platform_inactive_state(self):
        """
        Put platform in the INACTIVE state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        self.pa_client.execute_agent(cmd, timeout=self.receive_timeout)

    def platform_idle_state(self):
        """
        Put platform in the IDLE state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)

    def platform_command_state(self):
        """
        Put platform in the COMMAND state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)

    def platform_mission_running_state(self):
        """
        Put platform in the MISSION_COMMAND state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN_MISSION, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)

    def platform_go_inactive(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)
        state = self.pa_client.get_agent_state()
        print state
        log.debug(state)

    def platform_reset(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RESET, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)
        state = self.pa_client.get_agent_state()
        print state
        log.debug(state)

    def shutdown(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.SHUTDOWN, kwargs=kwargs)
        self.pa_client.execute_agent(cmd)
        state = self.pa_client.get_agent_state()
        print state
        log.debug(state)

    def shutdown_platform(self):
        try:
            self.platform_go_inactive()
            self.platform_reset()
        finally:  # attempt shutdown anyway
            self.shutdown()
            self.stop_error_event_subscriber

    #-------------------------------------------------------------------------------------
    # Error handling
    #-------------------------------------------------------------------------------------
    def check_error(self, error_event):
        """
        Error handling
        """
        print 'Checking error...'
        if error_event['error_code'] == 409:
            # Instrument State exception - do something
            for k, v in self.instruments.items():
                if error_event['origin'] == v.resource_id:
                    ia_client = v
            print error_event['error_code']

    #------------------------------------------------------------------------------
    # Event helpers. Taken from ion/agents/instrument/test/test_instrument_agent.py
    #------------------------------------------------------------------------------
    def start_error_event_subscriber(self, _ia_client):
        """
        Start a subscriber to the instrument agent error events.
        @_ia_client Instrument agent client to subsribe to
        """

        def get_error_event(*args, **kwargs):
            log.info('Mission recieved ION event: args=%s, kwargs=%s, event=%s.',
                     str(args), str(kwargs), str(args[0]))
            print('Mission recieved ION event: args=%s, kwargs=%s, event=%s.',
                  str(args), str(kwargs), str(args[0]))

            self.error_events_received.append(args[0])
            self.async_error_event_result.set()
            self.check_error(args[0])

            # if self.error_event_count > 0 and \
            #         self.error_event_count == len(self.error_events_received):
            #     self.async_event_result.set()

        # Event array and async event result.
        # self.error_events_received = []
        self.async_error_event_result = AsyncResult()

        self.error_event_subscriber = EventSubscriber(
            event_type='ResourceAgentErrorEvent',
            callback=get_error_event,
            origin=_ia_client.resource_id)

        # self.error_event_subscriber.append()

        self.error_event_subscriber.start()
        self.error_event_subscriber._ready_event.wait(timeout=5)

    def _stop_error_event_subscriber(self):
        """
        Stop event subscribers on cleanup.
        """
        self.error_event_subscriber.stop()
        self.error_event_subscriber = None

if __name__ == "__main__":  # pragma: no cover
    """
    Stand alone to check the mission loading/parsing capabilities
    """
    filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"

    mission = MissionLoader()
    mission.load_mission_file(filename)
