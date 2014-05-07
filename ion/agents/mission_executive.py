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
from pyon.public import log
from pyon.util.breakpoint import breakpoint
from pyon.util.config import Config

from ion.agents.platform.platform_agent_enums import PlatformAgentEvent
from ion.agents.platform.platform_agent_enums import PlatformAgentState
from ion.agents.platform.rsn.rsn_platform_driver import RSNPlatformDriverEvent

from ion.core.includes.mi import DriverEvent

from interface.objects import AgentCommand
from interface.objects import AgentCapability
from interface.objects import CapabilityType


class MissionEvents(BaseEnum):
    """
    Acceptable fake mission events.
    """
    #Shallow Profiler Events
    PROFILER_AT_CEILING = 'atCeiling'
    PROFILER_AT_FLOOR = 'atFloor'
    PROFILER_AT_STEP = 'atStep'
    PROFILER_AT_DEPTH = 'atDepth'
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
    accepted_error_values = ['abort', 'abortMission', 'retry', 'skip']

    def add_entry(self, instrument_id=[], error_handling = {}, start_time=0, loop={}, event = {},
                  premission_cmds=[], mission_cmds=[], postmission_cmds=[]):

        self.mission_entries.append({"instrument_id": instrument_id,
                                    "error_handling": error_handling,
                                    "start_time": start_time,
                                    "loop": loop,
                                    "event": event,
                                    "premission_cmds": premission_cmds,
                                    "mission_cmds": mission_cmds,
                                    "postmission_cmds": postmission_cmds})

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
        loop_duration = self.mission_entries[id_]["loop"]["loop_duration"]

        if start_time < current_time:
            next_interval = start_time
            while next_interval < current_time:
                next_interval += loop_duration
            log.debug("[mm] Current time is: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            log.debug("[mm] Next start at: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time + loop_duration)))
            return next_interval - current_time
        else:
            log.debug("[mm] Current time is: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            log.debug("[mm] Next start at: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))
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
                        log.debug("[mm] MissionLoader: validate_schedule: Start time has already elapsed")
                        # raise

            except ValueError:
                # log.error("MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))
                log.error("[mm] MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))

            log.debug("[mm] Current time is: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(current_time)))
            log.debug("[mm] Start time is: %s", time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))

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
                            log.error('[mm] Mission Error: Scheduling conflict: ' + str(sublist1) + str(sublist2))
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

    def parse_loop_parameters(self, schedule):
        """
        Parse loop parameters if given.
        """

        num_loops = schedule['loop']['quantity']
        loop_value = schedule['loop']['value']

        if num_loops:
            loop_units = schedule['loop']['units']
            if loop_units.lower() == 'days':
                loop_value *= 3600*24
            elif loop_units.lower() == 'hrs':
                loop_value *= 3600
            elif loop_units.lower() == 'mins':
                loop_value *= 60
        else:
            num_loops = None
            loop_value = None

        loop_parameters = {'loop_duration': loop_value, 'num_loops': num_loops}

        return loop_parameters

    def parse_error_parameters(self, error_parameters):
        """
        Parse the error parameters - default and maxRetries
        """

        if error_parameters['default'] not in self.accepted_error_values:
            error_parameters['default'] = 'retry'
        error_parameters['maxRetries'] = int(error_parameters['maxRetries'])

        return error_parameters

    def parse_mission_sequence(self, mission_sequence={}, instrument_id = []):
        """
        Check the mission commands and parameters for duration
        """
        mission_duration = 0
        mission_params = []

        if not mission_sequence:
            return [], mission_duration

        # Check mission duration
        for index, items in enumerate(mission_sequence):
            #Calculate mission duration
            command = items['command']
            error = items['onError']

            if error not in self.accepted_error_values:
                error = None

            if ',' in command:
                # Instrument ID is explicitly stated
                instrument, command = command.strip().split(',')
            else:
                if len(instrument_id) == 1:
                    instrument = instrument_id[0]

            # self.verify_command_and_params(command, params)
            if '(' in command and ')' in command:
                cmd_method, rest = command.strip().split('(')
                if '{' in rest and '}' in rest:
                    cmd, rest = rest.split('{')
                    param = rest.split('}')[0]
                    param = float(param) if '.' in param else int(param)
                else:
                    cmd = rest.split(')')[0]
                    param = None
            else:
                raise Exception('Error in mission command string')

            if cmd_method.lower() == 'wait':
                param = float(cmd) if '.' in cmd else int(cmd)
                cmd = cmd_method
                duration = param * 60
            else:
                duration = 0

            mission_duration += duration
            mission_params.append({'instrument_id': instrument,
                                   'method': cmd_method,
                                   'command': cmd,
                                   'parameters': param,
                                   'error': error})

        return mission_params, mission_duration

    def check_types(self, value, _type):
        """
        Check the mission file contents types
        """
        if type(value) != _type:
            log.debug("[mm] Mission Executive Parser Warning: value %s is not %s", value, _type)
            return _type(value)
        else:
            return value

    def check_event(self, schedule):
        """
        Verify the mission event
        """
        event = schedule['event']
        event_id = event['eventID']
        parent_id = event['parentID']

        if not event_id:
            log.error('[mm] Mission event not specified')
            raise Exception('Mission event not specified')
        elif not parent_id:
            log.error('[mm] Mission event parentID not specified')
            raise Exception('Mission event parentID not specified')

        return event

    def validate_schedule(self, mission={}):
        """
        Check the mission parameters for scheduling conflicts
        """

        for current_mission in mission:
            # platform_id = current_mission['platformID']
            instrument_id = current_mission['instrumentID']
            schedule      = current_mission['schedule']
            if type(instrument_id) == str:
                instrument_id = [instrument_id]

            premission_sequence  = current_mission['preMissionSequence']
            mission_sequence     = current_mission['missionSequence']
            postmission_sequence = current_mission['postMissionSequence']

            error_parameters = current_mission['errorHandling']

            premission_params, _ = self.parse_mission_sequence(premission_sequence, instrument_id)
            mission_params, mission_duration = self.parse_mission_sequence(mission_sequence, instrument_id)
            postmission_params, _ = self.parse_mission_sequence(postmission_sequence, instrument_id)
            loop_params = self.parse_loop_parameters(schedule)

            loop_duration = loop_params['loop_duration']
            if (loop_duration and loop_duration < mission_duration):
                log.error('[mm] Mission File Error: Mission duration > scheduled loop duration')
                raise Exception('Mission Error: Mission duration greater than scheduled loop duration')

            error = self.parse_error_parameters(error_parameters)
            start_time = self.check_start_time(schedule, loop_duration)

            if start_time:
                # Timed mission
                event = None
            else:
                # Event Driven Mission
                event = self.check_event(schedule)

            #Add mission entry
            self.add_entry(instrument_id, error, start_time, loop_params, event,
                           premission_params, mission_params, postmission_params)

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

        log.debug('[mm] Parsing %s', filename.split('/')[-1])

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

    def __init__(self, platform_agent, instruments, mission):

        import pprint
        pformat = pprint.PrettyPrinter().pformat
        log.debug('[mm] MissionScheduler: instruments=%s\nmission=%s',
                  pformat(instruments), pformat(mission))

        self.platform_agent = platform_agent
        self.instruments = instruments
        self.mission = mission

        log.debug('[mm] MissionScheduler: instruments=%s\nmission=%s',
                  pformat(instruments), pformat(mission))

        # Define max number of agent command retries
        self.max_attempts = mission[0]['error_handling']['maxRetries']
        self.default_error = mission[0]['error_handling']['default']

        # Initialize error events
        self.error_events_received = []

        # Initialize list of error event subscribers
        # self.error_event_subscriber = []

        # Initialize list of mission event subscribers
        self.mission_event_subscribers = []

        # Initialize list of mission threads
        self.threads = []

        # Boolean to hold global mission abort status
        self.mission_aborted = False

        # Should match the resource id in test_mission_executive.py
        self.profiler_resource_id = 'FakeID'

    def run_mission(self):
        log.debug('[mm] run_mission: mission=%s', self.mission)

        # Start up the platform
        self.startup_platform()

        self.schedule(self.mission)

    def abort_mission(self):
        """
        Set up gevent threads for each mission
        """

        # Only need the abort sequence once...
        if not self.mission_aborted:
            # For event driven missions, stop event subscribers
            for subscriber in self.mission_event_subscribers:
                subscriber.stop()
            self.mission_event_subscribers = None

            # Take instruments out of streaming and put platform into command state
            for instrument, client in self.instruments.iteritems():
                self.instrument_abort_sequence(client)

            self.mission_aborted = True

            #TODO Publish error as event

            log.error('[mm] Mission Aborted')
            raise Exception('Mission Aborted')

    def kill_all_mission_threads(self):
        log.debug('[mm] kill_mission_threads: mission=%s', self.mission)
        for thread in self.threads:
            if bool(thread):
                # thread.GreenletExit()
                thread.kill()
        # TODO

    def schedule(self, missions):
        """
        Set up gevent threads for each mission
        """

        for mission in missions:
            start_time = mission['start_time']

            # There are two types of mission schedules: timed and event
            if start_time:
                # Timed schedule
                self.threads.append(gevent.spawn(self.run_timed_mission, mission))
            else:
                # Event driven scheduler
                self.threads.append(gevent.spawn(self.run_event_driven_mission, mission))

        log.debug('[mm] schedule: waiting for mission to complete')
        gevent.joinall(self.threads)

    def send_command(self, agent_client, cmd):
        """
        Send agent command
        @param agent_client         Instrument/platform agent client
        @param cmd                  Mission command to be parsed
        """

        method = cmd['method']
        command = cmd['command']
        parameters = cmd['parameters']

        log.debug('[mm] Send mission command =  %s - %s', method, command)

        retval = agent_client.get_capabilities()
        agt_cmds, agt_pars, res_cmds, res_iface, res_pars = self.sort_capabilities(retval)

        # RSN_PLATFORM_DRIVER_TURN_ON_PORT
        # Get ports

        # kwargs = dict(ports=None)
        # cmd = AgentCommand(command=PlatformAgentEvent.GET_RESOURCE, kwargs=kwargs)
        # retval = self.platform_agent.execute_agent(cmd)

        # Turn on port
        # kwargs = dict(
        #     port_id = port_id
        # )
        # cmd = AgentCommand(command=RSNPlatformDriverEvent.TURN_OFF_PORT, kwargs=kwargs)
        # result = self.platform_agent.execute_resource(cmd)

        # Check that the method is legitimate
        # if method not in res_iface and method != 'wait':
        #     log.error('Mission Error: Method ' + method + ' not recognized')
        #     raise Exception('Mission Error: Method ' + method + ' not recognized')

        # Check that the command is legitimate
        if command in ResourceAgentEvent.__dict__.keys():
            cmd = AgentCommand(command=getattr(ResourceAgentEvent, command))
            reply = getattr(agent_client, method)(cmd)

        elif command in DriverEvent.__dict__.keys():
            cmd = AgentCommand(command=getattr(DriverEvent, command))
            reply = getattr(agent_client, method)(cmd)

        elif command in RSNPlatformDriverEvent.__dict__.keys():
            cmd = AgentCommand(command=getattr(RSNPlatformDriverEvent, command), kwargs=dict(port_id=parameters))
            reply = getattr(agent_client, method)(cmd)

        elif command in res_pars:
            # Set parameters - check parameter first, then set if necessary
            reply = getattr(agent_client, 'get_resource')(command)
            log.debug('[mm] %s = %s', command, str(reply[command]))

            if parameters and parameters != reply[command]:
                getattr(agent_client, method)({command: parameters})
                reply = getattr(agent_client, 'get_resource')(command)
                log.debug('[mm] %s = %s', command, str(reply[command]))

                if parameters != reply[command]:
                    log.error('[mm] Mission Error: Parameter ' + parameters + ' not set')
                    raise Exception('Mission Error: Parameter ' + parameters + ' not set')

        elif command == 'wait':
            gevent.sleep(parameters * 60)

        else:
            log.error('[mm] Mission Error: Command ' + command + ' not recognized')
            raise Exception('Mission Error: Command ' + command + ' not recognized')

        state = agent_client.get_agent_state()
        log.debug('[mm] Agent State = %s', state)

    def execute_mission_commands(self, mission_cmds):
        """
        Loop through the mission commands sequentially
        @param mission_cmds     mission command dict
        return an error code:
            0 = No error
            1 = Abort mission_thread
            2 = Abort mission
        """

        for cmd in mission_cmds:
            attempt = 0
            instrument_id = cmd['instrument_id']
            if instrument_id not in self.instruments:
                # TODO we can remove this verification or handle it in a
                # different way -- it was added while playing with possible
                # ways to select an instrument identification mechanism.
                log.warn('[mm] instrument_id=%s not present in instruments dict', instrument_id)
                continue
            ia_client = self.instruments[instrument_id]
            error_handling = cmd['error']

            if not error_handling:
                error_handling = self.default_error

            log.debug('[mm] %s', instrument_id)

            while attempt < self.max_attempts:
                if self.mission_aborted:
                    return 2

                attempt += 1
                log.debug('[mm] Mission command = %s, Attempt # %d', cmd['command'], attempt)
                try:
                    self.send_command(ia_client, cmd)
                except:
                    if error_handling == 'skip':
                        log.debug('[mm] Mission command %s skipped on error', cmd['command'])
                        break

                    elif (error_handling == 'abort' or attempt >= self.max_attempts):
                        return 1

                    elif error_handling == 'abortMission':
                        return 2
                else:
                    break

        return 0

    def run_timed_mission(self, mission):
        """
        Run a timed mission
        @param mission      Mission dictionary
        """

        error_code = 0   # False
        loop_count = 0

        self.max_attempts = mission['error_handling']['maxRetries']
        self.default_error = mission['error_handling']['default']

        instrument_ids = mission['instrument_id']
        for instrument_id in instrument_ids:
            if instrument_id not in self.instruments:
                continue
            ia_client = self.instruments[instrument_id]
            self.check_preconditions(ia_client)

        # First execute premission if necessary
        if mission['premission_cmds']:
            error_code = self.execute_mission_commands(mission['premission_cmds'])

        start_time = mission['start_time']
        num_loops = mission['loop']['num_loops']

        start_in = start_time - time.time() if (time.time() < start_time) else 0
        log.debug('[mm] Mission start in ' + str(int(start_in)) + ' seconds')

        # Master loop
        while not error_code:

            # Wait until next start
            while (time.time() < start_time):
                gevent.sleep(1)

            # Execute commands
            error_code = self.execute_mission_commands(mission['mission_cmds'])

            # Commands have been executed - increment loop count
            loop_count += 1
            # Calculate next start time
            if error_code or (loop_count >= num_loops and num_loops != -1):
                # Mission was completed successfully or aborted
                break

            start_time += mission['loop']['loop_duration']
            log.debug("[mm] Next Sequence starts at " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))

        if error_code:
            # Abort mission thread
            if error_code == 1:
                # Raise an exception
                log.error('[mm] Timed mission thread aborted')
                # raise Exception('Timed mission thread aborted')
            # Abort mission
            elif error_code == 2:
                self.abort_mission()
        else:
            # Execute postmission if specified
            if mission['postmission_cmds']:
                error_code = self.execute_mission_commands(mission['postmission_cmds'])
                if error_code == 2:
                    self.abort_mission()
                else:
                    #TODO Publish successful mission, stop event subscribers?
                    pass

    def run_event_driven_mission(self, mission):
        """
        Run an event driven mission
        @param mission      Mission dictionary
        """

        self.max_attempts = mission['error_handling']['maxRetries']
        self.default_error = mission['error_handling']['default']

        error_code = 0   # False
        instrument_ids = mission['instrument_id']
        for instrument_id in instrument_ids:
            if instrument_id not in self.instruments:
                continue
            ia_client = self.instruments[instrument_id]
            self.check_preconditions(ia_client)

        # Execute premission
        if mission['premission_cmds']:
            error_code = self.execute_mission_commands(mission['premission_cmds'])

        # Get the agent client for the device whos event needs monitoring
        parent_id = mission['event']['parentID']
        event_id = mission['event']['eventID']

        if parent_id in self.instruments:
            ia_event_client = self.instruments[parent_id]
        else:
            raise Exception('Parent ID unavailable')

        origin = ia_event_client.resource_id

        # Check that the command is legitimate
        if event_id in DriverEvent.__dict__.keys():
            event_type = 'ResourceAgentCommandEvent'
            event_id = getattr(DriverEvent, event_id)
        elif event_id in MissionEvents.__dict__.keys():
            event_type = 'ResourceAgentResourceStateEvent'
            event_id = getattr(MissionEvents, event_id)
            origin = self.profiler_resource_id

        #-------------------------------------------------------------------------------------
        # Set up the subscriber to catch the mission event
        #-------------------------------------------------------------------------------------
        def callback_for_mission_events(event, *args, **kwargs):

            # Check which type of event is being monitored
            for attr in dir(event):
                # An event was captured. Check that it is the correct event
                if event_id == event[attr]:
                    # Execute the mission
                    log.debug('[mm] Mission Event %s received!', event_id)
                    log.debug('[mm] Event Driven Mission execution commenced')

                    error_code = self.execute_mission_commands(mission['mission_cmds'])

                    if error_code == 1:  # Abort thread
                        self.mission_event_subscribers[mission_event_id].stop()
                        log.error('[mm] Event driven mission thread aborted')
                    elif error_code == 2:
                        self.abort_mission()    # Abort mission

        if error_code:
            if error_code == 2:
                self.abort_mission()
            else:
                log.error('[mm] Event driven mission thread aborted')
        else:
            # Start an event subscriber to catch mission event
            self.mission_event_subscribers.append(EventSubscriber(event_type=event_type,
                                                                  origin=origin,
                                                                  callback=callback_for_mission_events))

            mission_event_id = len(self.mission_event_subscribers) - 1
            self.mission_event_subscribers[mission_event_id].start()

            log.debug('[mm] Event driven mission started. Waiting for %s', event_id)

    def check_preconditions(self, ia_client):
        """
        Mission precondition checks
        """
        self.start_error_event_subscriber(ia_client)
        # self._connect_instrument(ia_client)

    def check_postconditions(self, ia_client):
        """
        Mission postcondition checks
        """
        self.stop_error_event_subscriber(ia_client)

    #-------------------------------------------------------------------------------------
    # Platform commands
    #-------------------------------------------------------------------------------------
    def _connect_instrument(self, ia_client):
        #
        # TODO more realistic settings for the connection
        #
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        instrument_attributes = self.INSTRUMENT_ATTRIBUTES_AND_VALUES

        kwargs = dict(
            port_id = port_id,
            instrument_id = instrument_id,
            attributes = instrument_attributes
        )
        cmd = AgentCommand(command=RSNPlatformDriverEvent.CONNECT_INSTRUMENT, kwargs=kwargs)
        retval = self.platform_agent.execute_resource(cmd)
        log.info("[mm] CONNECT_INSTRUMENT = %s", retval)

        # self.assertIsInstance(result, dict)
        # self.assertIn(port_id, result)
        # self.assertIsInstance(result[port_id], dict)
        # returned_attrs = self._verify_valid_instrument_id(instrument_id, result[port_id])
        # if isinstance(returned_attrs, dict):
        #     for attrName in instrument_attributes:
        #         self.assertIn(attrName, returned_attrs)

    def platform_inactive_state(self):
        """
        Put platform in the INACTIVE state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd, timeout=self.receive_timeout)

    def platform_idle_state(self):
        """
        Put platform in the IDLE state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)

    def platform_command_state(self):
        """
        Put platform in the COMMAND state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)

    def platform_mission_running_state(self):
        """
        Put platform in the MISSION_COMMAND state
        """
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RUN_MISSION, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)

    def platform_go_inactive(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.GO_INACTIVE, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)
        state = self.platform_agent.get_agent_state()
        log.debug('[mm] Agent state = %s', state)

    def platform_reset(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.RESET, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)
        state = self.platform_agent.get_agent_state()
        log.debug('[mm] Agent state = %s', state)

    def shutdown(self):
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.SHUTDOWN, kwargs=kwargs)
        self.platform_agent.execute_agent(cmd)
        state = self.platform_agent.get_agent_state()
        log.debug('[mm] Agent state = %s', state)

    def shutdown_platform(self):
        try:
            self.platform_go_inactive()
            self.platform_reset()
        finally:  # attempt shutdown anyway
            self.shutdown()
            self.stop_error_event_subscriber

    def startup_platform(self):
        """
        Verify platform is up and running in the MISSION_COMMAND state
        # TODO Error handling if attempt maxes out
        """
        from pyon.public import CFG
        self.receive_timeout = CFG.endpoint.receive.timeout

        state = self.platform_agent.get_agent_state()

        if state != PlatformAgentState.COMMAND:
            # Initialize platform
            if state == PlatformAgentState.UNINITIALIZED:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.INACTIVE):
                    attempt += 1
                    self.platform_inactive_state()
                    state = self.platform_agent.get_agent_state()

            # Go active
            if state == PlatformAgentState.INACTIVE:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.IDLE):
                    attempt += 1
                    self.platform_idle_state()
                    state = self.platform_agent.get_agent_state()

            # Run
            if state == PlatformAgentState.IDLE:
                attempt = 0
                while (attempt < self.max_attempts and state != PlatformAgentState.COMMAND):
                    attempt += 1
                    self.platform_command_state()
                    state = self.platform_agent.get_agent_state()

            # # Run Mission
            # if state == PlatformAgentState.COMMAND:
            #     attempt = 0
            #     while (attempt < self.max_attempts and state != PlatformAgentState.MISSION_COMMAND):
            #         attempt += 1
            #         self.platform_mission_running_state()
            #         state = self.platform_agent.get_agent_state()
    #-------------------------------------------------------------------------------------
    # Instrument commands
    #-------------------------------------------------------------------------------------

    def startup_instrument_into_command(self, agent_client):

        state = agent_client.get_agent_state()
        while state != ResourceAgentState.COMMAND:
            # UNINITIALIZED -> INACTIVE -> IDLE ->
            try:
                if state == ResourceAgentState.UNINITIALIZED:
                    log.debug('[mm] startup_instrument_into_command - INITIALIZE')
                    cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
                    retval = agent_client.execute_agent(cmd)
                elif state == ResourceAgentState.INACTIVE:
                    log.debug('[mm] startup_instrument_into_command - GO_ACTIVE')
                    cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
                    retval = agent_client.execute_agent(cmd)
                elif state == ResourceAgentState.IDLE:
                    log.debug('[mm] startup_instrument_into_command - RUN')
                    cmd = AgentCommand(command=ResourceAgentEvent.RUN)
                    retval = agent_client.execute_agent(cmd)
                else:
                    log.debug('[mm] startup_instrument_into_command - RESET')
                    cmd = AgentCommand(command=ResourceAgentEvent.RESET)
                    retval = agent_client.execute_agent(cmd)

                state = agent_client.get_agent_state()
                log.debug('[mm] Agent state = %s', state)
            except:
                return False

        return True

    def shutdown_instrument_into_inactive(self, agent_client):
        """
        Check state, stop streaming if necessary, and get into INACTIVE state
        """

        state = agent_client.get_agent_state()
        while state != ResourceAgentState.INACTIVE:
            # STREAMING -> COMMAND -> IDLE -> INACTIVE
            try:
                                # Get capabilities
                retval = agent_client.get_capabilities()
                agt_cmds, agt_pars, res_cmds, res_iface, res_pars = self.sort_capabilities(retval)

                if state == ResourceAgentState.UNINITIALIZED:
                    log.debug('[mm] startup_instrument_into_command - INITIALIZE')
                    cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
                    retval = agent_client.execute_agent(cmd)

                elif ResourceAgentEvent.GO_INACTIVE in agt_cmds:
                    log.debug('[mm] startup_instrument_into_command - GO_INACTIVE')
                    cmd = AgentCommand(command=ResourceAgentEvent.GO_INACTIVE)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.IDLE:
                    log.debug('[mm] startup_instrument_into_command - GO_INACTIVE')
                    cmd = AgentCommand(command=ResourceAgentEvent.GO_INACTIVE)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.COMMAND:
                    log.debug('[mm] startup_instrument_into_command - CLEAR')
                    cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.STREAMING:
                    log.debug('[mm] startup_instrument_into_command - STOP_AUTOSAMPLE')
                    cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
                    retval = agent_client.execute_resource(cmd)

                else:
                    # TODO: What about Calibrate?
                    log.debug('[mm] startup_instrument_into_command - RESET')
                    cmd = AgentCommand(command=ResourceAgentEvent.RESET)
                    retval = agent_client.execute_agent(cmd)

                state = agent_client.get_agent_state()
                log.debug('[mm] shutdown_instrument_into_inactive - Agent state = %s', state)
            except:
                return False

        return True

    def instrument_abort_sequence(self, agent_client):
        """
        Check state, and gracefully get into INACTIVE state
        """

        state = agent_client.get_agent_state()
        if state != ResourceAgentState.INACTIVE:
            self.shutdown_instrument_into_inactive(agent_client)

    #-------------------------------------------------------------------------------------
    # Error handling
    #-------------------------------------------------------------------------------------
    def check_error(self, error_event):
        """
        Error handling
        """
        # print 'Checking error...'
        if error_event['error_code'] == 409:
            # Instrument State exception - do something
            for k, v in self.instruments.items():
                if error_event['origin'] == v.resource_id:
                    ia_client = v
            # print error_event['error_code']

    #------------------------------------------------------------------------------
    # Event helpers. Taken from ion/agents/instrument/test/test_instrument_agent.py
    #------------------------------------------------------------------------------
    def start_error_event_subscriber(self, _ia_client):
        """
        Start a subscriber to the instrument agent error events.
        @_ia_client Instrument agent client to subsribe to
        """

        def get_error_event(*args, **kwargs):
            log.info('[mm] Mission recieved ION event: args=%s, kwargs=%s, event=%s.',
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

    def sort_capabilities(self, caps_list):
        agt_cmds = []
        agt_pars = []
        res_cmds = []
        res_iface = []
        res_pars = []

        if len(caps_list) > 0 and isinstance(caps_list[0], AgentCapability):
            agt_cmds = [x.name for x in caps_list if x.cap_type == CapabilityType.AGT_CMD]
            agt_pars = [x.name for x in caps_list if x.cap_type == CapabilityType.AGT_PAR]
            res_cmds = [x.name for x in caps_list if x.cap_type == CapabilityType.RES_CMD]
            res_iface = [x.name for x in caps_list if x.cap_type == CapabilityType.RES_IFACE]
            res_pars = [x.name for x in caps_list if x.cap_type == CapabilityType.RES_PAR]

        elif len(caps_list) > 0 and isinstance(caps_list[0], dict):
            agt_cmds = [x['name'] for x in caps_list if x['cap_type'] == CapabilityType.AGT_CMD]
            agt_pars = [x['name'] for x in caps_list if x['cap_type'] == CapabilityType.AGT_PAR]
            res_cmds = [x['name'] for x in caps_list if x['cap_type'] == CapabilityType.RES_CMD]
            res_iface = [x['name'] for x in caps_list if x['cap_type'] == CapabilityType.RES_IFACE]
            res_pars = [x['name'] for x in caps_list if x['cap_type'] == CapabilityType.RES_PAR]

        return agt_cmds, agt_pars, res_cmds, res_iface, res_pars

if __name__ == "__main__":  # pragma: no cover
    """
    Stand alone to check the mission loading/parsing capabilities
    """
    filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"

    mission = MissionLoader()
    mission.load_mission_file(filename)
