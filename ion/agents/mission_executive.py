"""
@package ion.agents.mission_executive
@file    ion/agents/mission_executive.py
@author  Bob Fratantonio
@brief   A class for the platform mission executive
"""

import calendar
import gevent
import yaml
import time
from time import gmtime
import pytz
from datetime import datetime

from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent
from pyon.agent.common import BaseEnum
from pyon.event.event import EventSubscriber
from pyon.public import log
from pyon.util.breakpoint import breakpoint
from pyon.util.config import Config

from ion.core.includes.mi import DriverEvent

from interface.objects import AgentCommand
from interface.objects import AgentCapability
from interface.objects import CapabilityType
from interface.objects import MissionExecutionStatus


class MissionErrorCode(BaseEnum):
    """
    Mission executive error code
            0 = No error
            1 = Abort mission_thread
            2 = Abort mission
    """

    NO_ERROR = 0
    ABORT_MISSION_THREAD = 1
    ABORT_MISSION = 2


class MissionThreadStatus(BaseEnum):
    """
    Mission thread status
            starting = mission thread is starting
            running = mission sequence is executing
            stopped = mission sequence is waiting for next sequence
            done = mission sequence has been terminated normally
            aborted = mission sequence was aborted
    """
    STARTING = 'starting'
    RUNNING = 'running'
    STOPPED = 'stopped'
    DONE = 'done'
    ABORTED = 'aborted'


class MissionLoader(object):
    """
    MissionLoader class is used to parse a mission file, check the mission logic
    and save the mission as a dict
    """
    def __init__(self, platform_agent):

        self.platform_agent = platform_agent
        self.mission_entries = []
        self.mission_id = None
        self.accepted_error_values = ['abort', 'abortMission', 'retry', 'skip']

    def add_entry(self, instrument_id=[], error_handling = {}, start_time=0, loop={}, event = {},
                  premission_cmds=[], mission_cmds=[], postmission_cmds=[]):

        self.mission_entries.append({"mission_id": self.mission_id,
                                    "instrument_id": instrument_id,
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

    def convert_to_utc_from_timezone(self, seconds_from_epoch, tz):
        """
        Convert user defined time zone to UTC
        @param seconds_from_epoch   Start time in seconds from epoch in timezone
        @param tz                   Timezone from pytz.all_timezones
        return                      Time in UTC seconds from epoch
        """
        if tz in pytz.all_timezones:
            user_timezone = pytz.timezone(tz)
        else:
            raise Exception('Time Zone not recognized')
        # Get the time string into the user defined time zone
        local_dt = user_timezone.localize(datetime.utcfromtimestamp(seconds_from_epoch))
        # Convert to UTC datetime
        utc_dt = local_dt.astimezone(pytz.utc)
        # Return as seconds from the epoch
        return calendar.timegm(utc_dt.timetuple())

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
        tz = schedule['timeZone']
        if not start_time_string or start_time_string.lower() == 'none':
            start_time = None
        else:
            try:
                start_time = calendar.timegm(time.strptime(start_time_string, '%m/%d/%Y %H:%M:%S'))
            except ValueError:
                self.publish_mission_loader_error_event('MissionLoader: validate_schedule: startTime format error')
                # log.error("MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))
                log.error("[mm] MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))
                raise Exception('MissionLoader: validate_schedule: startTime format error')
            else:
                if tz:
                    start_time = self.convert_to_utc_from_timezone(start_time, tz)

                current_time = time.time()
                # Compare mission start time to current time
                if (current_time > start_time):
                    if loop_duration > 0:
                        nloops = int((current_time-start_time)/loop_duration)+1
                        start_time += nloops*loop_duration
                    else:
                        log.debug("[mm] MissionLoader: validate_schedule: Start time has already elapsed")
                        # raise

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
                            self.publish_mission_loader_error_event('Mission Error: Scheduling conflict')
                            log.error('[mm] Mission Error: Scheduling conflict: ' + str(sublist1) + str(sublist2))
                            raise Exception('Mission Error: Scheduling conflict')

        return True

    def verify_command_and_params(self, cmd='', params={}):
        """
        Verify that specified command is defined.
        """
        pass
        # if cmd not in MissionCommands.all_cmds:
        #     raise Exception('Mission Error: %s Mission command not recognized' % cmd)
        # for param in params:
        #     if param not in MissionCommands.all_cmds[cmd]:
        #         raise Exception('Mission Error: %s Mission parameter not recognized' % param)

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
                if instrument not in instrument_id:
                    log.warn('[mm] instrument_id not recognized from instrumentID list: %s', instrument)
            else:
                #First quick check for a wait command
                cmd_method, rest = command.strip().split('(')
                if cmd_method.lower() == 'wait':
                    instrument = None
                elif len(instrument_id) == 1:
                    instrument = instrument_id[0]
                else:
                    log.error('[mm] instrument_id not given in command: %s', command)
                    raise Exception('Error in mission command string: instrument_id not specified in command: %s', command)

            # self.verify_command_and_params(command, params)
            if '(' in command and ')' in command:
                cmd_method, rest = command.strip().split('(')
                if '{' in rest and '}' in rest:
                    cmd, rest = rest.split('{')
                    param = rest.split('}')[0]
                    # Leave as string, doesn't have to be numeric
                    #param = float(param) if '.' in param else int(param)
                else:
                    cmd = rest.split(')')[0]
                    param = None
            else:
                self.publish_mission_loader_error_event('Error in mission command string')
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
            self.publish_mission_loader_error_event('Mission event not specified')
            log.error('[mm] Mission event not specified')
            raise Exception('Mission event not specified')
        elif not parent_id:
            self.publish_mission_loader_error_event('Mission event parentID not specified')
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

            if 'preMissionSequence' in current_mission:
                premission_sequence  = current_mission['preMissionSequence']
            else:
                premission_sequence = None

            mission_sequence = current_mission['missionSequence']

            if 'postMissionSequence' in current_mission:
                postmission_sequence = current_mission['postMissionSequence']
            else:
                postmission_sequence = None

            error_parameters = current_mission['errorHandling']

            premission_params, _ = self.parse_mission_sequence(premission_sequence, instrument_id)
            mission_params, mission_duration = self.parse_mission_sequence(mission_sequence, instrument_id)
            postmission_params, _ = self.parse_mission_sequence(postmission_sequence, instrument_id)
            loop_params = self.parse_loop_parameters(schedule)

            loop_duration = loop_params['loop_duration']
            if (loop_duration and loop_duration < mission_duration):
                self.publish_mission_loader_error_event('Mission File Error: Mission duration > scheduled loop duration')
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

    def publish_mission_loader_error_event(self, description):
        evt = dict(event_type='DeviceMissionEvent',
                   description=description,
                   mission_id=self.mission_id,
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id)

        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

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

    def load_mission(self, mission_id, mission_yml):
        """
        Load, parse, and check the mission file contents
        @param mission_id        Mission id from RR
        @param mission_yml       Mission file contents as string
        """
        self.mission_id = mission_id

        log.debug('[mm] Parsing mission_id %s', self.mission_id)

        self.mission_id = mission_id
        mission_dict = yaml.safe_load(mission_yml)
        self.raw_mission = mission_dict['mission']

        return self.validate_schedule(self.raw_mission)


class MissionScheduler(object):
    """
    MissionScheduler takes care of the command/control and associated timing for a
    platform mission
    """

    def __init__(self, platform_agent, instruments, mission):

        import pprint
        self.pformat = pprint.PrettyPrinter().pformat
        log.debug('[mm] MissionScheduler: instruments=%s\nmission=%s',
                  self.pformat(instruments), self.pformat(mission))

        self.platform_agent = platform_agent
        self.instruments = instruments

        self.mission = mission
        self.mission_id = mission[0]['mission_id']

        # Define max number of agent command retries
        self.max_attempts = mission[0]['error_handling']['maxRetries']
        self.default_error = mission[0]['error_handling']['default']

        # Initialize error events
        self.error_events_received = []

        # Initialize list of mission event subscribers
        self.mission_event_subscribers = []

        # Initialize list of mission threads
        self.threads = []

        # Initialize mission thread ID
        self.mission_thread_id = 0

        # Boolean to hold global mission abort status
        self.mission_aborted = False

        # Boolean to hold global mission running status
        self.mission_running = True

        # Keep track of all running mission threads
        self.mission_threads = []

    def run_mission(self):
        """
        Starts execution of the given mission file.
        """

        log.debug('[mm] run_mission: mission=%s', self.mission)

        # Start up the platform
        # self.startup_platform()

        self._schedule(self.mission)

    def abort_mission(self):
        """
        Terminates the ongoing mission execution by immediately stopping the
        main mission sequence and then running instrument abort sequence
        """

        # Only need the abort sequence once...
        if not self.mission_aborted:
            log.debug('[mm] abort_mission method called...')
            self._publish_mission_abort_event()

            # Setting this global to true will tell all running mission threads
            # to stop ASAP
            self.mission_aborted = True
            self.mission_running = False

            # Wait for all threads to finish current command...
            for thread in self.mission_threads:
                if thread['status'] == MissionThreadStatus.RUNNING:
                    # Wait for status change
                    thread_id = thread['mission_thread_id']
                    status = self.mission_threads[thread_id]['status']
                    while status == MissionThreadStatus.RUNNING:
                        gevent.sleep(1)
                        status = self.mission_threads[thread_id]['status']

            # For event driven missions, stop event subscribers
            self._stop_mission_event_subscribers()

            # Start abort sequence
            for instrument, client in self.instruments.iteritems():
                self._instrument_abort_sequence(client)

            log.error('[mm] Mission Aborted')

            self._publish_mission_aborted_event()

    def _abort_mission_thread(self, instruments):
        """
        Terminates the missionThread execution and puts instruments in INACTIVE State
        """

        log.debug('[mm] _abort_mission_thread called...')
        # Start abort sequence
        for instrument_id in instruments:
            if instrument_id not in self.instruments:
                continue
            ia_client = self.instruments[instrument_id]
            self._instrument_abort_sequence(ia_client)

        log.error('[mm] Mission thread aborted')

    def _check_mission_running(self):
        """
        This method checks all mission threads and if complete, stop any event subscribers
        and publish event
        """
        mission_complete = [MissionThreadStatus.ABORTED, MissionThreadStatus.DONE]
        for thread in self.mission_threads:
            if thread['status'] not in mission_complete:
                log.debug('[mm] _check_mission_running - Mission threads are still running')
                return True

        self.mission_running = False
        return False

    def _stop_mission_event_subscribers(self):
        """
        This method will stop all mission event subscribers when a mission has terminated
        """
        # For event driven missions, stop event subscribers
        for subscriber in self.mission_event_subscribers:
            subscriber.stop()
        self.mission_event_subscribers = []

    def _schedule(self, missions):
        """
        Set up gevent threads for each mission
        """

        self._publish_mission_start_event()

        for mission in missions:
            start_time = mission['start_time']

            # There are two types of mission schedules: timed and event
            if start_time:
                # Timed schedule
                self.threads.append(gevent.spawn(self._run_timed_mission, mission))
            else:
                # Event driven scheduler
                self.threads.append(gevent.spawn(self._run_event_driven_mission, mission))

        self._publish_mission_started_event()

        log.debug('[mm] schedule: waiting for mission to complete')
        gevent.joinall(self.threads)

    def _send_command(self, instrument_id, agent_client, cmd):
        """
        Send agent command
        @param instrument_id        for logging
        @param agent_client         Instrument/platform agent client
        @param cmd                  Mission command
        """

        method = cmd['method']
        command = cmd['command']
        parameters = cmd['parameters']

        # Three types of commands: wait, platform cmd, and instrument cmd
        if command == 'wait':
            log.debug('[mm] Send mission command = %s', command)
            wait_duration = parameters * 60
            now = time.time()
            wait_end = now + wait_duration
            while (time.time() < wait_end and not self.mission_aborted):
                gevent.sleep(1)

        elif agent_client is None:
            # This indicates platform agent command
            log.debug('[mm] Send mission command = %s - %s to platform agent %r',
                      method, command, instrument_id)
            driver_event_class = self.platform_agent._plat_driver.get_platform_driver_event_class()
            if command in driver_event_class.__dict__.keys():
                kwargs = {}
                if parameters:
                    # This must be a TURN_ON_PORT or TURN_OFF_PORT command
                    kwargs = dict(port_id=parameters)

                cmd = AgentCommand(command=getattr(driver_event_class, command), kwargs=kwargs)
                reply = getattr(self.platform_agent, method)(command=cmd)

            else:
                log.error('[mm] Mission Error: Command %s not recognized', command)
                raise Exception('Mission Error: Command %s not recognized', command)

        else:
            # This indicates instrument agent command
            log.debug('[mm] Send mission command = %s - %s to instrument agent resource_id=%r, instrument_id=%r',
                      method, command, agent_client.resource_id, instrument_id)
            retval = agent_client.get_capabilities()
            agt_cmds, agt_pars, res_cmds, res_iface, res_pars = self._sort_capabilities(retval)

            if command in ResourceAgentEvent.__dict__.keys():
                cmd = AgentCommand(command=getattr(ResourceAgentEvent, command))
                reply = getattr(agent_client, method)(cmd)

            elif command in DriverEvent.__dict__.keys():
                cmd = AgentCommand(command=getattr(DriverEvent, command))
                reply = getattr(agent_client, method)(cmd)

            elif command in res_pars:
                # Set parameters - check parameter first, then set if necessary
                reply = getattr(agent_client, 'get_resource')(command)
                log.debug('[mm] %s = %s', command, str(reply[command]))

                if parameters and parameters != reply[command]:
                    parameters = float(parameters) if '.' in parameters else int(parameters)
                    getattr(agent_client, method)({command: parameters})
                    reply = getattr(agent_client, 'get_resource')(command)
                    log.debug('[mm] %s = %s', command, str(reply[command]))

                    if parameters != reply[command]:
                        msg = '[mm] Mission Error: Parameter %s not set' % parameters
                        log.error(msg)
                        raise Exception(msg)

            else:
                log.error('[mm] Mission Error: Command %s not recognized', command)
                raise Exception('Mission Error: Command %s not recognized', command)

            state = agent_client.get_agent_state()
            log.debug('[mm] Agent State = %s', state)

    def _execute_mission_commands(self, mission_cmds):
        """
        Loop through the mission commands sequentially
        @param mission_cmds     mission command dict
        return an error dict containing an error code from MissionErrorCode and error message
        """

        for cmd in mission_cmds:
            attempt = 0

            instrument_id = cmd['instrument_id']
            if instrument_id in self.instruments:
                # This command is for a child instrument
                ia_client = self.instruments[instrument_id]
            elif instrument_id == self.platform_agent._platform_id or instrument_id is None:
                # This command is for the parent platform agent or a 'wait'
                ia_client = None
            else:
                log.warn('[mm] instrument_id=%r unrecognized', instrument_id)
                continue

            error_handling = cmd['error']

            if not error_handling:
                error_handling = self.default_error

            log.debug('[mm] instrument_id=%r', instrument_id)

            while attempt < self.max_attempts:
                error_string = ''
                if self.mission_aborted:
                    return dict(code=MissionErrorCode.ABORT_MISSION, message=error_string)

                attempt += 1
                log.debug('[mm] Mission command = %s, Attempt # %d', cmd['command'], attempt)
                try:
                    self._send_command(instrument_id, ia_client, cmd)
                except Exception, ex:
                    # Get a description of the error
                    error_string = str(ex) + ' Mission sequence command = ' + str(cmd)

                    if error_handling == 'skip':
                        log.warn('[mm] Mission command %s skipped on error', cmd['command'])
                        break

                    elif (error_handling == 'abort' or attempt >= self.max_attempts):
                        return dict(code=MissionErrorCode.ABORT_MISSION_THREAD, message=error_string)

                    elif error_handling == 'abortMission':
                        return dict(code=MissionErrorCode.ABORT_MISSION, message=error_string)

                    elif error_handling == 'retry':
                        # Wait 5 seconds before retrying
                        gevent.sleep(5)

                else:
                    break

        return dict(code=MissionErrorCode.NO_ERROR, message='')

    def _run_timed_mission(self, mission):
        """
        Run a timed mission
        @param mission      Mission dictionary
        """

        current_mission_thread = self.mission_thread_id
        self.mission_thread_id += 1

        # Simple status update
        self.mission_threads.append({"mission_thread_id": current_mission_thread,
                                     "status": MissionThreadStatus.STARTING,
                                     "type": 'timed'})

        # Publish event mission thread started
        self._publish_mission_thread_started_event(current_mission_thread)

        error_code = MissionErrorCode.NO_ERROR
        loop_count = 0

        self.max_attempts = mission['error_handling']['maxRetries']
        self.default_error = mission['error_handling']['default']

        instrument_ids = mission['instrument_id']

        # First execute premission if necessary
        if mission['premission_cmds']:
            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.RUNNING
            # Execute commands
            error = self._execute_mission_commands(mission['premission_cmds'])
            error_code = error['code']
            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.STOPPED

        start_time = mission['start_time']
        num_loops = mission['loop']['num_loops']

        start_in = start_time - time.time() if (time.time() < start_time) else 0
        log.debug('[mm] Mission start in ' + str(int(start_in)) + ' seconds')

        # Master loop
        while not error_code:

            # Wait until next start (or abort)
            while (time.time() < start_time) and not self.mission_aborted:
                gevent.sleep(1)

            # Publish event - missionSequence started
            self._publish_mission_sequence_starting_event(current_mission_thread, "MissionSequence starting...")

            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.RUNNING
            # Execute commands
            error = self._execute_mission_commands(mission['mission_cmds'])
            error_code = error['code']
            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.STOPPED

            # Commands have been executed - increment loop count
            loop_count += 1

            if error_code or (loop_count >= num_loops and num_loops != -1):
                # Mission was completed successfully or aborted
                break

            # Calculate next start time
            start_time += mission['loop']['loop_duration']
            log.debug("[mm] Next sequence starts at " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time)))
            event_description = ("MissionSequence loop {0} of {1} complete."
                                 " Next start at {2}".format
                                 (loop_count, num_loops, time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))))
            self._publish_mission_sequence_complete_event(current_mission_thread, event_description)

        if error_code:
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
            self._publish_mission_thread_failed_event(current_mission_thread, error['message'])

            if error_code == MissionErrorCode.ABORT_MISSION_THREAD:
                self._abort_mission_thread(instrument_ids)
                if not self._check_mission_running():
                    self._stop_mission_event_subscribers()
                    self._publish_mission_complete_event()

            elif error_code == MissionErrorCode.ABORT_MISSION:
                self.abort_mission()
        else:
            # Execute postmission if specified
            if mission['postmission_cmds']:
                # Update internal mission status
                self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.RUNNING
                # Execute commands
                error = self._execute_mission_commands(mission['postmission_cmds'])
                error_code = error['code']

                if error_code == MissionErrorCode.ABORT_MISSION_THREAD:
                    self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
                    self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                    self._abort_mission_thread(instrument_ids)
                    if not self._check_mission_running():
                        self._stop_mission_event_subscribers()
                        self._publish_mission_complete_event()

                elif error_code == MissionErrorCode.ABORT_MISSION:
                    self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
                    self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                    self.abort_mission()

                else:
                    self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.DONE
                    self._publish_mission_thread_complete_event(current_mission_thread)
                    if not self._check_mission_running():
                        self._stop_mission_event_subscribers()
                        self._publish_mission_complete_event()
            else:
                self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.DONE
                self._publish_mission_thread_complete_event(current_mission_thread)
                if not self._check_mission_running():
                    self._stop_mission_event_subscribers()
                    self._publish_mission_complete_event()

    def _run_event_driven_mission(self, mission):
        """
        Run an event driven mission
        @param mission      Mission dictionary
        """

        current_mission_thread = self.mission_thread_id
        self.mission_thread_id += 1

        # Simple status update
        self.mission_threads.append({"mission_thread_id": current_mission_thread,
                                     "status": MissionThreadStatus.STARTING,
                                     "type": 'event'})

        # Publish event mission thread started
        self._publish_mission_thread_started_event(current_mission_thread)

        self.max_attempts = mission['error_handling']['maxRetries']
        self.default_error = mission['error_handling']['default']
        error_code = MissionErrorCode.NO_ERROR

        instrument_ids = mission['instrument_id']

        # Execute premission
        if mission['premission_cmds']:
            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.RUNNING
            # Execute commands
            error = self._execute_mission_commands(mission['premission_cmds'])
            error_code = error['code']
            # Update internal mission status
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.DONE

        # Get the agent client for the device whos event needs monitoring
        parent_id = mission['event']['parentID']
        event_id = mission['event']['eventID']

        if parent_id in self.instruments:
            ia_event_client = self.instruments[parent_id]
            origin = ia_event_client.resource_id
        elif parent_id == self.platform_agent._platform_id:
            origin = self.platform_agent._platform_id
        else:
            self._publish_mission_thread_failed_event(current_mission_thread, 'Parent ID unrecognized - {0}'.format(parent_id))
            raise Exception('Parent ID unrecognized - {0}'.format(parent_id))

        # Check that the event id is legitimate
        driver_event_class = self.platform_agent._plat_driver.get_platform_driver_event_class()
        if event_id in DriverEvent.__dict__.keys():
            event_type = 'ResourceAgentCommandEvent'
            event_id = getattr(DriverEvent, event_id)
        elif event_id in driver_event_class.__dict__.keys():
            event_type = ''
            event_id = getattr(driver_event_class, event_id)
        else:
            # EXTERNAL event - check OMSDeviceStatusEvent
            event_type = 'OMSDeviceStatusEvent'
            #self._publish_mission_thread_failed_event(current_mission_thread, 'Event ID unrecognized - {0}'.format(event_id))
            #raise Exception('Event ID unrecognized - %s', event_id)

        #-------------------------------------------------------------------------------------
        # Set up the subscriber to catch the mission event
        #-------------------------------------------------------------------------------------
        def callback_for_mission_events(event, *args, **kwargs):
            #Check which type of event is being monitored
            for attr in dir(event):
                # An event was captured. Check that it is the correct event
                try:
                    event_attr = event[attr]
                except KeyError:
                    continue
                else:
                    if event_id == event_attr:
                        # Execute the mission
                        log.debug('[mm] Mission Event %s received!', event_id)
                        log.debug('[mm] Event Driven Mission execution commenced')
                        # Publish event - missionSequence started
                        event_description = "Event {0} received! MissionSequence starting...".format(event_id)
                        self._publish_mission_sequence_starting_event(current_mission_thread, event_description)

                        # Update internal mission status
                        self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.RUNNING
                        # Execute commands
                        error = self._execute_mission_commands(mission['mission_cmds'])
                        error_code = error['code']
                        # Update internal mission status
                        self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.DONE

                        if error_code == MissionErrorCode.ABORT_MISSION_THREAD:
                            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
                            self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                            self.mission_event_subscribers[mission_event_id].stop()
                            self._abort_mission_thread(instrument_ids)
                            if not self._check_mission_running():
                                self._publish_mission_complete_event()
                                self._stop_mission_event_subscribers()

                        elif error_code == MissionErrorCode.ABORT_MISSION:
                            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
                            self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                            self.abort_mission()

                        elif not self._check_mission_running():
                            self._publish_mission_thread_complete_event(current_mission_thread)
                            self._publish_mission_complete_event()
                            self._stop_mission_event_subscribers()
                        else:
                            event_description = "MissionSequence complete. Waiting for {0}".format(event_id)
                            self._publish_mission_sequence_complete_event(current_mission_thread, event_description)

        if error_code:
            self.mission_threads[current_mission_thread]['status'] = MissionThreadStatus.ABORTED
            if error_code == MissionErrorCode.ABORT_MISSION_THREAD:
                self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                self._abort_mission_thread(instrument_ids)
                if not self._check_mission_running():
                    self._stop_mission_event_subscribers()
                    self._publish_mission_complete_event()

            elif error_code == MissionErrorCode.ABORT_MISSION:
                self._publish_mission_thread_failed_event(current_mission_thread, error['message'])
                self.abort_mission()
        else:
            # Start an event subscriber to catch mission event
            self.mission_event_subscribers.append(EventSubscriber(event_type=event_type,
                                                                  origin=origin,
                                                                  callback=callback_for_mission_events))

            mission_event_id = len(self.mission_event_subscribers) - 1
            self.mission_event_subscribers[mission_event_id].start()

            log.debug('[mm] Event driven mission started. Waiting for %s', event_id)

            while self.mission_running:
                gevent.sleep(1)

    #-------------------------------------------------------------------------------------
    # Instrument state commands
    #-------------------------------------------------------------------------------------

    def _startup_instrument_into_command(self, agent_client):

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

    def _shutdown_instrument_into_inactive(self, agent_client):
        """
        Check state, stop streaming if necessary, and get into INACTIVE state
        """

        state = agent_client.get_agent_state()
        while state != ResourceAgentState.INACTIVE:
            # STREAMING -> COMMAND -> IDLE -> INACTIVE
            try:
                                # Get capabilities
                retval = agent_client.get_capabilities()
                agt_cmds, agt_pars, res_cmds, res_iface, res_pars = self._sort_capabilities(retval)

                if state == ResourceAgentState.UNINITIALIZED:
                    log.debug('[mm] shutdown_instrument_into_inactive - INITIALIZE')
                    cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
                    retval = agent_client.execute_agent(cmd)

                elif ResourceAgentEvent.GO_INACTIVE in agt_cmds:
                    log.debug('[mm] shutdown_instrument_into_inactive - GO_INACTIVE')
                    cmd = AgentCommand(command=ResourceAgentEvent.GO_INACTIVE)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.IDLE:
                    log.debug('[mm] shutdown_instrument_into_inactive - GO_INACTIVE')
                    cmd = AgentCommand(command=ResourceAgentEvent.GO_INACTIVE)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.COMMAND:
                    log.debug('[mm] shutdown_instrument_into_inactive - CLEAR')
                    cmd = AgentCommand(command=ResourceAgentEvent.CLEAR)
                    retval = agent_client.execute_agent(cmd)

                elif state == ResourceAgentState.STREAMING:
                    log.debug('[mm] shutdown_instrument_into_inactive - STOP_AUTOSAMPLE')
                    cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
                    retval = agent_client.execute_resource(cmd)

                else:
                    # TODO: What about Calibrate?
                    log.debug('[mm] shutdown_instrument_into_inactive - RESET')
                    cmd = AgentCommand(command=ResourceAgentEvent.RESET)
                    retval = agent_client.execute_agent(cmd)

                state = agent_client.get_agent_state()
                log.debug('[mm] shutdown_instrument_into_inactive - Agent state = %s', state)
            except:
                return False

        return True

    def _instrument_abort_sequence(self, agent_client):
        """
        Check state, and gracefully get into INACTIVE state
        """

        state = agent_client.get_agent_state()
        if state != ResourceAgentState.INACTIVE:
            self._shutdown_instrument_into_inactive(agent_client)

    def _sort_capabilities(self, caps_list):
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

    #----------------------------------------------------------------------------------------------------
    #   Publish mission executive events
    #   Refer to https://confluence.oceanobservatories.org/display/CIDev/Platform+Agent+Mission+Executive
    #----------------------------------------------------------------------------------------------------

    # Mission is about to start execution
    def _publish_mission_start_event(self):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   sub_type="STARTING",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description='Mission {0} is about to start execution'.format(self.mission_id))
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission has started
    def _publish_mission_started_event(self):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   sub_type="STARTED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description='Mission {0} has started'.format(self.mission_id))
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission has been requested to be aborted
    def _publish_mission_abort_event(self):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   sub_type="STOPPING",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.ABORTED,
                   description='Mission {0} abort sequence has started'.format(self.mission_id))
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission has been aborted
    def _publish_mission_aborted_event(self):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   sub_type="STOPPED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.ABORTED)
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission has completed, determine if it exited normally or due to exception
    def _publish_mission_complete_event(self):
        """
        This method can be called by a mission thread with no knowledge of
        the status of other threads. So a check must be made to see if any
        threads failed during mission execution!
        """
        mission_threads_aborted = 0
        mission_threads_successful = 0

        for thread in self.mission_threads:
            if thread['status'] == MissionThreadStatus.ABORTED:
                mission_threads_aborted += 1
            else:
                mission_threads_successful += 1

        if mission_threads_aborted > 0:
            execution_status = MissionExecutionStatus.FAILED
            description = ('Mission {0} has failed.'
                           ' {1} mission threads completed successfully,'
                           ' {2} mission threads aborted'
                           .format(self.mission_id, mission_threads_successful, mission_threads_aborted))
        else:
            execution_status = MissionExecutionStatus.OK
            description = 'Mission {0} has exited normally'.format(self.mission_id)

        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   sub_type="STOPPED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=execution_status,
                   description=description)
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission thread has started
    def _publish_mission_thread_started_event(self, mission_thread_id):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   mission_thread_id=str(mission_thread_id),
                   sub_type="STARTED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description='Mission thread {0} has started'.format(mission_thread_id))
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission thread has exited normally
    def _publish_mission_thread_complete_event(self, mission_thread_id):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   mission_thread_id=str(mission_thread_id),
                   sub_type="STOPPED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description='Mission thread {0} has exited normally'.format(mission_thread_id))
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission thread has started a missionSequence
    def _publish_mission_sequence_starting_event(self, mission_thread_id, description):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   mission_thread_id=str(mission_thread_id),
                   sub_type="STARTED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description=description)
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission thread has completed a missionSequence
    def _publish_mission_sequence_complete_event(self, mission_thread_id, description):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   mission_thread_id=str(mission_thread_id),
                   sub_type="STOPPED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.OK,
                   description=description)
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

    # Mission thread has exited due to some exception
    def _publish_mission_thread_failed_event(self, mission_thread_id, description=''):
        evt = dict(event_type='MissionLifecycleEvent',
                   mission_id=self.mission_id,
                   mission_thread_id=str(mission_thread_id),
                   sub_type="STOPPED",
                   origin_type=self.platform_agent.ORIGIN_TYPE,
                   origin=self.platform_agent.resource_id,
                   execution_status=MissionExecutionStatus.FAILED,
                   description=description)
        self.platform_agent._event_publisher.publish_event(**evt)
        log.debug('[mm] event published: %s', evt)

if __name__ == "__main__":  # pragma: no cover
    """
    Stand alone to check the mission loading/parsing capabilities
    """
    p_agent = []
    mission_id = 0
    mission = MissionLoader(p_agent)
    filename = "ion/agents/platform/test/mission_RSN_simulator1.yml"

    with open(filename, 'r') as f:
        mission_string = f.read()

    mission.load_mission(mission_id, mission_string)
