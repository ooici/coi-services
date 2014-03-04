"""
@package ion.agents.mission_executive
@file    ion/agents/mission_executive.py
@author  Bob Fratantonio
@brief   A class for the platform mission executive
"""

__author__ = 'Bob Fratantonio'
__license__ = 'Apache 2.0'

from pyon.public import log
import logging

import yaml
import calendar
import time

from time import gmtime, strftime 
from datetime import datetime
from pyon.agent.common import BaseEnum
from pyon.util.breakpoint import breakpoint

import gevent
from gevent import Greenlet
from interface.objects import AgentCommand
from ion.agents.platform.platform_agent import PlatformAgentEvent
from pyon.agent.agent import ResourceAgentClient, ResourceAgentState

from ion.agents.platform.test.base_test_platform_agent_with_rsn import FakeProcess


class MissionEvents(BaseEnum):
    """
    Acceptable mission events.
    TODO: Define all possible events that a mission can respond to
    """
    PROFILER_AT_CEILING = 'ShallowProfilerAtCeiling'
    PROFILER_STEP = 'ShallowProfilerStep'

class MissionLoader(object):  
    """
    MissionLoader class is used to parse a mission file, check the mission logic
    and save the mission as a dict
    """

    mission_entries = []
    
    def _add_entry(self, instrument_id = '', start_time = 0, duration = 0, 
                        num_loops = 0, loop_duration = 0, mission_cmds = []):
        
        self.mission_entries.append({"instrument_id": instrument_id,
                                    "start_time": start_time, 
                                    "duration": duration,
                                    "num_loops": num_loops,
                                    "loop_duration": loop_duration,
                                    "mission_cmds": mission_cmds})

    def _count_entries(self):
        return len(self.mission_entries)

    def _sort_entries(self):
        self.mission_entries = sorted(self.mission_entries,  key=lambda k: k["start_time"]) 

    def _get_entry_all(self, id_):
        return self.mission_entries[id_]

    def _print_entry_all(self):
        print self.mission_entries

    def _delete_entry(self, id_):
        self.mission_entries.pop(id_)

    def _calculate_next_interval(self, id_):
        current_time = time.time()
        start_time = self.mission_entries[id_]["start_time"]
        loop_duration = self.mission_entries[id_]["loop_duration"]

        if start_time < current_time:
            next_interval = start_time
            while next_interval < current_time:
                next_interval += loop_duration
            print "Current time is: " +  time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            print "Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time + loop_duration))
            return next_interval - current_time
        else:
            print "Current time is: " +  time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))
            print "Next start at: " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
            return (start_time - current_time) + loop_duration

    def _check_intersections(self, indices):
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
        
        if not start_times:
            print 'Start times are OK!'
        else: 
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
                mission_all_times.append(zip(start,end))
                
                print 'Mission ' + str(n) + ' Times'
                for n,y in mission_all_times[n]:
                    print time.strftime("%Y-%m-%d %H:%M:%S", gmtime(n)) + ' - ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(y))

            # This only compares adjacent missions (only works for 2 missions)
            # TODO: Update logic to work for multiple instrument missions
            for n in range(1,len(start_times)):
                for sublist1 in mission_all_times[n-1]:
                    for sublist2 in mission_all_times[n]:
                        if (sublist1[0]>=sublist2[0] and sublist1[0]<=sublist2[1]) or (
                            sublist1[1]>=sublist2[0] and sublist1[1]<=sublist2[1]):
                            print 'Conflict: ' + str(sublist1) + str(sublist2)
                            return False
        return True

    def _validate_schedule(self, mission = {}):
        """
        Check the mission parameters for scheduling conflicts
        """
        
        for currentMissionIndex in range(len(mission)):   
            
            # platform_id = mission[currentMissionIndex]['platformID']

            schedule = mission[currentMissionIndex]['schedule']
            mission_params = mission[currentMissionIndex]['missionParams']

            # instrument_name.append(mission[currentMissionIndex]['name'])
            instrument_id = mission[currentMissionIndex]['instrumentID']

            print instrument_id
            
            #Check mission start time 
            start_time_string = schedule['startTime']
            if start_time_string.lower() == 'none':
                start_time = None
                print 'None'
            else:
                try:
                    start_time = calendar.timegm(time.strptime(start_time_string,'%m/%d/%Y %H:%M:%S'))
                    #Now get current time
                    current_time = time.time()
                    #Compare mission start time to current time
                    if current_time > start_time:
                        print "MissionLoader: validate_schedule: Start time has already elapsed"
                        print "Current time is: " +  time.strftime("%Y-%m-%d %H:%M:%S", gmtime(current_time))
                        print "Start time is: " +  time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
                    else:
                        print "Start time is OK" 
                except ValueError:
                    # log.error("MissionLoader: validate_schedule: startTime format error: " + str(start_time_string))
                    print "MissionLoader: validate_schedule: startTime format error: " + str(start_time_string)

            mission_duration = 0;
            # Check mission duration
            # TODO: Add all commands that include time duration 
            for index, items in enumerate(mission_params):
                #Calculate mission duration
                command = items['command'].lower() 
                if command == 'wait':
                    if len(items['params']) != 2:
                        raise IndexError('Need 2 parameters for a wait command')
                    else:
                        duration = items['params'][0]
                        units = items['params'][1]
                elif command == 'sample':
                    if len(items['params']) != 3:
                        raise IndexError('Need 3 parameters for a wait command')
                    else:
                        duration = items['params'][0]
                        units = items['params'][1]
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
                    mission_params[index]['params'][0] = duration
                    mission_params[index]['params'][1] = 'secs'

                mission_duration += duration

            print 'Mission Duration = ' + str(mission_duration) + ' secs'
            
            #Now check loop schedule
            num_loops = schedule['loop']['quantity']
            
            loop_duration = schedule['loop']['value']
            if type(loop_duration) == str:
                # TODO: Come up with event 
                pass
                # print loop_duration
                #This is an event driven loop, check event cases

            elif loop_duration == -1 or loop_duration > 1:
                loop_units = schedule['loop']['units']
                if loop_units.lower() == 'days':
                    loop_duration *= 3600*24
                elif loop_units.lower() == 'hrs':
                    loop_duration *= 3600
                elif loop_units.lower() == 'mins':
                    loop_duration *= 60

                if loop_duration <= mission_duration:
                    print 'Mission File Error: Mission duration > scheduled loop duration'
                    raise Exception('Mission Error: Mission duration greater than scheduled loop duration') 

                print 'Loop duration = ' + str(loop_duration) + ' secs'

            if num_loops != 0:
                #Add mission entry
                self._add_entry(instrument_id, start_time, mission_duration, 
                                    num_loops, loop_duration, mission_params)

        #Sort mission entries by start time
        self._sort_entries()

        instrument_id = []
        for instrument in self.mission_entries: instrument_id.append(instrument['instrument_id'])

        #Return indices of duplicate instruments to check schedules
        indices = [i for i, x in enumerate(instrument_id) if instrument_id.count(x) > 1]

        #Now check timing schedule of duplicate instruments
        if len(indices) > 1:
            self._check_intersections(indices)

    def load_mission_file(self, filename):
        
        self.filename = filename

        print 'Parsing ' + filename.split('/')[-1]
        
        try:
            f = open(filename)
        except IOError:
            print "Error: Can\'t find file"
            return False

        # mission_dict = yaml.load(f, Loader=OrderedDictYAMLLoader)
        mission_dict = yaml.safe_load(f)
        f.close()

        # Get start time
        self.raw_mission = mission_dict['mission']

        self._validate_schedule(self.raw_mission)

        return True


class MissionScheduler(object):
    """
    MissionScheduler takes care of the command/control and associated timing for a 
    platform mission
    """

    def __init__(self, platform_agent_obj = None, instrument_obj = None, mission = []):
        # TODO: Implement the platform agent 
        self._pa_client = platform_agent_obj
        self._instruments = {}

        # Get instrument clients for each instrument in the mission file
        for missionIndex in range(len(mission)):
            instrument_id = mission[missionIndex]['instrument_id']

            if instrument_id in instrument_obj:
                instrument_device_id = instrument_obj[instrument_id]['instrument_device_id']
                # Start a resource agent client to talk with each instrument agent.
                ia_client = ResourceAgentClient(instrument_device_id, process=FakeProcess())
                # make a dictionary storing the instrument ids and client objects
                self._instruments.update({instrument_id: ia_client})

        # Start up the platform
        self._startup_platform()

        if mission:
            self._schedule(mission)
        else:
            raise Exception('Mission Scheduler Error: No mission')

    def _schedule(self, mission):
        """
        Set up gevent threads for each mission
        TODO: Replace gevent with spawn_process
        """
        self.threads = []
        for missionIndex in range(len(mission)): 
            start_time = mission[missionIndex]['start_time']
            if start_time:  
                start_in = start_time - time.time() if (time.time() < start_time) else 0
                print 'Start in ' + str(start_in) + ' seconds'
                self.threads.append(Greenlet.spawn_later(start_in, self._run_mission, mission[missionIndex]))

        gevent.joinall(self.threads)

    def _run_mission(self, mission_params):
        """
        This function needs to live within the platform agent
        TODO: Make a MissionExecutive class?
        """
        instrument_id = mission_params['instrument_id']
        self._check_preconditions()
        
        loop_running = True
        loop_count = 0

        start_time = mission_params['start_time']
        
        # Master loop
        while loop_running:
            # Wake up instrument if necessary (may have timed out)
            # self.wake_up_instrument()

            current_time = time.time()

            for cmd in mission_params['mission_cmds']:
                # Parse command and parameters
                self._parse_command(instrument_id, cmd)

            if mission_params['num_loops'] > 0:
                loop_count += 1
                if loop_count >= mission_params['num_loops']:
                    loop_running = False
                else:
                    #Calculate next start time
                    if start_time:
                        start_time += mission_params['loop_duration']
                        print "Next Sequence starts at " + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(start_time))
                        while (time.time() < start_time):
                            gevent.sleep(1)




    # def wait_for_next_sequence(self, start_time, loop_duration)

    def _parse_command(self, instrument_id, cmd):

        command = cmd['command'].lower() 
        params = cmd['params']
        if command == 'wait':
            print ('Waiting ' + str(params[0]) + ' Seconds ' +
                    time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))    
            gevent.sleep(params[0]) 
            print 'Wait Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))

        elif command == 'sample':
            
            self._send_command(instrument_id, command, params[2:])

            print ('Sampling ' + str(params[0]) + ' Seconds ' +
                    time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            gevent.sleep(params[0])
            print 'Sample Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))

            self._send_command(instrument_id, 'stop')

        elif command == 'calibrate':

            print ('Calibating ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time())))
            self._send_command(instrument_id, command)

            
            # gevent.sleep(params[0])
            print 'Calibrating Over ' + time.strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time()))

            self._send_command(instrument_id, 'stop')


    def _send_command(self, instrument_id, command, params = []):

        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37ProtocolEvent
        from mi.instrument.seabird.sbe37smb.ooicore.driver import SBE37Parameter

        num_tries = 0
        state = None

        ia_client = self._instruments[instrument_id] 
        state = ia_client.get_agent_state()
        print state

        if command == 'sample':
            # A sample command has a sample duration and frequency
            SBEparams = [SBE37Parameter.INTERVAL]
            reply = ia_client.get_resource(SBEparams)
            print 'Sample Interval= ' + str(reply[SBE37Parameter.INTERVAL])
            
            if params and params[0] != reply[SBE37Parameter.INTERVAL]:
                ia_client.set_resource({SBE37Parameter.INTERVAL:params[0]})     
                reply = ia_client.get_resource(SBEparams)

            # # Acquire Status from SBE37
            # cmd = AgentCommand(command=SBE37ProtocolEvent.ACQUIRE_STATUS)
            # retval = ia_client.execute_resource(cmd)
            # state = ia_client.get_agent_state()
            # print state

            while (num_tries < 3) and (state != ResourceAgentState.STREAMING):
                num_tries += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.START_AUTOSAMPLE)
                retval = ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state

        elif command == 'stop':
            while (num_tries < 3) and (state != ResourceAgentState.COMMAND):
                num_tries += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.STOP_AUTOSAMPLE)
                retval = ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state

        elif command == 'calibrate':
            while (num_tries < 3) and (state != ResourceAgentState.CALIBRATE):
                num_tries += 1
                cmd = AgentCommand(command=SBE37ProtocolEvent.CALIBRATE)
                retval = ia_client.execute_resource(cmd)
                state = ia_client.get_agent_state()
                print state

    def _startup_platform(self):
        # TODO: Check if platform is already running
        from pyon.public import CFG
        self._receive_timeout = CFG.endpoint.receive.timeout

        # # Ping the platform agent
        # retval = self._pa_client.ping_agent()

        # Initialize platform
        kwargs = dict(recursion=True)
        cmd = AgentCommand(command=PlatformAgentEvent.INITIALIZE, kwargs=kwargs)
        log.info("_execute_agent: cmd=%r kwargs=%r; timeout=%s ...",
                 cmd.command, cmd.kwargs, self._receive_timeout)
        time_start = time.time()
        retval = self._pa_client.execute_agent(cmd, timeout=self._receive_timeout)
        elapsed_time = time.time() - time_start
        log.info("_execute_agent: timing cmd=%r elapsed_time=%s, retval = %s",
                 cmd.command, elapsed_time, str(retval))

        # Go active
        cmd = AgentCommand(command=PlatformAgentEvent.GO_ACTIVE, kwargs=kwargs)
        retval = self._pa_client.execute_agent(cmd)

        # Run
        cmd = AgentCommand(command=PlatformAgentEvent.RUN, kwargs=kwargs)
        retval = self._pa_client.execute_agent(cmd)


    def _wake_up_instrument(self, _ia_client):
        """
        Wake up instrument before sending commands
        """
        pass

    def _check_preconditions(self):
        # TODO: Implement precondition checks
        pass

if __name__ == "__main__":  # pragma: no cover
    """
    Stand alone to check the mission loading/parsing capabilities
    """
    filename =  "ion/agents/platform/test/mission_RSN_simulator1.yml"

    mission = MissionLoader()
    mission.load_mission_file(filename)


