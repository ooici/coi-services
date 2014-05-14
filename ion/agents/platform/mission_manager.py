#!/usr/bin/env python

"""
@package ion.agents.platform.mission_manager
@file    ion/agents/platform/mission_manager.py
@author  Carlos Rueda
@brief   Coordinating class for integration of mission execution with
         platform agent
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.mission_executive import MissionLoader
from ion.agents.mission_executive import MissionScheduler
from pyon.core.exception import BadRequest


class MissionManager(object):
    """
    Coordinating class for integration of mission execution with platform agent.
    """

    def __init__(self, pa):
        """
        @param pa   The associated platform agent object to access the
                    elements handled by this helper.
        """
        self._agent = pa
        self._platform_id = pa._platform_id

        # mission_id -> MissionScheduler mapping:
        self._running_missions = {}

    def get_number_of_running_missions(self):
        return len(self._running_missions)

    # TODO perhaps we need some additional error handling below besides the
    # relevant event notifications done by mission executive.

    def run_mission(self, mission_id, mission_yml):
        """
        Runs a mission returning to caller when the execution is completed.

        @param mission_id
        @param mission_yml
        """

        if mission_id in self._running_missions:
            raise BadRequest('run_mission: mission_id=%r is already running', mission_id)

        mission_scheduler = self._create_mission_scheduler(mission_id, mission_yml)
        self._running_missions[mission_id] = mission_scheduler
        try:
            mission_scheduler.run_mission()
        except Exception as ex:
            log.exception('[mm] run_mission mission_id=%r', mission_id)
        finally:
            del self._running_missions[mission_id]

    def abort_mission(self, mission_id):
        if mission_id not in self._running_missions:
            raise BadRequest('abort_mission: invalid mission_id=%r', mission_id)

        mission_scheduler = self._running_missions[mission_id]
        try:
            mission_scheduler.abort_mission()
            return None
        except Exception as ex:
            log.exception('[mm] abort_mission')
            return ex
        finally:
            del self._running_missions[mission_id]

    def kill_mission(self, mission_id):
        if mission_id not in self._running_missions:
            raise BadRequest('kill_mission: invalid mission_id=%r', mission_id)

        mission_scheduler = self._running_missions[mission_id]
        try:
            mission_scheduler.kill_mission()
            return None
        except Exception as ex:
            log.exception('[mm] kill_mission')
            return ex
        finally:
            del self._running_missions[mission_id]

    ############
    # private
    ############

    def _create_mission_scheduler(self, mission_id, mission_yml):
        """
        @param mission_id
        @param mission_yml
        """
        log.debug('[mm] _create_mission_scheduler: mission_id=%r', mission_id)

        mission_loader = MissionLoader(self._agent)

        # TODO: for the moment passing the filename still while load_mission is implemented
        mission_loader.load_mission_file(mission_yml)
        # mission_loader.load_mission(mission_id, mission_yml)

        self._mission_entries = mission_loader.mission_entries

        log.debug('[mm] _create_mission_scheduler: _ia_clients=\n%s',
                  self._agent._pp.pformat(self._agent._ia_clients))

        # get instrument IDs and clients for the valid running instruments:
        instruments = {}
        for (instrument_id, obj) in self._agent._ia_clients.iteritems():
            if isinstance(obj, dict):
                # it's valid instrument.
                if instrument_id != obj.resource_id:
                    log.error('[mm] _create_mission_scheduler: instrument_id=%s, '
                              'resource_id=%s', instrument_id, obj.resource_id)

                instruments[obj.resource_id] = obj.ia_client

        mission_scheduler = MissionScheduler(self._agent,
                                             instruments,
                                             self._mission_entries)
        log.debug('[mm] _create_mission_scheduler: MissionScheduler created. entries=%s',
                  self._mission_entries)
        return mission_scheduler
