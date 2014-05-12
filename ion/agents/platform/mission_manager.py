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

        self._agent.aparam_mission = None
        self._mission_entries = None
        self._mission_scheduler = None

        self._agent.aparam_set_mission = self.aparam_set_mission

    # TODO confirm appropriate mechanism to indicate mission to the agent
    def aparam_set_mission(self, yaml_filename):
        """
        Specifies mission to be executed.
        @param yaml_filename  Mission definition filename; can be None.
        """
        log.debug('[mm] aparam_set_mission: yaml_filename=%s', yaml_filename)

        self._agent.aparam_mission = yaml_filename

        if self._agent.aparam_mission is None:
            self._mission_entries = None
            self._mission_scheduler = None
            return

        mission_loader = MissionLoader(self._agent)
        mission_loader.load_mission_file(yaml_filename)
        self._mission_entries = mission_loader.mission_entries

        log.debug('[mm] aparam_set_mission: _ia_clients=\n%s',
                  self._agent._pp.pformat(self._agent._ia_clients))

        # get instrument IDs and clients for the valid running instruments:
        instruments = {}
        for (instrument_id, obj) in self._agent._ia_clients.iteritems():
            if isinstance(obj, dict):
                # it's valid instrument.
                if instrument_id != obj.resource_id:
                    log.error('[mm] aparam_set_mission: instrument_id=%s, '
                              'resource_id=%s', instrument_id, obj.resource_id)

                instruments[obj.resource_id] = obj.ia_client

        self._mission_scheduler = MissionScheduler(self._agent,
                                                   instruments,
                                                   self._mission_entries)
        log.debug('[mm] aparam_set_mission: MissionScheduler created. entries=%s',
                  self._mission_entries)

    # TODO appropriate way to handle potential errors/exceptions in
    # methods below.  Very ad hoc for the moment.

    def run_mission(self):
        if self._mission_scheduler is not None:
            try:
                self._mission_scheduler.run_mission()
                return None
            except Exception as ex:
                log.exception('[mm] run_mission')
                return ex
        else:
            log.error('[mm] run_mission: no mission given')

    def abort_mission(self):
        if self._mission_scheduler is not None:
            try:
                self._mission_scheduler.abort_mission()
                return None
            except Exception as ex:
                log.exception('[mm] abort_mission')
                return ex
        else:
            log.error('[mm] abort_mission: no mission given')

    def kill_mission(self):
        if self._mission_scheduler is not None:
            try:
                self._mission_scheduler.kill_mission()
                return None
            except Exception as ex:
                log.exception('[mm] kill_mission')
                return ex
        else:
            log.error('[mm] kill_mission: no mission given')
