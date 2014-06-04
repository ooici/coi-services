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
import logging
from ion.agents.mission_executive import MissionLoader
from ion.agents.mission_executive import MissionScheduler
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from pyon.public import IonObject, OT, RT
from pyon.core.governance import get_system_actor_header
from pyon.util.containers import get_ion_ts


class MissionManager(object):
    """
    Coordinating class for integration of mission execution with platform agent.
    """

    def __init__(self, pa):
        """
        Called by platform agent upon its initialization so there is a driver
        already created and configured.

        @param pa   The associated platform agent object to access the
                    elements handled by this helper.
        """
        self._agent = pa
        self._platform_id = pa._platform_id

        # mission_id -> MissionScheduler mapping:
        self._running_missions = {}
        log.debug('%r: [mm] MissionManager created', self._platform_id)

        self._provider_id = self._agent._provider_id
        self._actor_id = self._agent._actor_id
        log.debug('[xa] provider_id=%r  actor_id=%r', self._provider_id, self._actor_id)

        # ctx = self._agent.get_context()
        # self._actor_id = ctx.get('ion-actor-id', None) if ctx else None
        # log.debug('[xa] actor_id=%r', self._actor_id)
        if self._actor_id is None:
            log.warn('[xa] actor_id is None')

        # _exaccess: resource_id -> {'commitment_id': id, 'mission_ids': [mission_id, ...]}:
        # the agents we have acquired exclusive access to. We remove the actual exclusive
        # access when there are no more associated mission_id's for a given resource_id.
        self._exaccess = {}

        self.ORG = OrgManagementServiceProcessClient(process=self._agent)
        self.RR  = ResourceRegistryServiceClient()

        # TODO what's the correct way to obtain the actor header? the following is
        # working but likely because the same call is done in
        # base_test_platform_agent_with_rsn for the IMS.start_platform_agent_instance call
        self._actor_header = get_system_actor_header()
        log.debug('[xa] actor_header=%s', self._actor_header)

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

        try:
            mission_loader, mission_scheduler = self._create_mission_scheduler(mission_id, mission_yml)
        except Exception as ex:
            log.exception('[mm] run_mission: mission_id=%r _create_mission_scheduler exception', mission_id)
            return

        self._running_missions[mission_id] = mission_scheduler
        log.debug('[mm] starting mission_id=%r (#running missions=%s)',
                  mission_id, len(self._running_missions))
        try:
            mission_scheduler.run_mission()
        except Exception as ex:
            log.exception('[mm] run_mission mission_id=%r', mission_id)
        finally:
            del self._running_missions[mission_id]

            # remove exclusive access:
            mission_entries = mission_loader.mission_entries
            for mission_entry in mission_entries:
                instrument_ids = mission_entry.get('instrument_id', [])
                for instrument_id in instrument_ids:
                    self._remove_exclusive_access(instrument_id, mission_id)

            log.debug('[mm] completed mission_id=%r (#running missions=%s)',
                      mission_id, len(self._running_missions))

    def external_event_received(self, evt):
        # TODO remove this method -- it was decided unneeded: any object
        # interested in reacting to events just subscribe to those directly,
        # as opposed to try to duplicate its dispatching as this method
        # would be trying to do.
        """
        Notifies all running missions about the received event.
        """
        log.debug('[mm] Notifying %s missions about external_event_received: %s. ',
                  len(self._running_missions), evt)
        # for scheduler in self._running_missions.itervalues():
        #     scheduler.event_received(evt)

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

    def destroy(self):
        """
        Called by platform agent when it is reset.
        Aborts all ongoing missions if any. Any errors are logged out.
        """
        mission_ids = self._running_missions.keys()
        nn = len(mission_ids)
        if nn:
            log.debug('%r: [mm] MissionManager.destroy called. Aborting %n ongoing missions...',
                      self._platform_id, nn)
            for mission_id in mission_ids:
                try:
                    self.abort_mission(mission_id)
                except Exception as ignored:
                    pass
            self._running_missions = {}
        else:
            log.debug('%r: [mm] MissionManager.destroy called. No ongoing missions executing.',
                      self._platform_id)

    ############
    # private
    ############

    def _create_mission_scheduler(self, mission_id, mission_yml):
        """
        @param mission_id
        @param mission_yml

        @return (mission_loader, mission_scheduler)
        @raise  Exception the first exception while requesting exclusive
                access to a child instrument. All other successful
                such requests, if any, are reverted.
        """
        log.debug('[mm] _create_mission_scheduler: mission_id=%r', mission_id)

        mission_loader = MissionLoader(self._agent)
        mission_loader.load_mission(mission_id, mission_yml)
        self._mission_entries = mission_loader.mission_entries

        if log.isEnabledFor(logging.DEBUG):
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

        mission_entries = mission_loader.mission_entries

        # get all involved instruments referenced in the mission:
        instrument_ids = set()
        for mission_entry in mission_entries:
            for instrument_id in mission_entry.get('instrument_id', []):
                if instrument_id in instruments:
                    instrument_ids.add(instrument_id)

        # get exclusive access to those instruments. If any one fails,
        # rollback and raise that first exception:
        instrument_ids_ok = set()
        exception = None
        for instrument_id in instrument_ids:
            try:
                self._get_exclusive_access(instrument_id, mission_id)
                instrument_ids_ok.add(instrument_id)
            except Exception as ex:
                exception = ex
                log.warn('[xa] _create_mission_scheduler: exclusive access request to'
                         ' resource_id=%r failed: %s', instrument_id, exception)
                break

        if exception:
            if len(instrument_ids_ok):
                log.warn('[xa] _create_mission_scheduler: reverting exclusive access '
                         'to the resources: %s', instrument_ids_ok)
                for instrument_id in instrument_ids_ok:
                    try:
                        self._remove_exclusive_access(instrument_id, mission_id)
                    except Exception as ex:
                        # just log warning an continue
                        log.warn('[xa] exception while reverting exclusive access to '
                                 'resource_id=%r: %s', instrument_id, ex)

            raise exception

        mission_scheduler = MissionScheduler(self._agent,
                                             instruments,
                                             mission_entries)
        log.debug('[mm] _create_mission_scheduler: MissionScheduler created. entries=%s',
                  mission_entries)
        return mission_loader, mission_scheduler

    def _get_exclusive_access(self, resource_id, mission_id):
        """
        Gets exclusive access for the given resource_id. The actual request is
        only done once for the same resource_id, but we keep track of the
        associated mission_id such that the exclusive access is removed when
        no missions remain referencing the resource_id.

        @param resource_id
        @param mission_id
        """

        # check if we already have exclusive access to resource_id:
        if resource_id in self._exaccess:
            mission_ids = self._exaccess[resource_id]['mission_ids']
            if mission_id in mission_ids:
                log.debug('[xa] resource_id=%r already with exclusive access, '
                          'mission_id=%r', resource_id, mission_id)
            else:
                mission_ids.append(mission_id)
                log.debug('[xa] resource_id=%r already with exclusive access from '
                          'previous call with mission_id=%r', resource_id, mission_ids[0])
            return

        log.debug('[xa] _get_exclusive_access: resource_id=%s, actor_id=%r, provider=%r',
                  resource_id, self._actor_id, self._provider_id)

        # TODO proper handling of BadRequest exception upon failure to obtain
        # exclusive access. For now, just logging ghe exception.
        try:
            commitment_id = self._do_get_exclusive_access(resource_id)
            self._exaccess[resource_id] = dict(commitment_id=commitment_id,
                                               mission_ids=[mission_id])
        except BadRequest:
            log.exception('[xa] _get_exclusive_access: resource_id=%r, mission_id=%r',
                          resource_id, mission_id)

    def _do_get_exclusive_access(self, resource_id):
        """
        Gets exclusive access to a given resource.

        @return  commitment_id
        """
        # TODO Needs review

        from interface.objects import NegotiationTypeEnum
        neg_type = NegotiationTypeEnum.INVITATION
        neg_obj = IonObject(RT.Negotiation, negotiation_type=neg_type)
        negotiation_id, _ = self.RR.create(neg_obj)

        # TODO determine appropriate expiration. Could it be without expiration
        # given that the exclusive access will be removed explicitly upon
        # termination (normal or abnormal) of the the mission?
        expiration = int(get_ion_ts()) + 20 * 60 * 1000

        # the SAP for the acquire resource exclusively proposal:
        arxp = IonObject(OT.AcquireResourceExclusiveProposal,
                         consumer=self._actor_id,
                         resource_id=resource_id,
                         provider=self._provider_id,
                         expiration=str(expiration),
                         negotiation_id=negotiation_id)

        # we are initially opting for only "phase 2" -- just acquire_resource:
        commitment_id = self.ORG.acquire_resource(arxp, headers=self._actor_header)
        log.debug('[xa] AcquireResourceExclusiveProposal: '
                  'resource_id=%s -> commitment_id=%s', resource_id, commitment_id)
        return commitment_id

        # #####################################################################
        # # with "negotiation" it seems it would involve something like the
        # # following (see coi/../test_gobernance.py):
        # # 1- negotiate a base acquire resource proposal
        # # 2- negotiate the exclusive acquire resource proposal
        #
        # # acquire resource proposal
        # arp = IonObject(OT.AcquireResourceProposal,
        #                 consumer=self._actor_id,
        #                 resource_id=resource_id,
        #                 provider=self._provider_id)
        # arp_response = self.ORG.negotiate(arp, headers=self._actor_header)
        # log.debug('[xa] _get_exclusive_access/AcquireResourceProposal: '
        #           'resource_id=%s -> arp_response=%s', resource_id, arp_response)
        #
        # arxp_response = self.ORG.negotiate(arxp, headers=self._actor_header)
        # log.debug('[xa] _get_exclusive_access/AcquireResourceExclusiveProposal: '
        #           'resource_id=%s -> arxp_response=%s', resource_id, arxp_response)

    def _remove_exclusive_access(self, resource_id, mission_id):
        """
        Removes the exclusive access for the given resource_id. The actual
        removal is only done if there are no more missions associated.

        @param resource_id
        @param mission_id
        """
        if not resource_id in self._exaccess:
            log.warn('[xa] not associated with exclusive access resource_id=%r', resource_id)
            return

        mission_ids = self._exaccess[resource_id]['mission_ids']
        if not mission_id in mission_ids:
            log.warn('[xa] not associated with exclusive access resource_id=%r', resource_id)
            return

        mission_ids.remove(mission_id)

        if len(mission_ids) > 0:
            log.debug('[xa] exclusive access association removed: resource_id=%r -> mission_id=%r',
                      resource_id, mission_id)
            return

        # no more mission_ids associated, so release the exclusive access:
        commitment_id = self._exaccess[resource_id]['commitment_id']
        del self._exaccess[resource_id]
        self._do_remove_exclusive_access(commitment_id, resource_id)

    def _do_remove_exclusive_access(self, commitment_id, resource_id):
        """
        Does the actual release of the exclusive access.

        @param commitment_id   commitment to the released
        @param resource_id     associated resource ID for logging purposes
        """
        # TODO: any exception below is just logged out; need different handling?
        try:
            ret = self.ORG.release_commitment(commitment_id)
            log.debug('[xa] exclusive access removed: resource_id=%r: '
                      'ORG.release_commitment(commitment_id=%r) returned=%r',
                      resource_id, commitment_id, ret)

        except Exception as ex:
            log.exception('[xa] resource_id=%r: ORG.release_commitment(commitment_id=%r)',
                          resource_id, commitment_id)
