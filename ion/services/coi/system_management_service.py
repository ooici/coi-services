#!/usr/bin/env python

__author__ = 'Michael Meisinger, Jonathan Newbrough'

from pyon.event.event import EventPublisher
from pyon.public import OT, IonObject, log
from pyon.util.containers import get_ion_ts

from interface.objects import AllContainers
from interface.services.coi.isystem_management_service import BaseSystemManagementService


ALL_CONTAINERS_INSTANCE = AllContainers()

class SystemManagementService(BaseSystemManagementService):
    """ container management requests are handled by the event listener
        ion.processes.event.container_manager.ContainerManager
        which must be running on each container.
    """
    def on_start(self,*a,**b):
        super(SystemManagementService,self).on_start(*a,**b)
        self.sender = EventPublisher()

    def on_quit(self,*a,**b):
        self.sender.close()

    def perform_action(self, predicate, action):
        userid = None # get from context
        self.sender.publish_event(event_type=OT.ContainerManagementRequest, origin=userid, predicate=predicate, action=action)

    def set_log_level(self, logger='', level='', recursive=False):
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.ChangeLogLevel, logger=logger, level=level, recursive=recursive))


    def reset_policy_cache(self, headers=None, timeout=None):
        """Clears and reloads the policy caches in all of the containers.

        @throws BadRequest    None
        """
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.ResetPolicyCache))

    def trigger_garbage_collection(self):
        """Triggers a garbage collection in all containers

        @throws BadRequest    None
        """
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.TriggerGarbageCollection))

    def trigger_container_snapshot(self, snapshot_id='', include_snapshots=None, exclude_snapshots=None,
                                   take_at_time='', clear_all=False, persist_snapshot=True, snapshot_kwargs=None):

        if not snapshot_id:
            snapshot_id = get_ion_ts()
        if not snapshot_kwargs:
            snapshot_kwargs = {}

        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.TriggerContainerSnapshot,
                                                               snapshot_id=snapshot_id,
                                                               include_snapshots=include_snapshots,
                                                               exclude_snapshots=exclude_snapshots,
                                                               take_at_time=take_at_time,
                                                               clear_all=clear_all,
                                                               persist_snapshot=persist_snapshot,
                                                               snapshot_kwargs=snapshot_kwargs))
        log.info("Event to trigger container snapshots sent. snapshot_id=%s" % snapshot_id)

    def start_gevent_block(self, alarm_mode=False):
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.StartGeventBlock, alarm_mode=alarm_mode))

    def stop_gevent_block(self):
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.StopGeventBlock))

    def prepare_system_shutdown(self, mode=''):
        self.perform_action(ALL_CONTAINERS_INSTANCE, IonObject(OT.PrepareSystemShutdown, mode=mode))
