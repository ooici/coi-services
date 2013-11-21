#!/usr/bin/env python

"""
@package  ion.util.agent_launcher
@author   Ian Katz
"""

from pyon.core import bootstrap
from pyon.core.exception import BadRequest

from interface.objects import ProcessSchedule, ProcessRestartMode, ProcessQueueingMode, ProcessStateEnum
from ion.services.cei.process_dispatcher_service import ProcessStateGate

from ooi.logging import log


class AgentLauncher(object):

    def __init__(self, process_dispatcher_client):
        self.process_dispatcher_client = process_dispatcher_client
        self.process_id = None


    def launch(self, agent_config, process_definition_id):
        """
        schedule the launch
        """
        if isinstance(agent_config, dict) and "instance_id" in agent_config:
            agent_instance_id = agent_config.get("instance_id", None)
            log.debug("Save the agent spawn config to the object store")
            obj_id = "agent_spawncfg_" + agent_instance_id

            obj_store = bootstrap.container_instance.object_store
            try:
                obj_store.delete_doc(obj_id)
            except Exception:
                pass
            obj_store.create_doc(agent_config, obj_id)

            config_ref = "objects:" + obj_id
            launch_config = {'process': {'config_ref': config_ref}}
        else:
            launch_config = agent_config

        log.debug("schedule agent process")
        process_schedule = ProcessSchedule(restart_mode=ProcessRestartMode.ABNORMAL,
                                           queueing_mode=ProcessQueueingMode.ALWAYS)
        process_id = self.process_dispatcher_client.schedule_process(process_definition_id=process_definition_id,
                                                                      schedule=process_schedule,
                                                                      configuration=launch_config)

        log.info("AgentLauncher got process id='%s' from process_dispatcher.schedule_process()", process_id)
        self.process_id = process_id
        return process_id


    def await_launch(self, timeout, process_id=None):

        if None is process_id:
            if None is self.process_id:
                raise BadRequest("No process_id was supplied to await_launch, and " +
                                 "no process_id was available from launch")
            else:
                process_id = self.process_id

        log.debug("waiting %s seconds for agent launch", timeout)
        psg = ProcessStateGate(self.process_dispatcher_client.read_process, process_id, ProcessStateEnum.RUNNING)
        if not psg.await(timeout):
            # todo: different error
            raise BadRequest("The agent process '%s' failed to launch in %s seconds" %
                             (process_id, timeout))

