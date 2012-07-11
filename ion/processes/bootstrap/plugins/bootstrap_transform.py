#!/usr/bin/env python

from ion.core.bootstrap_process import BootstrapPlugin
from pyon.public import RT, log, PRED, iex

from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceProcessClient


class BootstrapTransform(BootstrapPlugin):
    """
    Bootstrap process for transform management.
    """

    def on_restart(self, process, config, **kwargs):
        pds_client = ProcessDispatcherServiceProcessClient(process=self)

        def restart_transform(transform_id):
            transform = process.container.resource_registry.read(transform_id)
            configuration = transform.configuration
            proc_def_ids,other = process.container.resource_registry.find_objects(subject=transform_id,predicate=PRED.hasProcessDefinition,id_only=True)

            if len(proc_def_ids) < 1:
                log.warning('Transform did not have a correct process definition.')
                return

            pid = pds_client.schedule_process(
                process_definition_id=proc_def_ids[0],
                configuration=configuration
            )

            transform.process_id = pid
            process.container.resource_registry.update(transform)


        restart_flag = config.get_safe('service.transform_management.restart', False)
        if restart_flag:
            transform_ids, meta = process.container.resource_registry.find_resources(restype=RT.Transform, id_only=True)
            for transform_id in transform_ids:
                restart_transform(transform_id)
