#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/bootstrap/plugins/bootstrap_qc_post_processor.py
@description Bootstraps the QC Post Processor
'''

from interface.services.cei.ischeduler_service import SchedulerServiceProcessClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceProcessClient
from pyon.public import RT,CFG
from ion.core.bootstrap_process import BootstrapPlugin
from pyon.util.containers import DotDict
from interface.objects import ProcessDefinition
from uuid import uuid4
import time

class BootstrapQCPostProcessor(BootstrapPlugin):
    '''
    Sets up one QC Post Processing worker and initiates
    the Scheduler Service's interval every 24 hours.
    '''

    def on_initial_bootstrap(self, process, config, **kwargs):
        if self.process_exists(process, 'qc_post_processor'):
            # Short circuit the bootstrap to make sure not more than one is ever started
            return

        self.scheduler_service = SchedulerServiceProcessClient(process=process)
        self.process_dispatcher = ProcessDispatcherServiceProcessClient(process=process)
        self.run_interval = CFG.get_safe('service.qc_processing.run_interval', 24)

        interval_key = uuid4().hex # Unique identifier for this process

        config = DotDict()
        config.process.interval_key = interval_key

        process_definition = ProcessDefinition(name='qc_post_processor',
            executable={'module':'ion.processes.data.transforms.qc_post_processing', 'class':'QCPostProcessing'})
        process_definition_id = self.process_dispatcher.create_process_definition(process_definition)

        process_id = self.process_dispatcher.create_process(process_definition_id)
        self.process_dispatcher.schedule_process(process_definition_id, process_id=process_id, configuration=config)


        timer_id = self.scheduler_service.create_interval_timer(start_time=str(time.time()),
                end_time='-1', #Run FOREVER
                interval=3600*24,
                event_origin=interval_key)

    def process_exists(self, process, name):
        proc_ids, meta = process.container.resource_registry.find_resources(restype=RT.Process, id_only=True)
        return any([name in p['name'] for p in meta])

