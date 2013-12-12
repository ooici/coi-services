#!/usr/bin/env python
from pyon.ion.process import ImmediateProcess
from pyon.public import RT, PRED
from pyon.net.endpoint import RPCClient
from pyon.core.exception import ServiceUnavailable
from interface.objects import ProcessDefinition
from pyon.util.file_sys import FileSystem
import os

class RegistrationBootstrap(ImmediateProcess):
    def on_start(self):
        self.resource_registry = self.container.resource_registry

        if self.CFG.get_safe('op') == 'register_datasets':
            self.register_datasets()

    def register_datasets(self):
        self.refresh_datasets_xml()
        registration_client = self.get_registration_handle()
        registration_client.request({}, op='on_start')

        dataset_ids, _ = self.resource_registry.find_resources(restype=RT.Dataset, id_only=True)
        for dataset_id in dataset_ids:
            data_products_ids, _ = self.resource_registry.find_subjects(object=dataset_id, predicate=PRED.hasDataset, id_only=True)
            for data_product_id in data_products_ids:
                registration_client.request({'data_product_id':data_product_id}, op='register_dap_dataset')


    def get_registration_handle(self):
        # Get a handle on the registration worker
        procs,_ = self.resource_registry.find_resources(restype=RT.Process, id_only=True)
        pid = None
        for p in procs:
            if 'registration_worker' in p:
                pid = p
        if not pid: 
            pid = self.launch_registration()
            if pid is None:
                raise ServiceUnavailable("Failed to launch registration worker")
        rpc_cli = RPCClient(to_name=pid)
        return rpc_cli

    def launch_registration(self):
        res, meta = self.resource_registry.find_resources(name='registration_worker', restype=RT.ProcessDefinition)
        if len(res):
            return

        registration_module = self.CFG.get_safe('bootstrap.processes.registration.module', 'ion.processes.data.registration.registration_process')
        registration_class  = self.CFG.get_safe('bootstrap.processes.registration.class', 'RegistrationProcess')
        use_pydap = self.CFG.get_safe('bootstrap.use_pydap', False)


        process_definition = ProcessDefinition(
                name='registration_worker',
                description='For registering datasets with ERDDAP')
        process_definition.executable['module'] = registration_module
        process_definition.executable['class']  = registration_class


        return self._create_and_launch(process_definition, use_pydap)

    def refresh_datasets_xml(self):
        datasets_xml_path = self.CFG.get_safe('server.pydap.datasets_xml_path', "RESOURCE:ext/datasets.xml")
        filename = datasets_xml_path.split('/')[-1]
        base = '/'.join(datasets_xml_path.split('/')[:-1])
        real_path = FileSystem.get_extended_url(base)
        datasets_xml_path = os.path.join(real_path, filename)

        os.remove(datasets_xml_path)

    def _create_and_launch(self, process_definition, conditional=True):
        proc_def_id = self.pds_client.create_process_definition(process_definition=process_definition)

        if conditional:

            process_res_id = self.pds_client.create_process(process_definition_id=proc_def_id)
            self.pds_client.schedule_process(process_definition_id=proc_def_id, process_id=process_res_id)
            return process_res_id
        return None

