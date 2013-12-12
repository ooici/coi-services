#!/usr/bin/env python
from pyon.ion.process import ImmediateProcess
from pyon.public import RT, PRED
from pyon.util.file_sys import FileSystem
from ion.processes.data.registration.registration_process import RegistrationProcess
import os

class RegistrationBootstrap(ImmediateProcess):
    def on_start(self):
        self.resource_registry = self.container.resource_registry

        if self.CFG.get_safe('op') == 'register_datasets':
            self.register_datasets()

    def register_datasets(self):
        self.refresh_datasets_xml()
        registrar = self.get_registration_handle()
        registrar.on_start()

        dataset_ids, _ = self.resource_registry.find_resources(restype=RT.Dataset, id_only=True)
        for dataset_id in dataset_ids:
            data_products_ids, _ = self.resource_registry.find_subjects(object=dataset_id, predicate=PRED.hasDataset, id_only=True)
            for data_product_id in data_products_ids:
                registrar.register_dap_dataset(data_product_id)


    def get_registration_handle(self):
        # Get a handle on the registration worker
        rp = RegistrationProcess()
        rp.CFG = self.CFG
        rp.container = self.container
        return rp

    def refresh_datasets_xml(self):
        datasets_xml_path = self.CFG.get_safe('server.pydap.datasets_xml_path', "RESOURCE:ext/datasets.xml")
        filename = datasets_xml_path.split('/')[-1]
        base = '/'.join(datasets_xml_path.split('/')[:-1])
        real_path = FileSystem.get_extended_url(base)
        datasets_xml_path = os.path.join(real_path, filename)

        os.remove(datasets_xml_path)

