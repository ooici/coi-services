
"""
@author Luke Campbell
@file ion/processes/data/ingestion/ingestion_launcher.py
@description Ingestion Launcher
"""
from interface.objects import CouchStorage, HdfStorage

from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from pyon.ion.process import StandaloneProcess

class IngestionLauncher(StandaloneProcess):
    def on_start(self):
        super(IngestionLauncher,self).on_start()

        exchange_point = self.CFG.get_safe('ingestion.exchange_point','science_data')
        couch_opts = self.CFG.get_safe('ingestion.couch_storage',{})
        couch_storage = CouchStorage(**couch_opts)
        hdf_opts = self.CFG.get_safe('ingestion.hdf_storage',{})
        hdf_storage = HdfStorage(**hdf_opts)
        number_of_workers = self.CFG.get_safe('ingestion.number_of_workers',2)

        ingestion_management_service = IngestionManagementServiceClient(node=self.container.node)
        ingestion_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id=exchange_point,
            couch_storage=couch_storage,
            hdf_storage=hdf_storage,
            number_of_workers=number_of_workers
        )
        ingestion_management_service.activate_ingestion_configuration(ingestion_id)

