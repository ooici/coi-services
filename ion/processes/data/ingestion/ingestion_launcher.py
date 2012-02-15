
"""
@author Luke Campbell
@file ion/processes/data/ingestion/ingestion_launcher.py
@description Ingestion Launcher
"""
from interface.objects import CouchStorage

from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from pyon.service.service import BaseService

class IngestionLauncher(BaseService):
    def on_start(self):
        super(IngestionLauncher,self).on_start()
        exchange_point = self.CFG.get('process',{}).get('exchange_point','science_data')
        couch_storage = self.CFG.get('process',{}).get('couch_storage',{})
        couch_storage = CouchStorage(**couch_storage)
        hdf_storage = self.CFG.get('process',{}).get('hdf_storage',{})
        number_of_workers = self.CFG.get('process',{}).get('number_of_workers',2)

        ingestion_management_service = IngestionManagementServiceClient(node=self.container.node)
        ingestion_id = ingestion_management_service.create_ingestion_configuration(
            exchange_point_id=exchange_point,
            couch_storage=couch_storage,
            hdf_storage=hdf_storage,
            number_of_workers=number_of_workers,
            default_policy={}
        )
        ingestion_management_service.activate_ingestion_configuration(ingestion_id)

