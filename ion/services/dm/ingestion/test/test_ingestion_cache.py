'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/ingestion/test/test_aggregate_ingestion.py
@description Integration Test for aggregate ingestion worker
'''
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from pyon.util.int_test import IonIntegrationTestCase

class IngestionCacheTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

    def start_worker(self):
        pubsub_cli = PubsubManagementServiceClient()
        tms_cli = TransformManagementServiceClient()
        pd_cli = ProcessDispatcherServiceClient()

