#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file data_retriever_test_a.py
@date 06/14/12 15:06
@description DESCRIPTION
'''
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.arg_check import validate_is_instance
from pyon.public import CFG
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from pyon.datastore.datastore import DataStore
from pyon.core.bootstrap import get_sys_name
from pyon.ion.transforma import TransformAlgorithm
from numbers import Number


@attr('INT', group='dm')
class DataRetrieverIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverIntTest,self).setUp()


        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')


        self.datastore_name = 'test_datasets'
        self.datastore      = self.container.datastore_manager.get_datastore(self.datastore_name, profile=DataStore.DS_PROFILE.SCIDATA)

        self.data_retriever     = DataRetrieverServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.resource_registry  = ResourceRegistryServiceClient()
        self.pubsub_management  = PubsubManagementServiceClient()

        xs_dot_xp = CFG.core_xps.science_data

        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)


    def test_transform_data(self):
        module = __name__
        cls = 'FakeTransform'
        retval = DataRetrieverService._transform_data(0,module,cls)
        self.assertEquals(retval,1)



#--------------------------------------------------------------------------------
# FakeTransform
#--------------------------------------------------------------------------------
class FakeTransform(TransformAlgorithm):
    @staticmethod
    def execute(data):
        validate_is_instance(data,Number)
        return data+1


