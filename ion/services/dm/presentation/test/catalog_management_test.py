    #!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file 
@date 04/18/12 11:56
@description DESCRIPTION
'''
from mock import Mock
from interface.objects import Catalog, Index, SearchOptions
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from ion.services.dm.presentation.catalog_management_service import CatalogManagementService
from pyon.core.exception import BadRequest, NotFound
from pyon.ion.resource import PRED
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr


@attr('UNIT', group='dm')
class CatalogManagementUnitTest(PyonTestCase):
    def setUp(self):
        super(CatalogManagementUnitTest, self).setUp()

        mock_clients = self._create_service_mock('catalog_management')
        self.catalog_management = CatalogManagementService()
        self.catalog_management.clients = mock_clients

        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_assoc = mock_clients.resource_registry.find_associations
        self.rr_find_assocs_mult = mock_clients.resource_registry.find_associations_mult
        self.rr_find_res = mock_clients.resource_registry.find_resources
        self.rr_find_obj = mock_clients.resource_registry.find_objects
        self.rr_find_subj = mock_clients.resource_registry.find_subjects
        self.rr_delete_assoc = mock_clients.resource_registry.delete_association
        self.rr_create_assoc = mock_clients.resource_registry.create_association

        self.ims_list_indexes = mock_clients.index_management.list_indexes

    def test_create_catalog(self):
        self.rr_find_res.return_value = ([],[])
        fake_indexes = [
            DotDict({'_id':'1', 'name':'one', 'options' : {'attribute_match':['attr_field'],'range_fields':[], 'geo_fields':[] }}),
            DotDict({'_id':'1', 'name':'two', 'options' : {'attribute_match':[],'range_fields':['range_field'], 'geo_fields':[] }}),
            DotDict({'_id':'1', 'name':'three', 'options' : {'attribute_match':[],'range_fields':['range_field'], 'geo_fields':['geo_field'] }})
        ]
        def find_res(*args, **kwargs):
            retval = {}
            for d in fake_indexes:
                retval[d.name] = d
            return retval


        self.ims_list_indexes.side_effect = find_res

        self.rr_create.return_value = 'cat_id','rev'


        retval = self.catalog_management.create_catalog('catalog_name',['range_field','geo_field'])
        self.assertTrue(self.rr_create_assoc.call_count==1,
            'Not enough associations %d' % self.rr_create_assoc.call_count)
        retval = self.catalog_management.create_catalog('catalog_name',['range_field'])
        self.assertTrue(self.rr_create_assoc.call_count==3,
            'Not enough associations %d' % self.rr_create_assoc.call_count)
        self.assertTrue(retval=='cat_id')




    def test_update_catalog(self):
        # Mocks
        catalog_res = Catalog()

        # Execution
        self.catalog_management.update_catalog(catalog_res)

        # Assertions
        self.assertTrue(self.rr_update.called, "Improper update of resource")

    def test_read_catalog(self):
        # Mocks
        catalog_res = dict(check=True)
        self.rr_read.return_value = catalog_res

        # Execution
        retval = self.catalog_management.read_catalog('catalog_id')

        # Assertions
        self.rr_read.assert_called_once_with('catalog_id', '')
        self.assertTrue(retval['check'])

    def test_delete_catalog(self):
        view_assoc = DotDict(_id=0)
        index_assoc = DotDict(_id=1)

        # Mocks
        self.rr_find_assocs_mult.return_value = ([],[index_assoc])
        self.rr_find_subj.return_value = ([],[view_assoc])


        # Execution
        self.catalog_management.delete_catalog('catalog_id')

        # Assertions
        self.rr_delete.assert_called_once_with('catalog_id')
        self.assertTrue(self.rr_delete_assoc.call_count == 2)



        



@attr('INT', group='dm')
class CatalogManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CatalogManagementIntTest,self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.cms_cli = CatalogManagementServiceClient()
        self.rr_cli  = ResourceRegistryServiceClient()

    def test_create_catalog(self):
        #-------------------------------------
        # Tests basic catalog creation
        #-------------------------------------

        index1 = Index(name='index1')
        index1.options.attribute_match = ['common']
        index_id, _ = self.rr_cli.create(index1)

        catalog_id = self.cms_cli.create_catalog('common_catalog', ['common'])
        self.assertTrue(index_id in self.cms_cli.list_indexes(catalog_id))

    def test_create_catalog_matching(self):
        #-------------------------------------------
        # Tests catalog creation and index matching
        #-------------------------------------------

        indexes = [
            Index(name='index1'),
            Index(name='index2'),
            Index(name='index3')
        ]
        indexes[0].options = SearchOptions(**{ # Devices
            'attribute_match' : ['name','model','serial'],
            'geo_fields'      : ['nominal_location']
        })
        indexes[1].options = SearchOptions(**{ # BankAccount
            'attribute_match' : ['name','description','owner'],
            'range_fields'    : ['cash_balance']
        })
        indexes[2].options = SearchOptions(**{ # Hybrid
            'attribute_match' : ['name'],
            'range_fields'    : ['arc'],
            'geo_fields'      : ['nominal_location']
        })

        index_ids = list([ self.rr_cli.create(i)[0] for i in indexes])

        devices_catalog_id = self.cms_cli.create_catalog('devices_catalog', ['model'])
        self.assertTrue(index_ids[0] in self.cms_cli.list_indexes(devices_catalog_id), 'The catalog did\'nt match the correct index.')

        accounts_catalog_id = self.cms_cli.create_catalog('accounts_catalog', ['cash_balance'])
        self.assertTrue(index_ids[1] in self.cms_cli.list_indexes(accounts_catalog_id), 'The catalog did\'nt match the correct index.')
        
        geo_catalog_id = self.cms_cli.create_catalog('geo_catalog', ['nominal_location'])
        self.assertTrue(index_ids[2] in self.cms_cli.list_indexes(geo_catalog_id), 'The catalog did\'nt match the correct index.')

        names_catalog_id = self.cms_cli.create_catalog('names_catalog', ['name'])
        names_list = self.cms_cli.list_indexes(names_catalog_id)
        self.assertTrue(set(index_ids) == set(names_list), 'The catalog did\'nt match the correct index.')

    def test_catalog_field_exclusion(self):
        half_match = Index(name='half_match', options=SearchOptions(attribute_match=['one']))
        full_match = Index(name='full_match', options=SearchOptions(attribute_match=['one','two']))

        half_match_id, _ = self.rr_cli.create(half_match)
        full_match_id, _ = self.rr_cli.create(full_match)

        catalog_id = self.cms_cli.create_catalog('test_cat', keywords=['one','two'])
        index_list = self.cms_cli.list_indexes(catalog_id, id_only=True)
        self.assertTrue(index_list == [full_match_id])



    def test_update_catalog(self):
        empty_catalog_id = self.cms_cli.create_catalog('empty_catalog')

        empty_catalog = self.rr_cli.read(empty_catalog_id)
        empty_catalog.description = 'testing update'
        self.cms_cli.update_catalog(empty_catalog)

        empty_catalog = self.rr_cli.read(empty_catalog_id)
        self.assertTrue(empty_catalog.description == 'testing update')

    def test_read_catalog(self):
        catalog_res = Catalog(name='fake_catalog')
        catalog_id, _ = self.rr_cli.create(catalog_res)
        
        cat = self.cms_cli.read_catalog(catalog_id)
        self.assertTrue(cat.name == 'fake_catalog')

    def test_delete_catalog(self):
        empty_catalog_id, _ = self.rr_cli.create(Catalog(name='empty_catalog'))

        self.rr_cli.read(empty_catalog_id) # Throws exception if it doesn't exist

        self.cms_cli.delete_catalog(empty_catalog_id)

        with self.assertRaises(NotFound):
            self.rr_cli.read(empty_catalog_id)

        with self.assertRaises(NotFound):
            self.cms_cli.read_catalog(empty_catalog_id)
