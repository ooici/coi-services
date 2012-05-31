#!/usr/bin/env python

'''
@file ion/services/dm/distribution/test/test_preservation_management_service.py
@author Tim Giguere
@test ion.services.dm.preservation.preservation_management_service Unit test suite to cover all pub sub mgmt service code
'''
from pyon.public import PRED, RT
from mock import Mock, call
from interface.services.dm.ipreservation_management_service import PreservationManagementServiceClient
from ion.services.dm.preservation.preservation_management_service import PreservationManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.util.containers import DotDict
from nose.plugins.attrib import attr
import unittest
from interface.objects import PersistenceInstance, PersistenceSystem, DataStore


@attr('UNIT', group='dm_pres')
class PreservationManagementServiceTest(PyonTestCase):

    def setUp(self):
        mock_clients = self._create_service_mock('preservation_management')
        self.pres_man_service = PreservationManagementService()
        self.pres_man_service.clients = mock_clients
        self.pres_man_service.container = DotDict()
        self.pres_man_service.container.node = Mock()

        # save some typing
        self.mock_create = mock_clients.resource_registry.create
        self.mock_update = mock_clients.resource_registry.update
        self.mock_delete = mock_clients.resource_registry.delete
        self.mock_read = mock_clients.resource_registry.read
        self.mock_create_association = mock_clients.resource_registry.create_association
        self.mock_delete_association = mock_clients.resource_registry.delete_association
        self.mock_find_resources = mock_clients.resource_registry.find_resources
        self.mock_find_associations = mock_clients.resource_registry.find_associations
        self.mock_find_objects = mock_clients.resource_registry.find_objects

        #Couch PersistenceSystem
        self.couch_cluster_id = "couch_cluster_id"
        self.couch_cluster = Mock()
        self.couch_cluster.name = "CouchCluster"
        self.couch_cluster.description = "Sample CouchCluster"
        self.couch_cluster.defaults = {'replicas' : 1, 'partitions' : 1}

        #ElasticSearch PersistenceSystem
        self.elastic_search_cluster_id = "elastic_search_cluster_id"
        self.elastic_search_cluster = Mock()
        self.elastic_search_cluster.name = "ElasticSearchCluster"
        self.elastic_search_cluster.description = "Sample ElasticSearchCluster"
        self.elastic_search_cluster.defaults = {'replicas' : 1, 'shards' : 1}

        #Compellent PersistenceSystem
        self.compellent_cluster_id = "compellent_cluster_id"
        self.compellent_cluster = Mock()
        self.compellent_cluster.name = "CompellentSearchCluster"
        self.compellent_cluster.description = "Sample CompellentSearchCluster"
        self.compellent_cluster.defaults = {'replicas' : 1}

        #Couch PersistenceInstance
        self.couch_instance_id = 'couch_instance_id'
        self.couch_instance = Mock()
        self.couch_instance.name = 'CouchInstance'
        self.couch_instance.description = 'Sample CouchInstance'
        self.couch_instance.host = 'couch_host'
        self.couch_instance.port = 5984
        self.couch_instance.username = 'couch_user'
        self.couch_instance.password = 'couch_pass'
        self.couch_instance.config = {'sample' : 'SampleConfig'}

        #ElasticSearch PersistenceInstance
        self.elastic_search_instance_id = 'elastic_search_instance_id'
        self.elastic_search_instance = Mock()
        self.elastic_search_instance.name = 'ElasticSearchInstance'
        self.elastic_search_instance.description = 'Sample ElasticSearchInstance'
        self.elastic_search_instance.host = 'elastic_search_host'
        self.elastic_search_instance.port = 9200
        self.elastic_search_instance.username = 'elastic_search_user'
        self.elastic_search_instance.password = 'elastic_search_pass'
        self.elastic_search_instance.config = {'sample' : 'SampleConfig'}

        #FileSystemDatastore
        self.file_system_datastore_id = 'file_system_datastore_id'
        self.file_system_datastore = Mock()
        self.file_system_datastore.name = 'FileSystemDatastore'
        self.file_system_datastore.description = 'Sample FileSystemDatastore'
        self.file_system_datastore.namespace = 'FileSystemDatastore.Namespace'
        self.file_system_datastore.config = {'replicas' : 1}

        #ElasticSearch Datastore
        self.elastic_search_datastore_id = 'elastic_search_datastore_id'
        self.elastic_search_datastore = Mock()
        self.elastic_search_datastore.name = "ElasticSearchDatastore"
        self.elastic_search_datastore.description = "Sample ElasticSearchDatastore"
        self.elastic_search_datastore.namespace = 'ElasticSearchDatastore Namespace'
        self.elastic_search_datastore.config = {'replicas' : 1, 'shards' : 1}

        #Couch Datastore
        self.couch_datastore_id = 'couch_datastore_id'
        self.couch_datastore = Mock()
        self.couch_datastore.name = "CouchDatastore"
        self.couch_datastore.description = "Sample CouchDatastore"
        self.couch_datastore.namespace = 'CouchDatastore Namespace'
        self.couch_datastore.config = {'replicas' : 1, 'partitions' : 1}

        #PersistentArchive
        self.archive_id = 'archive_id'
        self.archive = Mock()
        self.archive.name = 'Archive'
        self.archive.description = 'Sample Archive'
        self.archive.size = 0
        self.archive.device = 'Archive Device'
        self.archive.vfstype = 'Archive FSType'
        self.archive.label = 'Archive Label'

        #PersistenceInstance Has DataStore DataStore
        self.instance_has_datastore_id = "instance_has_datastore_id"
        self.instance_has_datastore = Mock()
        self.instance_has_datastore._id = self.instance_has_datastore_id

        #PersistenceSystem Has PersistenceInstance
        self.system_has_instance_id = 'system_has_instance_id'
        self.system_has_instance = Mock()
        self.system_has_instance._id = self.system_has_instance_id

        #PersistenceSystem has DataStore
        self.system_has_datastore_id = 'system_has_datastore_id'
        self.system_has_datastore = Mock()
        self.system_has_datastore._id = self.system_has_datastore_id

        #DataStore has Archive
        self.datastore_has_archive_id = 'datastore_has_archive_id'
        self.datastore_has_archive = Mock()
        self.datastore_has_archive._id = self.datastore_has_archive_id

    def test_create_couch_cluster(self):
        self.mock_create.return_value = [self.couch_cluster_id, 1]

        couch_cluster_id = self.pres_man_service.create_couch_cluster(name=self.couch_cluster.name,
            description=self.couch_cluster.description,
            replicas=self.couch_cluster.defaults['replicas'],
            partitions=self.couch_cluster.defaults['partitions'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(couch_cluster_id, self.couch_cluster_id)

    def test_create_elastic_search_cluster(self):
        self.mock_create.return_value = [self.elastic_search_cluster_id, 1]

        elastic_search_cluster_id = self.pres_man_service.create_elastic_search_cluster(name=self.elastic_search_cluster.name,
            description=self.elastic_search_cluster.description,
            replicas=self.elastic_search_cluster.defaults['replicas'],
            shards=self.elastic_search_cluster.defaults['shards'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(elastic_search_cluster_id, self.elastic_search_cluster_id)

    def test_create_compellent_cluster(self):
        self.mock_create.return_value = [self.compellent_cluster_id, 1]

        compellent_cluster_id = self.pres_man_service.create_compellent_cluster(name=self.compellent_cluster.name,
            description=self.compellent_cluster.description,
            replicas=self.compellent_cluster.defaults['replicas'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(compellent_cluster_id, self.compellent_cluster_id)

    def test_read_persistence_system(self):
        self.mock_read.return_value = self.couch_cluster
        couch_cluster_obj = self.pres_man_service.read_persistence_system(self.couch_cluster_id)

        assert couch_cluster_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.couch_cluster_id, '')

    def test_read_persistence_system_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.read_persistence_system(persistence_system_id='notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistenceSystem notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_persistence_system(self):
        self.mock_read.return_value = self.couch_cluster

        self.pres_man_service.delete_persistence_system(persistence_system_id=self.couch_cluster_id)

        self.mock_read.assert_called_once_with(self.couch_cluster_id, '')
        self.mock_delete.assert_called_once_with(self.couch_cluster_id)

    def test_delete_persistence_system_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.delete_persistence_system('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistenceSystem notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_create_couch_instance_no_associations(self):
        self.mock_create.return_value = [self.couch_instance_id, 1]

        couch_instance_id = self.pres_man_service.create_couch_instance(name=self.couch_instance.name,
            description=self.couch_instance.description,
            host=self.couch_instance.host,
            port=self.couch_instance.port,
            username=self.couch_instance.username,
            password=self.couch_instance.password)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(couch_instance_id, self.couch_instance_id)

    def test_create_couch_instance_with_file_system_datastore(self):
        self.mock_create.return_value = [self.couch_instance_id, 1]

        couch_instance_id = self.pres_man_service.create_couch_instance(name=self.couch_instance.name,
            description=self.couch_instance.description,
            host=self.couch_instance.host,
            port=self.couch_instance.port,
            username=self.couch_instance.username,
            password=self.couch_instance.password,
            file_system_datastore_id=self.file_system_datastore_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.couch_instance_id, PRED.hasDatastore, self.file_system_datastore_id, None)
        self.assertEqual(couch_instance_id, self.couch_instance_id)

    def test_create_couch_instance_with_persistence_system(self):
        self.mock_create.return_value = [self.couch_instance_id, 1]

        couch_instance_id = self.pres_man_service.create_couch_instance(name=self.couch_instance.name,
            description=self.couch_instance.description,
            host=self.couch_instance.host,
            port=self.couch_instance.port,
            username=self.couch_instance.username,
            password=self.couch_instance.password,
            persistence_system_id=self.couch_cluster_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.couch_cluster_id, PRED.hasPersistenceInstance, self.couch_instance_id, None)
        self.assertEqual(couch_instance_id, self.couch_instance_id)

    def test_create_couch_instance_with_config(self):
        self.mock_create.return_value = [self.couch_instance_id, 1]

        couch_instance_id = self.pres_man_service.create_couch_instance(name=self.couch_instance.name,
            description=self.couch_instance.description,
            host=self.couch_instance.host,
            port=self.couch_instance.port,
            username=self.couch_instance.username,
            password=self.couch_instance.password,
            config=self.couch_instance.config)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(couch_instance_id, self.couch_instance_id)

    def test_create_elastic_search_instance_no_associations(self):
        self.mock_create.return_value = [self.elastic_search_instance_id, 1]

        elastic_search_instance_id = self.pres_man_service.create_elastic_search_instance(name=self.elastic_search_instance.name,
            description=self.elastic_search_instance.description,
            host=self.elastic_search_instance.host,
            posrt=self.elastic_search_instance.port,
            username=self.elastic_search_instance.username,
            password=self.elastic_search_instance.password)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(elastic_search_instance_id, self.elastic_search_instance_id)

    def test_create_elastic_search_instance_with_file_system_datastore(self):
        self.mock_create.return_value = [self.elastic_search_instance_id, 1]

        elastic_search_instance_id = self.pres_man_service.create_elastic_search_instance(name=self.elastic_search_instance.name,
            description=self.elastic_search_instance.description,
            host=self.elastic_search_instance.host,
            posrt=self.elastic_search_instance.port,
            username=self.elastic_search_instance.username,
            password=self.elastic_search_instance.password,
            file_system_datastore_id=self.file_system_datastore_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.elastic_search_instance_id, PRED.hasDatastore, self.file_system_datastore_id, None)
        self.assertEqual(elastic_search_instance_id, self.elastic_search_instance_id)

    def test_create_elastic_search_instance_with_persistence_system(self):
        self.mock_create.return_value = [self.elastic_search_instance_id, 1]

        elastic_search_instance_id = self.pres_man_service.create_elastic_search_instance(name=self.elastic_search_instance.name,
            description=self.elastic_search_instance.description,
            host=self.elastic_search_instance.host,
            posrt=self.elastic_search_instance.port,
            username=self.elastic_search_instance.username,
            password=self.elastic_search_instance.password,
            persistence_system_id=self.elastic_search_cluster_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.elastic_search_cluster_id, PRED.hasPersistenceInstance, self.elastic_search_instance_id, None)
        self.assertEqual(elastic_search_instance_id, self.elastic_search_instance_id)

    def test_create_elastic_search_instance_with_config(self):
        self.mock_create.return_value = [self.elastic_search_instance_id, 1]

        elastic_search_instance_id = self.pres_man_service.create_elastic_search_instance(name=self.elastic_search_instance.name,
            description=self.elastic_search_instance.description,
            host=self.elastic_search_instance.host,
            posrt=self.elastic_search_instance.port,
            username=self.elastic_search_instance.username,
            password=self.elastic_search_instance.password,
            config=self.elastic_search_instance.config)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(elastic_search_instance_id, self.elastic_search_instance_id)

    def test_read_persistence_instance(self):
        self.mock_read.return_value = self.couch_instance
        couch_instance_obj = self.pres_man_service.read_persistence_instance(self.couch_instance_id)

        assert couch_instance_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.couch_instance_id, '')

    def test_read_persistence_instance_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.read_persistence_instance(persistence_instance_id='notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistenceInstance notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_persistence_instance(self):
        self.mock_read.return_value = self.couch_instance
        returns = [[self.instance_has_datastore], [self.system_has_instance]]
        def side_effect(*args):
            result = returns.pop(0)
            return result

        self.mock_find_associations.side_effect = side_effect

        self.pres_man_service.delete_persistence_instance(persistence_instance_id=self.couch_instance_id)

        self.mock_read.assert_called_once_with(self.couch_instance_id, '')
        self.assertEqual(self.mock_find_associations.call_count, 2)
        expected = [call(self.couch_instance_id, '', '', PRED.hasDatastore, False), call('', self.couch_instance_id, '', PRED.hasPersistenceInstance, False)]
        self.assertEqual(expected, self.mock_find_associations.call_args_list)
        self.assertEqual(self.mock_delete_association.call_count, 2)
        expected = [call(self.instance_has_datastore_id), call(self.system_has_instance_id)]
        self.assertEqual(expected, self.mock_delete_association.call_args_list)
        self.mock_delete.assert_called_once_with(self.couch_instance_id)

    def test_delete_persistence_instance_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.delete_persistence_instance('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistenceInstance notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_create_couch_datastore_no_association(self):
        self.mock_create.return_value = [self.couch_datastore_id, 1]

        couch_datastore_id = self.pres_man_service.create_couch_datastore(name=self.couch_datastore.name,
            description=self.couch_datastore.description,
            namespace=self.couch_datastore.namespace,
            replicas=self.couch_datastore.config['replicas'],
            partitions=self.couch_datastore.config['partitions'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(couch_datastore_id, self.couch_datastore_id)

    def test_create_couch_datastore_with_persistence_system(self):
        self.mock_create.return_value = [self.couch_datastore_id, 1]

        couch_datastore_id = self.pres_man_service.create_couch_datastore(name=self.couch_datastore.name,
            description=self.couch_datastore.description,
            namespace=self.couch_datastore.namespace,
            replicas=self.couch_datastore.config['replicas'],
            partitions=self.couch_datastore.config['partitions'],
            persistence_system_id=self.couch_cluster_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.couch_cluster_id, PRED.hasDatastore, self.couch_datastore_id, None)
        self.assertEqual(couch_datastore_id, self.couch_datastore_id)

    def test_create_elastic_search_datastore_no_association(self):
        self.mock_create.return_value = [self.elastic_search_datastore_id, 1]

        elastic_search_datastore_id = self.pres_man_service.create_elastic_search_datastore(name=self.elastic_search_datastore.name,
            description=self.elastic_search_datastore.description,
            namespace=self.elastic_search_datastore.namespace,
            replicas=self.elastic_search_datastore.config['replicas'],
            shards=self.elastic_search_datastore.config['shards'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(elastic_search_datastore_id, self.elastic_search_datastore_id)

    def test_create_elastic_search_datastore_with_persistence_system(self):
        self.mock_create.return_value = [self.elastic_search_datastore_id, 1]

        elastic_search_datastore_id = self.pres_man_service.create_elastic_search_datastore(name=self.elastic_search_datastore.name,
            description=self.elastic_search_datastore.description,
            namespace=self.elastic_search_datastore.namespace,
            replicas=self.elastic_search_datastore.config['replicas'],
            shards=self.elastic_search_datastore.config['shards'],
            persistence_system_id=self.elastic_search_cluster_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.elastic_search_cluster_id, PRED.hasDatastore, self.elastic_search_datastore_id, None)
        self.assertEqual(elastic_search_datastore_id, self.elastic_search_datastore_id)

    def test_create_file_system_datastore_no_association(self):
        self.mock_create.return_value = [self.file_system_datastore_id, 1]

        file_system_datastore_id = self.pres_man_service.create_file_system_datastore(name=self.file_system_datastore.name,
            description=self.file_system_datastore.description,
            namespace=self.file_system_datastore.namespace,
            replicas=self.file_system_datastore.config['replicas'])

        self.assertTrue(self.mock_create.called)
        self.assertEqual(file_system_datastore_id, self.file_system_datastore_id)

    def test_create_file_system_datastore_with_persistence_system(self):
        self.mock_create.return_value = [self.file_system_datastore_id, 1]

        file_system_datastore_id = self.pres_man_service.create_file_system_datastore(name=self.file_system_datastore.name,
            description=self.file_system_datastore.description,
            namespace=self.file_system_datastore.namespace,
            replicas=self.file_system_datastore.config['replicas'],
            persistence_system_id=self.couch_cluster_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.couch_cluster_id, PRED.hasDatastore, self.file_system_datastore_id, None)
        self.assertEqual(file_system_datastore_id, self.file_system_datastore_id)

    def test_create_file_system_datastore_with_persistent_archive(self):
        self.mock_create.return_value = [self.file_system_datastore_id, 1]

        file_system_datastore_id = self.pres_man_service.create_file_system_datastore(name=self.file_system_datastore.name,
            description=self.file_system_datastore.description,
            namespace=self.file_system_datastore.namespace,
            replicas=self.file_system_datastore.config['replicas'],
            persistent_archive_id=self.archive_id)

        self.assertTrue(self.mock_create.called)
        self.mock_create_association.assert_called_once_with(self.file_system_datastore_id, PRED.hasArchive, self.archive_id, None)
        self.assertEqual(file_system_datastore_id, self.file_system_datastore_id)

    def test_read_datastore(self):
        self.mock_read.return_value = self.couch_datastore
        couch_datastore_obj = self.pres_man_service.read_datastore(self.couch_datastore_id)

        assert couch_datastore_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.couch_datastore_id, '')

    def test_read_datastore_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.read_datastore('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Datastore notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_datastore(self):
        self.mock_read.return_value = self.couch_datastore

        self.mock_find_associations.return_value = [self.system_has_datastore, self.datastore_has_archive]

        self.pres_man_service.delete_datastore(datastore_id=self.couch_datastore_id)

        self.mock_read.assert_called_once_with(self.couch_datastore_id, '')
        self.assertEqual(self.mock_find_associations.call_count, 1)
        expected = [call(self.couch_datastore_id, '', '', PRED.hasDatastore, False)]
        self.assertEqual(expected, self.mock_find_associations.call_args_list)
        self.assertEqual(self.mock_delete_association.call_count, 2)
        expected = [call(self.system_has_datastore_id), call(self.datastore_has_archive_id)]
        self.assertEqual(expected, self.mock_delete_association.call_args_list)
        self.mock_delete.assert_called_once_with(self.couch_datastore_id)

    def test_delete_datastore_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.delete_datastore('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'Datastore notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)

    def test_create_persistent_archive(self):
        self.mock_create.return_value = [self.archive_id, 1]

        archive_id = self.pres_man_service.create_persistent_archive(name=self.archive.name,
            description=self.archive.description,
            size=self.archive.size,
            device=self.archive.device,
            vfstype=self.archive.vfstype,
            label=self.archive.label)

        self.assertTrue(self.mock_create.called)
        self.assertEqual(archive_id, self.archive_id)

    def test_replicate_persistent_archive(self):
        self.mock_read.return_value = self.archive
        new_archive_id = 'new_archive_id'
        self.mock_create.return_value = [new_archive_id, 1]

        archive_id = self.pres_man_service.replicate_persistent_archive(existing_archive_id=self.archive_id,
            device='NewDevice',
            vfstype='NewVFSType',
            label='NewLabel')

        self.assertEqual(archive_id, new_archive_id)
        self.mock_read.assert_called_once_with(self.archive_id, '')
        self.assertTrue(self.mock_create.called)

    def test_replicate_persistent_archive_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.replicate_persistent_archive('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistentArchive notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_create.call_count, 0)

    def test_read_persistent_archive(self):
        self.mock_read.return_value = self.archive
        archive_obj = self.pres_man_service.read_persistent_archive(self.archive_id)

        assert archive_obj is self.mock_read.return_value
        self.mock_read.assert_called_once_with(self.archive_id, '')

    def test_read_persistent_archive_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.read_persistent_archive('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistentArchive notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')

    def test_delete_persistent_archive(self):
        self.mock_read.return_value = self.archive

        self.pres_man_service.delete_persistent_archive(self.archive_id)

        self.mock_read.assert_called_once_with(self.archive_id, '')
        self.mock_delete.assert_called_once_with(self.archive_id)

    def test_delete_persistent_archive_not_found(self):
        self.mock_read.return_value = None

        # TEST: Execute the service operation call
        with self.assertRaises(NotFound) as cm:
            self.pres_man_service.delete_persistent_archive('notfound')

        ex = cm.exception
        self.assertEqual(ex.message, 'PersistentArchive notfound does not exist')
        self.mock_read.assert_called_once_with('notfound', '')
        self.assertEqual(self.mock_delete.call_count, 0)