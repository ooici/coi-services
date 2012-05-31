#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipreservation_management_service import BasePreservationManagementService
from interface.objects import PersistenceSystem, PersistentArchive, PersistenceType, PersistenceInstance, DataStore, DataStoreType
from pyon.public import RT, PRED, log
from pyon.core.exception import NotFound

class PreservationManagementService(BasePreservationManagementService):

    def create_couch_cluster(self, name='', description='', replicas=1, partitions=1):
        """Create a PersistenceSystem resource describing a couch cluster.

        @param name    str
        @param description    str
        @param replicas    int
        @param partitions    int
        @retval persistence_system_id    str
        """
        persistence_sys = PersistenceSystem(name=name, description=description, type=PersistenceType.COUCHDB)
        persistence_sys.defaults['replicas'] = replicas
        persistence_sys.defaults['partitions'] = partitions
        persistence_sys_id, rev = self.clients.resource_registry.create(persistence_sys)
        return persistence_sys_id

    def create_elastic_search_cluster(self, name='', description='', replicas=1, shards=1):
        """Create a PersistenceSystem resource describing a couch cluster.

        @param name    str
        @param description    str
        @param replicas    int
        @param shards    int
        @retval persistence_system_id    str
        """
        persistence_sys = PersistenceSystem(name=name, description=description, type=PersistenceType.ELASTICSEARCH)
        persistence_sys.defaults['replicas'] = replicas
        persistence_sys.defaults['shards'] = shards
        persistence_sys_id, rev = self.clients.resource_registry.create(persistence_sys)
        return persistence_sys_id

    def create_compellent_cluster(self, name='', description='', replicas=1):
        """Create a PersistenceSystem resource describing a compellent cluster

        @param name    str
        @param description    str
        @param replicas    int
        @retval persistence_system_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        persistence_sys = PersistenceSystem(name=name, description=description, type=PersistenceType.COMPELLANT)
        persistence_sys.defaults['replicas'] = replicas
        persistence_sys_id, rev = self.clients.resource_registry.create(persistence_sys)
        return persistence_sys_id

    def read_persistence_system(self, persistence_system_id=''):
        """read a PersistenceSystem resource from the resource registry

        @param persistence_system_id    str
        @retval persistence_system    PersistenceSystem
        @throws NotFound    object with specified id does not exist
        """
        persistence_system = self.clients.resource_registry.read(persistence_system_id)
        if persistence_system is None:
            raise NotFound("PersistenceSystem %s does not exist" % persistence_system_id)
        return persistence_system

    def delete_persistence_system(self, persistence_system_id=''):
        """delete a PersistenceSystem resource from the resource registry

        @param persistence_system_id    str
        @throws NotFound    object with specified id does not exist
        """
        persistence_system = self.clients.resource_registry.read(persistence_system_id)
        if persistence_system is None:
            raise NotFound("PersistenceSystem %s does not exist" % persistence_system_id)

        self.clients.resource_registry.delete(persistence_system_id)

    def create_couch_instance(self, name='', description='', host='', port=5984, username='', password='', file_system_datastore_id='', persistence_system_id='', config=None):
        """Create an Persistence Instance resource describing a couch instance.

        @param name    str
        @param description    str
        @param host    str
        @param port    int
        @param username    str
        @param password    str
        @param file_system_datastore_id    str
        @param persistence_system_id    str
        @param config    IngestionConfiguration
        @retval persistence_instance_id    str
        """
        persistence_instance = PersistenceInstance(name=name, description=description, type=PersistenceType.COUCHDB, host=host, port=port, username=username, password=password)
        if not config is None:
            persistence_instance.config.update(config)
        persistence_instance_id, rev = self.clients.resource_registry.create(persistence_instance)
        log.debug(persistence_system_id)

        if file_system_datastore_id != '':
            self.clients.resource_registry.create_association(persistence_instance_id, PRED.hasDatastore, file_system_datastore_id)

        if persistence_system_id != '':
            self.clients.resource_registry.create_association(persistence_system_id, PRED.hasPersistenceInstance, persistence_instance_id)

        return persistence_instance_id

    def create_elastic_search_instance(self, name='', description='', host='', posrt=9200, username='', password='', file_system_datastore_id='', persistence_system_id='', config=None):
        """Create an Persistence Instance resource describing a couch instance.

        @param name    str
        @param description    str
        @param host    str
        @param posrt    int
        @param username    str
        @param password    str
        @param file_system_datastore_id    str
        @param persistence_system_id    str
        @param config    IngestionConfiguration
        @retval persistence_instance_id    str
        """
        persistence_instance = PersistenceInstance(name=name, description=description, type=PersistenceType.ELASTICSEARCH, host=host, port=posrt, username=username, password=password)
        if not config is None:
            persistence_instance.config.update(config)
        persistence_instance_id, rev = self.clients.resource_registry.create(persistence_instance)

        if file_system_datastore_id != '':
            self.clients.resource_registry.create_association(persistence_instance_id, PRED.hasDatastore, file_system_datastore_id)

        if persistence_system_id != '':
            self.clients.resource_registry.create_association(persistence_system_id, PRED.hasPersistenceInstance, persistence_instance_id)

        return persistence_instance_id

    def read_persistence_instance(self, persistence_instance_id=''):
        """Read a PersistenceInstance resource from the resource registry

        @param persistence_instance_id    str
        @retval persistence_instance    PersistenceInstance
        @throws NotFound    resource with specified id does not exist
        """
        persistence_instance = self.clients.resource_registry.read(persistence_instance_id)
        if persistence_instance is None:
            raise NotFound("PersistenceInstance %s does not exist" % persistence_instance_id)
        return persistence_instance

    def delete_persistence_instance(self, persistence_instance_id=''):
        """delete a PersistenceInstance resource from the resource registry
           remove any associations with the PersistenceInstance (Datastores or PersistenceSystems)

        @param persistence_instance_id    str
        @throws NotFound    resource with specified id does not exist
        """
        persistence_instance = self.clients.resource_registry.read(persistence_instance_id)
        if persistence_instance is None:
            raise NotFound("PersistenceInstance %s does not exist" % persistence_instance_id)

        assocs = self.clients.resource_registry.find_associations(subject=persistence_instance_id, assoc_type=PRED.hasDatastore)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with Datastore

        assocs = self.clients.resource_registry.find_associations(predicate=persistence_instance_id, assoc_type=PRED.hasPersistenceInstance)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with PersistenceSystem

        self.clients.resource_registry.delete(persistence_instance_id)

    def create_couch_datastore(self, name='', description='', persistence_system_id='', namespace='', replicas=1, partitions=1):
        """Create a couch datastore

        @param name    str
        @param description    str
        @param persistence_system_id    str
        @param namespace    str
        @param replicas    int
        @param partitions    int
        @retval datastore_id    str
        """
        datastore = DataStore(name=name, description=description, type=DataStoreType.COUCHDB, namespace=namespace)
        datastore.config['replicas'] = replicas
        datastore.config['partitions'] = partitions
        data_store_id, rev = self.clients.resource_registry.create(datastore)

        if persistence_system_id != '':
            self.clients.resource_registry.create_association(persistence_system_id, PRED.hasDatastore, data_store_id)

        return data_store_id

    def create_elastic_search_datastore(self, name='', description='', persistence_system_id='', namespace='', replicas=1, shards=1):
        """Create an elastic seach datastore

        @param name    str
        @param description    str
        @param persistence_system_id    str
        @param namespace    str
        @param replicas    int
        @param shards    int
        @retval datastore_id    str
        """
        datastore = DataStore(name=name, description=description, type=DataStoreType.ELASTICSEARCH, namespace=namespace)
        datastore.config['replicas'] = replicas
        datastore.config['shards'] = shards
        data_store_id, rev = self.clients.resource_registry.create(datastore)

        if persistence_system_id != '':
            self.clients.resource_registry.create_association(persistence_system_id, PRED.hasDatastore, data_store_id)

        return data_store_id

    def create_file_system_datastore(self, name='', description='', persistent_archive_id='', persistence_system_id='', namespace='', replicas=1):
        """Create a file system datastore

        @param name    str
        @param description    str
        @param persistent_archive_id    str
        @param persistence_system_id    str
        @param namespace    str
        @param replicas    int
        @retval datastore_id    str
        """
        datastore = DataStore(name=name, description=description, type=DataStoreType.FILESYSTEM, namespace=namespace)
        datastore.config['replicas'] = replicas
        data_store_id, rev = self.clients.resource_registry.create(datastore)

        if persistence_system_id != '':
            self.clients.resource_registry.create_association(subject=persistence_system_id, predicate=PRED.hasDatastore, object=data_store_id)

        if persistent_archive_id != '':
            self.clients.resource_registry.create_association(subject=data_store_id, predicate=PRED.hasArchive, object=persistent_archive_id)

        return data_store_id

    def read_datastore(self, datastore_id=''):
        """read a datastore resource from the resource registry

        @param datastore_id    str
        @retval datastore    DataStore
        @throws NotFound    object with specified id does not exist
        """
        datastore = self.clients.resource_registry.read(datastore_id)
        if datastore is None:
            raise NotFound("Datastore %s does not exist" % datastore_id)

        return datastore

    def delete_datastore(self, datastore_id=''):
        """delete a datastore resource from the resource registry
           remove any associations with the datastore (PersistenceSystems or PersistenceInstances)

        @param datastore_id    str
        @throws NotFound    object with specified id does not exist
        """
        datastore = self.clients.resource_registry.read(datastore_id)
        if datastore is None:
            raise NotFound("Datastore %s does not exist" % datastore_id)

        assocs = self.clients.resource_registry.find_associations(subject=datastore_id, assoc_type=PRED.hasDatastore)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with PersistenceSystem

        self.clients.resource_registry.delete(datastore_id)

    def create_persistent_archive(self, name='', description='', size=0, device='', vfstype='', label=''):
        """Create a Persistent Archive resource describing the archive and its content.

        @param name    str
        @param description    str
        @param size    int
        @param device    str
        @param vfstype    str
        @param label    str
        @retval archive_id    str
        @throws BadRequest    Invalid arguments
        """
        persistent_archive = PersistentArchive(name=name, description=description, size=size, device=device, vfstype=vfstype, label=label)
        persistent_archive_id, rev = self.clients.resource_registry.create(persistent_archive)
        return persistent_archive_id

    def replicate_persistent_archive(self, existing_archive_id=None, device='', vfstype='', label=''):
        """replicate an existing persistent archive and create a new one

        @param existing_archive_id    NoneType
        @param device    str
        @param vfstype    str
        @param label    str
        """
        new_persistent_archive = self.clients.resource_registry.read(existing_archive_id)
        if not new_persistent_archive is None:
            new_persistent_archive.device = device
            new_persistent_archive.vfstype = vfstype
            new_persistent_archive.label = label
            new_persistent_archive_id, rev = self.clients.resource_registry.create(new_persistent_archive)
            return new_persistent_archive_id
        else:
            raise NotFound("PersistentArchive %s does not exist" % existing_archive_id)

    def read_persistent_archive(self, persistent_archive_id=''):
        """read a persistent archive resource from the resource registry

        @param persistent_archive_id    str
        @retval persistent_archive    PersistentArchive
        @throws NotFound    object with specified id does not exist
        """
        persistent_archive = self.clients.resource_registry.read(persistent_archive_id)
        if persistent_archive is None:
            raise NotFound("PersistentArchive %s does not exist" % persistent_archive_id)

        return persistent_archive

    def delete_persistent_archive(self, persistent_archive_id=''):
        """delete a persistent archive resource from the resource registry

        @param persistent_archive_id    str
        @throws NotFound    object with specified id does not exist
        """
        persistent_archive = self.clients.resource_registry.read(persistent_archive_id)
        if persistent_archive is None:
            raise NotFound("PersistentArchive %s does not exist" % persistent_archive_id)

        self.clients.resource_registry.delete(persistent_archive_id)
