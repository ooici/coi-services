#!/usr/bin/env python
__license__ = 'Apache 2.0'
'''
@author Maurice Manning
@author Swarbhanu Chatterjee
@file ion/services/dm/ingestion/ingestion_management_service.py
@description Implementation for IngestionManagementService
'''
from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from pyon.core import bootstrap
from pyon.core.exception import NotFound
from pyon.public import RT, PRED, log, IonObject
from pyon.public import CFG
from pyon.core.exception import IonException
from interface.objects import ExchangeQuery, IngestionConfiguration, ProcessDefinition
from interface.objects import StreamIngestionPolicy, StreamPolicy
from pyon.event.event import StreamIngestionPolicyEventPublisher


from pyon.datastore.datastore import DataStore



class IngestionManagementServiceException(IonException):
    """
    Exception class for IngestionManagementService exceptions. This class inherits from IonException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


class IngestionManagementService(BaseIngestionManagementService):
    """
    id_p = cc.spawn_process('ingestion_worker', 'ion.services.dm.ingestion.ingestion_management_service', 'IngestionManagementService')
    cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()
    """

    base_exchange_name = 'ingestion_queue'

    def __init__(self):
        BaseIngestionManagementService.__init__(self)

        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = '.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)


    def on_start(self):
        super(IngestionManagementService,self).on_start()
        self.event_publisher = StreamIngestionPolicyEventPublisher(node = self.container.node)


        #########################################################################################################
        #   The code for process_definition may not really belong here, but we do not have a different way so
        #   far to preload the process definitions. This will later probably be part of a set of predefinitions
        #   for processes.
        #########################################################################################################
        self.process_definition = ProcessDefinition()
        self.process_definition.executable['module']='ion.processes.data.ingestion.ingestion_worker'
        self.process_definition.executable['class'] = 'IngestionWorker'
        self.process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=self.process_definition)

    def on_quit(self):
        self.clients.process_dispatcher.delete_process_definition(process_definition_id=self.process_definition_id)
        super(IngestionManagementService,self).on_quit()

    def create_ingestion_configuration(self, exchange_point_id='', couch_storage=None, hdf_storage=None,\
                                       number_of_workers=0, default_policy=None):
        """
        @brief Setup ingestion workers to ingest all the data from a single exchange point.
        @param exchange_point_id is the resource id for the exchagne point to ingest from
        @param couch_storage is the specification of the couch database to use
        @param hdf_storage is the specification of the filesystem to use for hdf data files
        @param number_of_workers is the number of ingestion workers to create
        @param default_policy is the default policy for streams ingested on this exchange point
        """


        # Give each ingestion configuration its own queue name to receive data on
        exchange_name = 'ingestion_queue'

        ##------------------------------------------------------------------------------------
        ## declare our intent to subscribe to all messages on the exchange point
        query = ExchangeQuery()

        subscription_id = self.clients.pubsub_management.create_subscription(query=query,\
            exchange_name=exchange_name, name='Ingestion subscription', description='Subscription for ingestion workers')

        ##------------------------------------------------------------------------------------------

        # create an ingestion_configuration instance and update the registry
        # @todo: right now sending in the exchange_point_id as the name...
        ingestion_configuration = IngestionConfiguration( name = self.XP)
        ingestion_configuration.description = '%s exchange point ingestion configuration' % self.XP
        ingestion_configuration.number_of_workers = number_of_workers

        if hdf_storage is not None:
            ingestion_configuration.hdf_storage.update(hdf_storage)

        if couch_storage is not None:
            ingestion_configuration.couch_storage.update(couch_storage)

        if default_policy is not None:
            ingestion_configuration.default_policy.update(default_policy)


        ingestion_configuration_id, _ = self.clients.resource_registry.create(ingestion_configuration)

        self._launch_transforms(
            ingestion_configuration.number_of_workers,
            subscription_id,
            ingestion_configuration_id,
            ingestion_configuration,
            self.process_definition_id
        )
        return ingestion_configuration_id

    def _launch_transforms(self, number_of_workers, subscription_id, ingestion_configuration_id, ingestion_configuration, process_definition_id):
        """
        This method spawns the two transform processes without activating them...Note: activating the transforms does the binding
        """

        description = 'Ingestion worker'

        # launch the transforms
        for i in xrange(number_of_workers):
            name = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, i+1)
            transform_id = self.clients.transform_management.create_transform(
                name = name,
                description = description,
                in_subscription_id= subscription_id,
                out_streams = {},
                process_definition_id=process_definition_id,
                configuration=ingestion_configuration) # The config is the ingestion configuration object!

            # create association between ingestion configuration and the transforms that act as Ingestion Workers
            if not transform_id:
                raise IngestionManagementServiceException('Transform could not be launched by ingestion.')
            self.clients.resource_registry.create_association(ingestion_configuration_id, PRED.hasTransform, transform_id)


    def update_ingestion_configuration(self, ingestion_configuration=None):
        """Change the number of workers or the default policy for ingesting data on each stream

        @param ingestion_configuration    IngestionConfiguration
        """
        log.debug("Updating ingestion configuration")
        id, rev = self.clients.resource_registry.update(ingestion_configuration)

    def read_ingestion_configuration(self, ingestion_configuration_id=''):
        """Get an existing ingestion configuration object.

        @param ingestion_configuration_id    str
        @retval ingestion_configuration    IngestionConfiguration
        @throws NotFound    if ingestion configuration did not exist
        """
        log.debug("Reading ingestion configuration object id: %s", ingestion_configuration_id)
        ingestion_configuration = self.clients.resource_registry.read(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %s does not exist" % ingestion_configuration_id)
        return ingestion_configuration

    def delete_ingestion_configuration(self, ingestion_configuration_id=''):
        """Delete an existing ingestion configuration object.

        @param ingestion_configuration_id    str
        @throws NotFound    if ingestion configuration did not exist
        """
        log.debug("Deleting ingestion configuration: %s", ingestion_configuration_id)


        #ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        #@todo Should we check to see if the ingestion configuration exists?

        #delete the transforms associated with the ingestion_configuration_id
        transform_ids = self.clients.resource_registry.find_objects(ingestion_configuration_id, PRED.hasTransform, RT.Transform, True)

        if len(transform_ids) < 1:
            raise NotFound('No transforms associated with this ingestion configuration!')

        log.debug('len(transform_ids): %s' % len(transform_ids))

        for transform_id in transform_ids:
            # To Delete - we need to actually remove each of the transforms
            self.clients.transform_management.delete_transform(transform_id)


        # delete the associations too...
        associations = self.clients.resource_registry.find_associations(ingestion_configuration_id,PRED.hasTransform)
        log.info('associations: %s' % associations)
        for association in associations:
            self.clients.resource_registry.delete_association(association)
            #@todo How should we deal with failure?


        self.clients.resource_registry.delete(ingestion_configuration_id)


    def activate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Activate an ingestion configuration and the transform processes that execute it

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """

        log.debug("Activating ingestion configuration")

        # check whether the ingestion configuration object exists
        #ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        #@todo Should we check to see if the ingestion configuration exists?

        # read the transforms
        transform_ids, _ = self.clients.resource_registry.find_objects(ingestion_configuration_id, PRED.hasTransform, RT.Transform, True)
        if len(transform_ids) < 1:
            raise NotFound('The ingestion configuration %s does not exist' % str(ingestion_configuration_id))

        # since all ingestion worker transforms have the same subscription, only deactivate one
        self.clients.transform_management.activate_transform(transform_ids[0])

        return True


    def deactivate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Deactivate one of the transform processes that uses an ingestion configuration

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """
        log.debug("Deactivating ingestion configuration")

        # check whether the ingestion configuration object exists
        #ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        #@todo Should we check to see if the ingestion configuration exists?


        # use the deactivate method in transformation management service
        transform_ids, _ = self.clients.resource_registry.find_objects(ingestion_configuration_id, PRED.hasTransform, RT.Transform, True)
        if len(transform_ids) < 1:
            raise NotFound('The ingestion configuration %s does not exist' % str(ingestion_configuration_id))

        # since all ingestion worker transforms have the same subscription, only deactivate one
        self.clients.transform_management.deactivate_transform(transform_ids[0])

        return True

    def create_stream_policy(self, stream_id='', archive_data=True, archive_metadata=True):
        """Create a policy for a particular stream and associate it to the ingestion configuration for the exchange point the stream is on. (After LCA)

        @param stream_id    str
        @param archive_data    str
        @param archive_metadata    str
        @retval stream_policy_id    str
        """

        if not stream_id:
            raise IngestionManagementServiceException('Must pass a stream id to create stream policy')

        log.debug("Creating stream policy")

        # Read the stream to get the stream definition
        stream = self.clients.pubsub_management.read_stream(stream_id=stream_id)

        #@todo - once we have an exchange point associaiton this might all make sense. For now just add it to the db for all configs
        resources, _ = self.clients.resource_registry.find_resources(RT.IngestionConfiguration, None, None, False)

        for ing_conf in resources:

            try:
                couch_storage = ing_conf.couch_storage
            except AttributeError:
                continue

            log.info('Adding stream definition for stream "%s" to ingestion database "%s"' % (stream_id, couch_storage.datastore_name))
            #@todo how do we get them to the right database?!?!
            db = self.container.datastore_manager.get_datastore(couch_storage.datastore_name, couch_storage.datastore_profile, self.CFG)

            db.create(stream.stream_definition)

            db.close()


        policy = StreamPolicy(  archive_data=archive_data,
                                archive_metadata=archive_metadata,
                                stream_id=stream_id)

        stream_policy = StreamIngestionPolicy(  name='',
                                                description='policy for %s' % stream_id,
                                                policy = policy)

        stream_policy_id, _ = self.clients.resource_registry.create(stream_policy)


        #Use the Exchang Point name (id?) as the origin for stream policy events
        #@todo Once XP is really a resource get the exchange_point id for the stream
        XP = self.XP

        self.event_publisher.create_and_publish_event(
            origin=XP,
            description = stream_policy.description,
            stream_id =stream_id,
            archive_data=archive_data,
            archive_metadata=archive_metadata,
            resource_id = stream_policy_id
            )


        return stream_policy_id

    def update_stream_policy(self, stream_policy=None):
        """Change the number of workers or the default policy for ingesting data on each stream (After LCA)

        @param stream_policy    Unknown
        @throws NotFound    if policy does not exist
        """

        log.info('stream policy to update: %s' % stream_policy)

        log.debug("Updating stream policy")
        stream_policy_id, rev = self.clients.resource_registry.update(stream_policy)

        self.event_publisher.create_and_publish_event(
            origin=self.XP,
            description = stream_policy.description,
            stream_id =stream_policy.policy.stream_id,
            archive_data=stream_policy.policy.archive_data,
            archive_metadata=stream_policy.policy.archive_metadata,
            resource_id = stream_policy_id
        )


    def read_stream_policy(self, stream_policy_id=''):
        """Get an existing stream policy object. (After LCA)

        @param stream_policy_id    str
        @retval stream_policy    StreamIngestionPolicy
        @throws NotFound    if stream policy does not exist
        """

        log.debug("Reading stream policy")
        stream_policy = self.clients.resource_registry.read(stream_policy_id)

        return stream_policy

    def delete_stream_policy(self, stream_policy_id=''):
        """Delete an existing stream policy object. (After LCA)

        @param stream_policy_id    str
        @throws NotFound    if stream_policy does not exist
        """

        stream_policy = self.clients.resource_registry.read(stream_policy_id)

        log.debug("Deleting stream policy")
        self.clients.resource_registry.delete(stream_policy_id)

        #@todo publish an event for deleting policy

        self.event_publisher.create_and_publish_event(
            origin=self.XP,
            description = 'delete stream_policy',
            stream_id =stream_policy.policy.stream_id,
            archive_data=stream_policy.policy.archive_data,
            archive_metadata=stream_policy.policy.archive_metadata,
            resource_id = stream_policy_id,
            deleted = True
        )
