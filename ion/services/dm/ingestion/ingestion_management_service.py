#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from pyon.core.exception import NotFound
from pyon.public import RT, AT, log, IonObject
from pyon.public import CFG, StreamProcess
from pyon.ion.endpoint import ProcessPublisher
from pyon.net.channel import SubscriberChannel
from pyon.container.procs import ProcManager
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

class IngestionManagementService(BaseIngestionManagementService):
    """
    id_p = cc.spawn_process('ingestion_worker', 'ion.services.dm.ingestion.ingestion_management_service', 'IngestionManagementService')
    cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()
    """

    def __init__(self):
        BaseIngestionManagementService.__init__(self)

    def on_start(self):
        """
        This code may not really belong here, but we do not have a different way so far to preload the
        process definitions. This will later probably be part of a set of predefinitions for processes.
        """
         # set up process definition
        process_definition = IonObject(RT.ProcessDefinition, name='ingestion_example')
        process_definition.executable = {'module': 'ion.services.dm.ingestion.ingestion_example', 'class':'IngestionExample'}
        self.process_definition_id, _ = self.clients.resource_registry.create(process_definition)

    def create_ingestion_configuration(self, exchange_point_id='', couch_storage={}, hfd_storage={}, \
                                       number_of_workers=0, default_policy={}):
        """Setup ingestion workers to ingest all the data from a single exchange point.

        @param exchange_point_id    str
        @param couch_storage    Unknown
        @param hfd_storage    Unknown
        @param number_of_workers    int
        @param default_policy    Unknown
        @retval ingestion_configuration_id    str
        """

        # Get Exchange Point name from exchange_point_id
        ## Exchange points don't exist yet - use a hard coded name
        XP = 'science_data' #CFG.exchange_spaces.ioncore.exchange_points.science_data.name

        exchange_name = XP + '_ingestion_queue'


        ##------------------------------------------------------------------------------------
        ## For testing until changes to to pubsub are finished to subscribe to star...

        # create a stream
        # ProcManager.spawn_process(name='aStream', module=None, cls=None, config=None, process_type=RT.Stream)

        # this should be removed tomorrow
        input_stream = IonObject(RT.Stream,name='input_stream')
        input_stream.original = True
        input_stream.mimetype = 'hdf'
        input_stream_id = self.clients.pubsub_management.create_stream(input_stream)

#        # subscribe to that stream

        subscription_obj = IonObject(RT.Subscription, name = "subscription", description = "input subscription")
        subscription_obj.exchange_name = exchange_name
        subscription_obj.query['stream_id'] = input_stream_id
        # Call pubsub management to create a subscription for the exchange name
        subscription_id = self.clients.pubsub_management.create_subscription(subscription_obj)

        ## open Rabbitmq control and create a binding to *!!!

        ##------------------------------------------------------------------------------------------

        # create an ingestion_configuration instance and update the registry
        # @todo: right now sending in the exchange_point_id as the name...
        ingestion_configuration = IonObject(RT.IngestionConfiguration, name = 'ingestion_configuration')
        ingestion_configuration.number_of_workers = number_of_workers
        ingestion_configuration.hfd_storage = hfd_storage
        ingestion_configuration.couch_storage = couch_storage
        ingestion_configuration.default_policy = default_policy

        ingestion_configuration_id, rev = self.clients.resource_registry.create(ingestion_configuration)

        # Launch the transforms!

        # @todo: Check whether the correct listen_name is being passed

        self._launch_transforms(ingestion_configuration.number_of_workers, subscription_id, ingestion_configuration_id)

        return ingestion_configuration_id

    def _launch_transforms(self, number_of_workers, subscription_id, ingestion_configuration_id):
        """
        We spawn the transform processes without actually activating them...
        """

        #configuration= {'process':{'name':'configuration','type':"stream_process",'listen_name': listen_name }}
        configuration= {}
        transform_ids = []

        # launch the transforms
        # we spawn the transform processes without actually activating them yet...
        for i in range(number_of_workers):
            transform_id = self.clients.transform_management.create_transform(in_subscription_id=subscription_id, \
                process_definition_id=self.process_definition_id, configuration=configuration)
#            transform_id = self.clients.transform_management.create_transform(in_subscription_id=subscription_id,\
#                        process_definition_id=self.process_definition_id, configuration={'process':{'name':'transform'+str(i)}})
            self.clients.resource_registry.create_association(ingestion_configuration_id, AT.hasTransform, transform_id)


    def update_ingestion_configuration(self, ingestion_configuration={}):
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
        ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        if ingestion_configuration is None:
            raise NotFound("Ingestion configuration %d does not exist" % ingestion_configuration_id)

        self.clients.resource_registry.delete(ingestion_configuration)


    def activate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Activate an ingestion configuration and the transform processes that execute it

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """

        log.debug("Activating ingestion configuration")

        transform_ids, _ = self.clients.resource_registry.find_objects(ingestion_configuration_id,
            AT.hasTransform, RT.Transform, True)
        if len(transform_ids) < 1:
            raise NotFound
        for transform_id in transform_ids:
            self.clients.transform_management.activate_transform(transform_id)
        return True


    def deactivate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Deactivate an ingestion configuration and the transform processeses that execute it

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """
        log.debug("Deactivating ingestion configuration")
        # use the deactivate method in transformation management service
        pass

    def create_stream_policy(self, stream_id='', archive_data='', archive_metadata=''):
        """Create a policy for a particular stream and associate it to the ingestion configuration for the exchange point the stream is on. (After LCA)

        @param stream_id    str
        @param archive_data    str
        @param archive_metadata    str
        @retval ingestion_policy_id    str
        """

#        return ingestion_policy_id

    def update_stream_policy(self, stream_policy={}):
        """Change the number of workers or the default policy for ingesting data on each stream (After LCA)

        @param stream_policy    Unknown
        @throws NotFound    if ingestion configuration did not exist
        """


    def read_stream_policy(self, stream_policy_id=''):
        """Get an existing stream policy object. (After LCA)

        @param stream_policy_id    str
        @retval ingestion_configuration    IngestionConfiguration
        @throws NotFound    if ingestion configuration did not exist
        """

#        return ingestion_configuration

    def delete_stream_policy(self, ingestion_configuration_id=''):
        """Delete an existing stream policy object. (After LCA)

        @param ingestion_configuration_id    str
        @throws NotFound    if ingestion configuration did not exist
        """
