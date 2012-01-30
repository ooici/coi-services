#!/usr/bin/env python
__license__ = 'Apache 2.0'
'''
@author Maurice Manning
@author Swarbhanu Chatterjee
@file ion/services/dm/ingestion/ingestion_management_service.py
@description Implementation for IngestionManagementService
'''
from interface.services.dm.iingestion_management_service import BaseIngestionManagementService
from pyon.core.exception import NotFound
from pyon.public import RT, PRED, log, IonObject
from pyon.public import CFG, StreamProcess
from pyon.ion.endpoint import ProcessPublisher
from pyon.net.channel import SubscriberChannel
from pyon.container.procs import ProcManager
from pyon.core.exception import IonException
from interface.objects import StreamQuery, ExchangeQuery

class IngestionManagementServiceException(IonException):
    """
    Exception class for HDFEncoder exceptions. This class inherits from ScienceObjectTransportException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


class IngestionManagementService(BaseIngestionManagementService):
    """
    id_p = cc.spawn_process('ingestion_worker', 'ion.services.dm.ingestion.ingestion_management_service', 'IngestionManagementService')
    cc.proc_manager.procs['%s.%s' %(cc.id,id_p)].start()
    """

    def __init__(self):
        BaseIngestionManagementService.__init__(self)


    def create_ingestion_configuration(self, exchange_point_id='', couch_storage=None, hdf_storage=None, \
                                       number_of_workers=0, default_policy=None):
        """Setup ingestion workers to ingest all the data from a single exchange point.

        @param exchange_point_id    str
        @param couch_storage    Unknown
        @param hfd_storage    Unknown
        @param number_of_workers    int
        @param default_policy    Unknown
        @retval ingestion_configuration_id    str
        """

        XP = 'science_data' #CFG.exchange_spaces.ioncore.exchange_points.science_data.name
        exchange_name = 'ingestion_queue'

        ###################
        # adding output_streams so that ingestion looks like an ordinary transform
#        self.transform_output_stream_id = self.clients.pubsub_management.create_stream(name='transform_output', original=True)

        #########################################################################################################
        #   The code for process_definition may not really belong here, but we do not have a different way so
        #   far to preload the process definitions. This will later probably be part of a set of predefinitions
        #   for processes.
        #########################################################################################################
        process_definition = IonObject(RT.ProcessDefinition, name='ingestion_example')
        process_definition.executable = {'module': 'ion.services.dm.ingestion.ingestion_example', 'class':'IngestionExample'}
        process_definition_id, _ = self.clients.resource_registry.create(process_definition)

        ##------------------------------------------------------------------------------------
        ## declare our intent to subscribe to all messages on the exchange point
        query = ExchangeQuery()

        subscription_id = self.clients.pubsub_management.create_subscription(query=query, \
            exchange_name=exchange_name, name='subscription', description='only to launch ingestion workers')

        ##------------------------------------------------------------------------------------------

        # create an ingestion_configuration instance and update the registry
        # @todo: right now sending in the exchange_point_id as the name...
        ingestion_configuration = IonObject(RT.IngestionConfiguration, name = 'ingestion_configuration')
        ingestion_configuration.number_of_workers = number_of_workers
        ingestion_configuration.hdf_storage.update(hdf_storage or {})
        ingestion_configuration.couch_storage.update(couch_storage or {})
        ingestion_configuration.default_policy.update(default_policy or {})

        ingestion_configuration_id, _ = self.clients.resource_registry.create(ingestion_configuration)

        self._launch_transforms(ingestion_configuration.number_of_workers, subscription_id, ingestion_configuration_id, process_definition_id)

        return ingestion_configuration_id

    def _launch_transforms(self, number_of_workers, subscription_id, ingestion_configuration_id, process_definition_id):
        """
        This method spawns the two transform processes without activating them...Note: activating the transforms does the binding
        """
        configuration= {}
        description = 'Ingestion worker'

        # launch the transforms
        for i in range(number_of_workers):
            name = '(%s)_Ingestion_Worker_%s' % (ingestion_configuration_id, i+1)
            transform_id = self.clients.transform_management.create_transform(name = name, description = description, \
                     in_subscription_id= subscription_id, out_streams = {}, process_definition_id=process_definition_id, \
                                configuration=configuration)
            # create association between ingestion configuration and the transforms that act as Ingestion Workers
            if not transform_id:
                raise IngestionManagementServiceException('Transform could not be launched by ingestion.')
            try:
                self.clients.resource_registry.create_association(ingestion_configuration_id, PRED.hasTransform, transform_id)
            except Exception as exc:
                raise IngestionManagementServiceException\
                    ('Associations could not be generated between ingestion configuration and transform %s' % transform_id)

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
        ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)

        if ingestion_configuration is None:
            log.debug("Ingestion configuration %d does not exist" % ingestion_configuration_id)

        self.clients.resource_registry.delete(ingestion_configuration_id)


    def activate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Activate an ingestion configuration and the transform processes that execute it

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """

        log.debug("Activating ingestion configuration")

        # check whether the ingestion configuration object exists
        try:
            ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        except:
            raise NotFound("Ingestion configuration %s does not exist" % str(ingestion_configuration_id))

        # read the transforms
        transform_ids, _ = self.clients.resource_registry.find_objects(ingestion_configuration_id, PRED.hasTransform, RT.Transform, True)
        if len(transform_ids) > 0:
            # This is messy - but for now activate_transform, which calls activate subscription is idempotent.
            # calling it many times is just activating the same subscription many times.
            #
            # Maybe we should bypass activate transform and directly call pubsub activate/deactivate?
            try:
                # need to activate only one transform as both have the same subscription
                self.clients.transform_management.activate_transform(transform_ids[0])
            except Exception as exc:
                raise IngestionManagementServiceException('Error while using transform_management to activate transform %s.'\
                    % transform_id)
        else:
            log.debug("No transforms attached as ingestion workers to the ingestion configuration object.")

        return True


    def deactivate_ingestion_configuration(self, ingestion_configuration_id=''):
        """Deactivate an ingestion configuration and the transform processeses that execute it

        @param ingestion_configuration_id    str
        @throws NotFound    The ingestion configuration id did not exist
        """
        log.debug("Deactivating ingestion configuration")

        # check whether the ingestion configuration object exists
        try:
            ingestion_configuration = self.read_ingestion_configuration(ingestion_configuration_id)
        except:
            raise NotFound("Ingestion configuration %s does not exist" % str(ingestion_configuration_id))


#        # use the deactivate method in transformation management service
        transform_ids, _ = self.clients.resource_registry.find_objects(ingestion_configuration_id, PRED.hasTransform, RT.Transform, True)
        if len(transform_ids) < 1:
            raise NotFound('The ingestion configuration %s does not exist' % str(ingestion_configuration_id))
        # since both transforms have the same subscription, only deactivate one
        self.clients.transform_management.deactivate_transform(transform_ids[0])

        return True

    def create_stream_policy(self, stream_id='', archive_data='', archive_metadata=''):
        """Create a policy for a particular stream and associate it to the ingestion configuration for the exchange point the stream is on. (After LCA)

        @param stream_id    str
        @param archive_data    str
        @param archive_metadata    str
        @retval ingestion_policy_id    str
        """
        pass

#        return ingestion_policy_id

    def update_stream_policy(self, stream_policy=None):
        """Change the number of workers or the default policy for ingesting data on each stream (After LCA)

        @param stream_policy    Unknown
        @throws NotFound    if ingestion configuration did not exist
        """
        pass


    def read_stream_policy(self, stream_policy_id=''):
        """Get an existing stream policy object. (After LCA)

        @param stream_policy_id    str
        @retval ingestion_configuration    IngestionConfiguration
        @throws NotFound    if ingestion configuration did not exist
        """
        pass

#        return ingestion_configuration

    def delete_stream_policy(self, ingestion_configuration_id=''):
        """Delete an existing stream policy object. (After LCA)

        @param ingestion_configuration_id    str
        @throws NotFound    if ingestion configuration did not exist
        """
        pass

