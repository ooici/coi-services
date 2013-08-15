#!/usr/bin/env python

"""Data Acquisition Management service to keep track of Data Producers, Data Sources and external data agents
and the relationships between them"""
from pyon.agent.agent import ResourceAgentClient

__author__ = 'Maurice Manning, Michael Meisinger'

from collections import deque
import logging
from copy import deepcopy

from ooi.timer import Timer, Accumulator

from pyon.core.exception import NotFound, BadRequest, ServerError
from pyon.public import CFG, IonObject, log, RT, LCS, PRED, OT
from pyon.util.arg_check import validate_is_instance

from ion.services.sa.instrument.agent_configuration_builder import ExternalDatasetAgentConfigurationBuilder
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.util.stored_values import StoredValueManager
from ion.util.agent_launcher import AgentLauncher

from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget, ProcessRestartMode
from interface.objects import Parser, DataProducer, InstrumentProducerContext, ExtDatasetProducerContext, DataProcessProducerContext
from interface.objects import AttachmentType
from interface.services.sa.idata_acquisition_management_service import BaseDataAcquisitionManagementService


stats = Accumulator(persist=True)

class DataAcquisitionManagementService(BaseDataAcquisitionManagementService):

    def on_init(self):
        self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)

    # -----------------
    # The following operations register different types of data producers
    # -----------------

    def register_external_data_set(self, external_dataset_id=''):
        """Register an existing external data set as data producer

        @param external_dataset_id    str
        @retval data_producer_id    str
        """
        ext_dataset_obj = self.clients.resource_registry.read(external_dataset_id)

        if ext_dataset_obj is None:
            raise NotFound('External Data Set %s does not exist' % external_dataset_id)

        #create a InstrumentProducerContext to hold the state of the this producer
        producer_context_obj = ExtDatasetProducerContext(configuration=vars(ext_dataset_obj))

        #create data producer resource and associate to this data_process_id
        data_producer_obj = DataProducer(name=ext_dataset_obj.name,
                                         description='Primary DataProducer for External Dataset %s' % ext_dataset_obj.name,
                                         is_primary=True,
                                         producer_context=producer_context_obj)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(subject=external_dataset_id, predicate=PRED.hasDataProducer, object=data_producer_id)

        return data_producer_id

    def unregister_external_data_set(self, external_dataset_id=''):
        """

        @param external_dataset_id    str
        @throws NotFound    object with specified id does not exist
        """
        # Verify that  id is valid
        external_data_set_obj = self.clients.resource_registry.read(external_dataset_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(
            subject=external_dataset_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_external_data_set  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_external_data_set  delete producer %s", str(producer))
            self.clients.resource_registry.delete(producer)

        return


    def register_process(self, data_process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if data_process_obj is None:
            raise NotFound('Data Process %s does not exist' % data_process_id)

        producer_context_obj = DataProcessProducerContext(configuration=data_process_obj.configuration)

        #create data producer resource and associate to this data_process_id
        data_producer_obj = DataProducer(name=data_process_obj.name,
            description='Primary DataProducer for DataProcess %s' % data_process_obj.name,
            producer_context=producer_context_obj,
            is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(data_process_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def register_event_process(self, process_id=''):
        """
        Register an existing data process as data producer
        """

        # retrieve the data_process object
        # retrieve the data_process object
        data_process_obj = self.clients.resource_registry.read(process_id)
        if data_process_obj is None:
            raise NotFound('Data Process %s does not exist' % process_id)

        producer_context_obj = DataProcessProducerContext(configuration=data_process_obj.process_configuration)

        #create data producer resource and associate to this data_process_id
        data_producer_obj = DataProducer(name=data_process_obj.name,
                                         description='Primary DataProducer for DataProcess %s' % data_process_obj.name,
                                         producer_context=producer_context_obj,
                                         is_primary=True)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(process_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_process(self, data_process_id=''):
        """
        Remove the associated DataProcess and disc

        """
        # Verify that  id is valid
        input_process_obj = self.clients.resource_registry.read(data_process_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=data_process_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_process  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_process  delete producer %s", str(producer))

            log.debug("DAMS:unregister_process delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)


    def unregister_event_process(self, process_id=''):
        """
        Remove the associated Process and disc

        """
        # Verify that  id is valid
        input_process_obj = self.clients.resource_registry.read(process_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=process_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_process  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_process  delete producer %s", str(producer))

            log.debug("DAMS:unregister_process delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)

    def register_instrument(self, instrument_id=''):
        """
        Register an existing instrument as data producer
        """
        # retrieve the data_process object
        instrument_obj = self.clients.resource_registry.read(instrument_id)

        if instrument_obj is None:
            raise NotFound('Instrument object %s does not exist' % instrument_id)

        #create a InstrumentProducerContext to hold the state of the this producer
        producer_context_obj = InstrumentProducerContext(configuration=vars(instrument_obj))

        #create data producer resource and associate to this data_process_id
        data_producer_obj = DataProducer(name=instrument_obj.name,
                                         description='Primary DataProducer for DataProcess %s' % instrument_obj.name,
                                         is_primary=True,
                                         producer_context=producer_context_obj)
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)

        # Create association
        self.clients.resource_registry.create_association(instrument_id, PRED.hasDataProducer, data_producer_id)

        return data_producer_id

    def unregister_instrument(self, instrument_id=''):

        # Verify that  id is valid
        # Verify that  id is valid
        input_process_obj = self.clients.resource_registry.read(instrument_id)

        # List all resource ids that are objects for this data_source and has the hasDataProducer link
        producers, producer_assns = self.clients.resource_registry.find_objects(subject=instrument_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer, producer_assn in zip(producers, producer_assns):
            log.debug("DataAcquisitionManagementService:unregister_instrument  delete association %s", str(producer_assn))
            self.clients.resource_registry.delete_association(producer_assn)
            log.debug("DataAcquisitionManagementService:unregister_instrument  delete producer %s", str(producer))
            self.clients.resource_registry.delete(producer)
        return

    def assign_data_product(self, input_resource_id='', data_product_id=''):

        log.debug('assigning data product %s to resource %s', data_product_id, input_resource_id)
        #Connect the producer for an existing input resource with a data product

        t = Timer() if stats.is_log_enabled() else None

        # Verify that both ids are valid
        #input_resource_obj = self.clients.resource_registry.read(input_resource_id) #actually, don't need this one unless producer is not found (see if below)
        data_product_obj = self.clients.resource_registry.read(data_product_id)
        if t:
            t.complete_step('dams.assign_data_product.read_dataproduct')

        #find the data producer resource associated with the source resource that is creating the data product
        primary_producer_ids, _ = self.clients.resource_registry.find_objects(subject=input_resource_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=True)
        if t:
            t.complete_step('dams.assign_data_product.find_producer')
        if not primary_producer_ids:
            self.clients.resource_registry.read(input_resource_id) # raise different NotFound if resource didn't exist
            raise NotFound("Data Producer for input resource %s does not exist" % input_resource_id)

        #connect the producer to the product directly
        self.clients.resource_registry.create_association(subject=input_resource_id, predicate=PRED.hasOutputProduct, object=data_product_id)
        if t:
            t.complete_step('dams.assign_data_product.create_association.hasOutputProduct')

        #create data producer resource for this data product
        data_producer_obj = DataProducer(name=data_product_obj.name, description=data_product_obj.description)
        data_producer_obj.producer_context.configuration = {}
        data_producer_id, rev = self.clients.resource_registry.create(data_producer_obj)
        if t:
            t.complete_step('dams.assign_data_product.create_dataproducer')

        attachments = self.clients.resource_registry.find_attachments(data_product_id, include_content=False, id_only=False)
        if t:
            t.complete_step('dams.assign_data_product.find_attachments')
        for attachment in attachments:
            if attachment.attachment_type == AttachmentType.REFERENCE:
                parser_id = attachment.context.parser_id
                if parser_id:
                    self.register_producer_qc_reference(data_producer_id, parser_id, attachment._id)
        if t:
            t.complete_step('dams.assign_data_product.register_qc')
        # Associate the Product with the Producer
        self.clients.resource_registry.create_association(data_product_id,  PRED.hasDataProducer,  data_producer_id)
        if t:
            t.complete_step('dams.assign_data_product.create_association.hasDataProducer')
        # Associate the Producer with the main Producer
        self.clients.resource_registry.create_association(data_producer_id,  PRED.hasParent,  primary_producer_ids[0])
        if t:
            t.complete_step('dams.assign_data_product.create_association.hasParent')
            stats.add(t)
            stats.add_value('dams.assign_data_product.attachment_count', len(attachments))

    def unassign_data_product(self, input_resource_id='', data_product_id=''):
        """
        Disconnect the Data Product from the Data Producer

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        # Verify that both ids are valid
        input_resource_obj = self.clients.resource_registry.read(input_resource_id)
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        #find the data producer resource associated with the source resource that is creating the data product
        primary_producer_ids, _ = self.clients.resource_registry.find_objects(input_resource_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
        if not primary_producer_ids:
            raise NotFound("Data Producer for input resource %s does not exist" % input_resource_id)
        else:
            log.debug("unassign_data_product: primary producer ids %s" % str(primary_producer_ids))


        #find the hasDataProduct association between the data product and the input resource
        associations = self.clients.resource_registry.find_associations(subject=input_resource_id, predicate=PRED.hasOutputProduct, object=data_product_id, id_only=True)
        for association in associations:
            log.debug("unassign_data_product: unlink input resource with data product %s" % association)
            self.clients.resource_registry.delete_association(association)

        #find the data producer resource associated with the source resource that is creating the data product
        producers, producer_assns = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataProducer, RT.DataProducer, True)
        for producer, producer_assn in zip(producers, producer_assns):
            #remove the link to the data product
            self.clients.resource_registry.delete_association(producer_assn)

            #remove the link to the parent data producer
            associations = self.clients.resource_registry.find_associations(subject=producer, predicate=PRED.hasParent, id_only=True)
            for association in associations:
                self.clients.resource_registry.delete_association(association)

            log.debug("DAMS:unassign_data_product delete producer: %s ", str(producer) )
            self.clients.resource_registry.delete(producer)

        return



    def assign_data_product_source(self, data_product_id='', source_id=''):
        # Connect a Data Product to the data source, either a Site or a Device
        if source_id:
            #connect the producer to the product directly
            self.clients.resource_registry.create_association(data_product_id,  PRED.hasSource,  source_id)

        return


    def unassign_data_product_source(self, data_product_id='', source_id=''):
        # Disconnect the Data Product from the data source
        # Find and break association with either a Site or a Decvice
        assocs = self.clients.resource_registry.find_associations(data_product_id, PRED.hasSource, source_id)
        if not assocs or len(assocs) == 0:
            raise NotFound("DataProduct to source association for data product id %s to source %s does not exist" % (data_product_id, source_id))
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        return



#
#    def create_data_producer(name='', description=''):
#        """Create a data producer resource, create a stream reource via DM then associate the two resources. Currently, data producers and streams are one-to-one. If the data producer is a process, connect the data producer to any parent data producers.
#
#        @param name    str
#        @param description    str
#        @retval data_producer_id    str
#        @throws BadRequest    if object passed has _id or _rev attribute
#        """
#        pass
#
#    def update_data_producer(self, data_producer=None):
#        '''
#        Update an existing data producer.
#
#        @param data_producer The data_producer object with updated properties.
#        @retval success Boolean to indicate successful update.
#        @todo Add logic to validate optional attributes. Is this interface correct?
#        '''
#        # Return Value
#        # ------------
#        # {success: true}
#        #
#        log.debug("Updating data_producer object: %s" % data_producer.name)
#        return self.clients.resource_registry.update(data_producer)
#
#    def read_data_producer(self, data_producer_id=''):
#        '''
#        Get an existing data_producer object.
#
#        @param data_producer_id The id of the stream.
#        @retval data_producer The data_producer object.
#        @throws NotFound when data_producer doesn't exist.
#        '''
#        # Return Value
#        # ------------
#        # data_producer: {}
#        #
#        log.debug("Reading data_producer object id: %s" % data_producer_id)
#        data_producer_obj = self.clients.resource_registry.read(data_producer_id)
#
#        return data_producer_obj
#
#    def delete_data_producer(self, data_producer_id=''):
#        '''
#        Delete an existing data_producer.
#
#        @param data_producer_id The id of the stream.
#        @retval success Boolean to indicate successful deletion.
#        @throws NotFound when data_producer doesn't exist.
#        '''
#        # Return Value
#        # ------------
#        # {success: true}
#        #
#        log.debug("Deleting data_producer id: %s" % data_producer_id)
#
#        return self.clients.resource_registry.retire(data_producer_id)
#
#
#    def force_delete_data_producer(self, data_producer_id=''):
#        self._remove_associations(data_producer_id)
#        self.clients.resource_registry.delete(data_producer_id)

    # -----------------
    # The following operations manage EOI resources
    # -----------------

    ##########################################################################
    #
    # External Data Provider
    #
    ##########################################################################

    def create_external_data_provider(self, external_data_provider=None):
        # Persist ExternalDataProvider object and return object _id as OOI id
        return self.RR2.create(external_data_provider, RT.ExternalDataProvider)

    def update_external_data_provider(self, external_data_provider=None):
        # Overwrite ExternalDataProvider object
        self.RR2.update(external_data_provider, RT.ExternalDataProvider)

    def read_external_data_provider(self, external_data_provider_id=''):
        # Read ExternalDataProvider object with _id matching passed user id
        return self.RR2.read(external_data_provider_id, RT.ExternalDataProvider)

    def delete_external_data_provider(self, external_data_provider_id=''):
        self.RR2.retire(external_data_provider_id, RT.ExternalDataProvider)

    def force_delete_external_data_provider(self, external_data_provider_id=''):
        self.RR2.pluck_delete(external_data_provider_id, RT.ExternalDataProvider)

    ##########################################################################
    #
    # Data Source
    #
    ##########################################################################

    def create_data_source(self, data_source=None):
        # Persist DataSource object and return object _id as OOI id
        return self.RR2.create(data_source, RT.DataSource)

    def update_data_source(self, data_source=None):
        # Overwrite DataSource object
        self.RR2.update(data_source, RT.DataSource)

    def read_data_source(self, data_source_id=''):
        # Read DataSource object with _id matching passed user id
        log.debug("Reading DataSource object id: %s" % data_source_id)
        data_source_obj = self.RR2.read(data_source_id, RT.DataSource)
        return data_source_obj

    def delete_data_source(self, data_source_id=''):
        # Read and delete specified DataSource object
        log.debug("Deleting DataSource id: %s" % data_source_id)
        self.RR2.retire(data_source_id, RT.DataSource)
        return

    def force_delete_data_source(self, data_source_id=''):
        self.RR2.pluck_delete(data_source_id, RT.DataSource)


    def create_data_source_model(self, data_source_model=None):
        # Persist DataSourceModel object and return object _id as OOI id
        return self.RR2.create(data_source_model, RT.DataSourceModel)

    def update_data_source_model(self, data_source_model=None):
        # Overwrite DataSourceModel object
        self.RR2.update(data_source_model, RT.DataSourceModel)

    def read_data_source_model(self, data_source_model_id=''):
        # Read DataSourceModel object with _id matching passed user id
        return self.RR2.read(data_source_model_id, RT.DataSourceModel)

    def delete_data_source_model(self, data_source_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        self.RR2.retire(data_source_model_id, RT.DataSourceModel)
        return

    def force_delete_data_source_model(self, data_source_model_id=''):
        self.RR2.pluck_delete(data_source_model_id, RT.DataSourceModel)

    def create_data_source_agent(self, data_source_agent=None, data_source_model_id='' ):
        # Persist ExternalDataSourcAgent object and return object _id as OOI id
        data_source_agent_id = self.RR2.create(data_source_agent, RT.DataSourceAgent)

        if data_source_model_id:
            self.RR2.assign_data_source_model_to_data_source_agent_with_has_model(data_source_model_id, data_source_agent_id)
        return data_source_agent_id

    def update_data_source_agent(self, data_source_agent=None):
        # Overwrite DataSourceAgent object
        self.RR2.update(data_source_agent, RT.DataSourceAgent)

    def read_data_source_agent(self, data_source_agent_id=''):
        # Read DataSourceAgent object with _id matching passed user id
        data_source_agent = self.RR2.read(data_source_agent_id, RT.DataSourceAgent)
        return data_source_agent

    def delete_data_source_agent(self, data_source_agent_id=''):
        # Read and delete specified DataSourceAgent object
        self.RR2.retire(data_source_agent_id, RT.DataSourceAgent)

    def force_delete_data_source_agent(self, data_source_agent_id=''):
        self.RR2.pluck_delete(data_source_agent_id, RT.DataSourceAgent)


    def create_data_source_agent_instance(self, data_source_agent_instance=None, data_source_agent_id='', data_source_id=''):
        # Persist DataSourceAgentInstance object and return object _id as OOI id
        data_source_agent_instance_id = self.RR2.create(data_source_agent_instance, RT.DataSourceAgentInstance)

        if data_source_id:
            self.RR2.assign_data_source_agent_instance_to_data_source_with_has_agent_instance(data_source_agent_instance_id, data_source_id)

        if data_source_agent_id:
            self.RR2.assign_data_source_agent_to_data_source_agent_instance_with_has_agent_definition(data_source_agent_id, data_source_agent_instance_id)

        return data_source_agent_instance_id

    def update_data_source_agent_instance(self, data_source_agent_instance=None):
        # Overwrite DataSourceAgentInstance object
        self.RR2.update(data_source_agent_instance, RT.DataSourceAgentInstance)

    def read_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read DataSourceAgentInstance object with _id matching passed user id
        data_source_agent_instance = self.RR2.read(data_source_agent_instance_id, RT.DataSourceAgentInstance)
        return data_source_agent_instance

    def delete_data_source_agent_instance(self, data_source_agent_instance_id=''):
        # Read and delete specified DataSourceAgentInstance object
        self.RR2.retire(data_source_agent_instance_id, RT.DataSourceAgentInstance)

    def force_delete_data_source_agent_instance(self, data_source_agent_instance_id=''):
        self.RR2.pluck_delete(data_source_agent_instance_id, RT.DataSourceAgentInstance)

    def start_data_source_agent_instance(self, data_source_agent_instance_id=''):
        """Launch an data source agent instance process and return its process id. Agent instance resource
        must exist and be associated with an external data source

        @param data_source_agent_instance_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass

    def stop_data_source_agent_instance(self, data_source_agent_instance_id=''):
        """Deactivate the  agent instance process

        @param data_source_agent_instance_id    str
        @throws NotFound    object with specified id does not exist
        """
        pass


    ##########################################################################
    #
    # External Data Set
    #
    ##########################################################################
    def create_external_dataset(self, external_dataset=None, external_dataset_model_id=''):
        # Persist ExternalDataSet object and return object _id as OOI id
        external_dataset_id = self.RR2.create(external_dataset, RT.ExternalDataset)
        if external_dataset_model_id:
            self.RR2.assign_external_dataset_model_to_external_dataset_with_has_model(external_dataset_model_id, external_dataset_id)
        return external_dataset_id

    def update_external_dataset(self, external_dataset=None):
        # Overwrite ExternalDataSet object
        self.RR2.update(external_dataset, RT.ExternalDataset)

    def read_external_dataset(self, external_dataset_id=''):
        # Read ExternalDataSet object with _id matching passed user id
        external_dataset = self.RR2.read(external_dataset_id, RT.ExternalDataset)

        return external_dataset

    def delete_external_dataset(self, external_dataset_id=''):
        # Read and delete specified ExternalDataSet object

        self.RR2.retire(external_dataset_id, RT.ExternalDataset)

    def force_delete_external_dataset(self, external_dataset_id=''):
        self.RR2.pluck_delete(external_dataset_id, RT.ExternalDataset)

    def create_external_dataset_model(self, external_dataset_model=None):
        # Persist ExternalDatasetModel object and return object _id as OOI id
        return self.RR2.create(external_dataset_model, RT.ExternalDatasetModel)

    def update_external_dataset_model(self, external_dataset_model=None):
        # Overwrite ExternalDatasetModel object
        self.RR2.update(external_dataset_model, RT.ExternalDatasetModel)

    def read_external_dataset_model(self, external_dataset_model_id=''):
        # Read ExternalDatasetModel object with _id matching passed user id
        external_dataset_model = self.RR2.read(external_dataset_model_id, RT.ExternalDatasetModel)

        return external_dataset_model

    def delete_external_dataset_model(self, external_dataset_model_id=''):
        # Read and delete specified ExternalDatasetModel object
        self.RR2.retire(external_dataset_model_id, RT.ExternalDatasetModel)

    def force_delete_external_dataset_model(self, external_dataset_model_id=''):
        self.RR2.pluck_delete(external_dataset_model_id, RT.ExternalDatasetModel)

    #
    # ExternalDatasetAgent
    #

    def create_external_dataset_agent(self, external_dataset_agent=None, external_dataset_model_id=''):
        # Persist ExternalDatasetAgent object and return object _id as OOI id
        agent_id = self.RR2.create(external_dataset_agent, RT.ExternalDatasetAgent)

        if external_dataset_model_id:
            # NOTE: external_dataset_model_id can be any model type
            self.clients.resource_registry.create_association(agent_id, PRED.hasModel, external_dataset_model_id)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.name = "ProcessDefinition for ExternalDatasetAgent %s" % external_dataset_agent.name
        process_definition.executable['url'] = external_dataset_agent.agent_uri
        process_definition.executable['module'] = external_dataset_agent.agent_module or 'ion.agents.data.simple_dataset_agent'
        process_definition.executable['class'] = external_dataset_agent.agent_class or 'TwoDelegateDatasetAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)
        log.debug("external_dataset_agent has process definition id %s", process_definition_id)

        # Associate the agent and the process def
        self.RR2.assign_process_definition_to_external_dataset_agent_with_has_process_definition(process_definition_id, agent_id)
        return agent_id

    def update_external_dataset_agent(self, external_dataset_agent=None):
        # Overwrite ExternalDataAgent object
        self.RR2.update(external_dataset_agent, RT.ExternalDatasetAgent)

    def read_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent = self.RR2.read(external_dataset_agent_id, RT.ExternalDatasetAgent)

        return external_dataset_agent

    def delete_external_dataset_agent(self, external_dataset_agent_id=''):
        # Read and delete specified ExternalDataAgent object

        self.RR2.retire(external_dataset_agent_id, RT.ExternalDatasetAgent)

    def force_delete_external_dataset_agent(self, external_dataset_agent_id=''):

        self.RR2.pluck_delete(external_dataset_agent_id, RT.ExternalDatasetAgent)

    def assign_model_to_external_dataset_agent(self, model_id='', external_dataset_agent_id=''):
        self.clients.resource_registry.create_association(external_dataset_agent_id, PRED.hasModel, model_id)

    #
    # ExternalDatasetAgentInstance
    #

    def create_external_dataset_agent_instance(self, external_dataset_agent_instance=None, external_dataset_agent_id='', external_dataset_id=''):
        # Persist ExternalDatasetAgentInstance object and return object _id as OOI id
        external_dataset_agent_instance_id = self.RR2.create(external_dataset_agent_instance, RT.ExternalDatasetAgentInstance)

        if external_dataset_id:
            self.RR2.assign_external_dataset_agent_instance_to_external_dataset_with_has_agent_instance(
                external_dataset_agent_instance_id, external_dataset_id)

        self.assign_external_data_agent_to_agent_instance(external_dataset_agent_id, external_dataset_agent_instance_id)
        log.debug('created dataset agent instance %s, agent id=%s', external_dataset_agent_instance_id, external_dataset_agent_id)
        return external_dataset_agent_instance_id

    def update_external_dataset_agent_instance(self, external_dataset_agent_instance=None):
        # Overwrite ExternalDataAgent object
        self.RR2.update(external_dataset_agent_instance, RT.ExternalDatasetAgentInstance)

    def read_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        # Read ExternalDatasetAgent object with _id matching passed user id
        external_dataset_agent_instance = self.RR2.read(external_dataset_agent_instance_id, RT.ExternalDatasetAgentInstance)

        return external_dataset_agent_instance

    def delete_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        self.RR2.retire(external_dataset_agent_instance_id, RT.ExternalDatasetAgentInstance)

    def force_delete_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        self.RR2.pluck_delete(external_dataset_agent_instance_id, RT.ExternalDatasetAgentInstance)

    def assign_external_dataset_agent_instance_to_device(self, external_dataset_agent_instance_id='', device_id=''):
        self.clients.resource_registry.create_association(device_id, PRED.hasAgentInstance, external_dataset_agent_instance_id)

    def start_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        """Launch an external dataset agent instance process and return its process id.
        Agent instance resource must exist and be associated with an external dataset or device and an agent definition
        @param external_dataset_agent_instance_id    str
        @retval process_id    str
        @throws NotFound    object with specified id does not exist
        """
        #todo: may want to call retrieve_external_dataset_agent_instance here
        #todo: if instance running, then return or throw
        #todo: if instance exists and dataset_agent_instance_obj.dataset_agent_config is completd then just schedule_process

        dataset_agent_instance_obj = self.clients.resource_registry.read(external_dataset_agent_instance_id)

        # can be a Device or ExternalDataset
        source_id = self.clients.resource_registry.read_subject(
            predicate=PRED.hasAgentInstance, object=external_dataset_agent_instance_id, id_only=True)

        ext_dataset_agent_obj = self.clients.resource_registry.read_object(
            object_type=RT.ExternalDatasetAgent, predicate=PRED.hasAgentDefinition, subject=external_dataset_agent_instance_id, id_only=False)
        process_definition_id = self.clients.resource_registry.read_object(
            subject=ext_dataset_agent_obj._id, predicate=PRED.hasProcessDefinition, object_type=RT.ProcessDefinition, id_only=True)

        # # Get ALL streams for given external dataset or device
        # data_product_objs, _ = self.clients.resource_registry.find_objects(source_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        # if not data_product_objs:
        #     raise NotFound("No stream found for %s" % source_id)
        #
        # for dp in data_product_objs:
        #     if 'parsed' in dp.processing_level_code.lower():
        #         data_product_id = dp._id
        #         break
        #
        # if not data_product_id:
        #     data_product_id = data_product_objs[0]._id
        #     log.warn("Cannot find parsed DataProduct for %s" % source_id)
        #
        # stream_def_id = self.clients.resource_registry.read_object(data_product_id, PRED.hasStreamDefinition, RT.StreamDefinition, id_only=True)
        # stream_def_obj = self.clients.pubsub_management.read_stream_definition(stream_def_id)
        #
        # stream_id = self.clients.resource_registry.read_object(data_product_id, PRED.hasStream, RT.Stream, id_only=True)
        # route = self.clients.pubsub_management.read_stream_route(stream_id)
        #
        # if log.isEnabledFor(logging.DEBUG):
        #     log.debug('stream def: %r', {key: getattr(stream_def_obj, key) for key in stream_def_obj._schema})

        # dataset_agent_instance_obj.dataset_agent_config['driver_config']['parameter_dict'] = stream_def_obj.parameter_dictionary
        # dataset_agent_instance_obj.dataset_agent_config['driver_config']['stream_id'] = stream_id
        # dataset_agent_instance_obj.dataset_agent_config['driver_config']['stream_route'] = {key: getattr(route, key) for key in route._schema}

        # Agent launch

        config_builder = ExternalDatasetAgentConfigurationBuilder(self.clients)
        try:
            config_builder.set_agent_instance_object(dataset_agent_instance_obj)
            config = config_builder.prepare()
            # import pprint
            # pprint.pprint(config)
        except:
            log.error('failed to launch', exc_info=True)
            raise ServerError('failed to launch')

        launcher = AgentLauncher(self.clients.process_dispatcher)
        process_id = launcher.launch(config, config_builder._get_process_definition()._id)
        if not process_id:
            raise ServerError("Launched external dataset agent instance but no process_id")
        config_builder.record_launch_parameters(config)

        launcher.await_launch(10.0)

        return process_id


    def stop_external_dataset_agent_instance(self, external_dataset_agent_instance_id=''):
        """
        Deactivate the agent instance process
        """
        # this dataset agent instance could be link to a external dataset or a instrument device. Retrieve whatever is the data producer.
        external_dataset_device_ids, _ = self.clients.resource_registry.find_subjects( predicate=PRED.hasAgentInstance, object=external_dataset_agent_instance_id, id_only=True)
        if len(external_dataset_device_ids) != 1:
            raise NotFound("ExternalDatasetAgentInstance resource is not correctly associated with an ExternalDataset or InstrumentDevice" )

        agent_process_id = ResourceAgentClient._get_agent_process_id(external_dataset_device_ids[0])

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(agent_process_id)



    def retrieve_external_dataset_agent_instance(self, external_dataset_id=''):
        """
        Retrieve the agent instance for an external dataset and check if it is running
        """
        #Connect the data source with an external data provider
        data_set = self.clients.resource_registry.read(external_dataset_id)

        # check if the association already exists
        ai_ids, _  = self.clients.resource_registry.find_objects(external_dataset_id,  PRED.hasAgentInstance, id_only=True)
        if len(ai_ids) > 1:
            raise NotFound("ExternalDataset resource %s is associated with multiple agent instances" % external_dataset_id)

        if ai_ids is None:
            return None, None
        else:
            if not ResourceAgentClient._get_agent_process_id(external_dataset_id):
                active = False
            else:
                active = True
            return ai_ids[0], active


    ##########################################################################
    #
    # Resource Assign Functions
    #
    ##########################################################################

    def assign_data_source_to_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Connect the data source with an external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(external_data_provider_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasProvider,  external_data_provider_id, id_only=True)
        if not associations:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasProvider,  external_data_provider_id)

    def unassign_data_source_from_external_data_provider(self, data_source_id='', external_data_provider_id=''):
        #Disconnect the data source from the external data provider
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(external_data_provider_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id, PRED.hasProvider, external_data_provider_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_data_source_to_data_model(self, data_source_id='', data_source_model_id=''):
        #Connect the data source with an external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(data_source_model_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, id_only=True)
        if not associations:
            self.clients.resource_registry.create_association(data_source_id,  PRED.hasModel,  data_source_model_id)

    def unassign_data_source_from_data_model(self, data_source_id='', data_source_model_id=''):
        #Disonnect the data source from the external data model
        data_source = self.clients.resource_registry.read(data_source_id)
        agent_instance = self.clients.resource_registry.read(data_source_model_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(data_source_id,  PRED.hasModel,  data_source_model_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_dataset_to_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        #Connect the agent instance with an external data set
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        log.debug("associating: external dataset %s hasAgentInstance %s", external_dataset_id, agent_instance_id)
        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, id_only=True)
        if not associations:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id)

    def unassign_external_dataset_from_agent_instance(self, external_dataset_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasAgentInstance,  agent_instance_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)



    def assign_external_data_agent_to_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        #Connect the agent with an agent instance
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)
        log.debug("associating: external dataset agent instance %s hasAgentDefinition %s", agent_instance_id, external_data_agent_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(agent_instance_id,  PRED.hasAgentDefinition,   external_data_agent_id, id_only=True)
        log.trace('found associations: %r', associations)
        if not associations:
            self.clients.resource_registry.create_association(agent_instance_id,  PRED.hasAgentDefinition,   external_data_agent_id)

    def unassign_external_data_agent_from_agent_instance(self, external_data_agent_id='', agent_instance_id=''):
        data_source = self.clients.resource_registry.read(external_data_agent_id)
        agent_instance = self.clients.resource_registry.read(agent_instance_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(agent_instance_id,  PRED.hasAgentDefinition,  external_data_agent_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_dataset_agent_to_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Connect the external data agent with an external data model
        external_data_agent = self.clients.resource_registry.read(dataset_agent_id)
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id, id_only=True)
        if not associations:
            self.clients.resource_registry.create_association(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id)

    def unassign_dataset_agent_from_external_dataset_model(self, dataset_agent_id='', external_dataset_model_id=''):
        #Disonnect the external data agent from the external data model
        dataset_agent = self.clients.resource_registry.read(dataset_agent_id)
        external_dataset_model = self.clients.resource_registry.read(external_dataset_model_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(dataset_agent_id,  PRED.hasModel,  external_dataset_model_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)


    def assign_external_dataset_to_data_source(self, external_dataset_id='', data_source_id=''):
        #Connect the external data set to a data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(data_source_id)

        # check if the association already exists
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasSource,  data_source_id, id_only=True)
        if not associations:
            self.clients.resource_registry.create_association(external_dataset_id,  PRED.hasDataSource,  data_source_id)


    def unassign_external_dataset_from_data_source(self, external_dataset_id='', data_source_id=''):
        #Disonnect the external data set from the data source
        data_source = self.clients.resource_registry.read(external_dataset_id)
        agent_instance = self.clients.resource_registry.read(data_source_id)

        # delete the associations
        # List all association ids with given subject, predicate, object triples
        associations = self.clients.resource_registry.find_associations(external_dataset_id,  PRED.hasDataSource,  data_source_id, id_only=True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)

    def create_parser(self, parser=None):
        parser_id, rev = self.clients.resource_registry.create(parser)
        return parser_id

    def read_parser(self, parser_id=''):
        parser = self.clients.resource_registry.read(parser_id)
        validate_is_instance(parser,Parser,'The specified identifier does not correspond to a Parser resource')
        return parser

    def delete_parser(self, parser_id=''):
        self.clients.resource_registry.delete(parser_id)
        return True

    def update_parser(self, parser=None):
        if parser:
            self.clients.resource_registry.update(parser)


    def register_producer_qc_reference(self, producer_id='', parser_id='', attachment_id=''):
        log.debug('register_producer_qc_reference: %s %s %s', producer_id, parser_id, attachment_id)
        attachment = self.clients.resource_registry.read_attachment(attachment_id, include_content=True)
        document = attachment.content
        document_keys = self.parse_qc_reference(parser_id, document) or []

        producer_obj = self.clients.resource_registry.read(producer_id)
        if 'qc_keys' in producer_obj.producer_context.configuration:
            producer_obj.producer_context.configuration['qc_keys'].extend(document_keys)
        else:
            producer_obj.producer_context.configuration['qc_keys'] = document_keys

        self.clients.resource_registry.update(producer_obj)
        return True

    def parse_qc_reference(self, parser_id='', document=None):
        document_keys = []
        if document is None:
            raise BadRequest('Empty Document')
        parser = self.read_parser(parser_id=parser_id)
        try:
            module = __import__(parser.module, fromlist=[parser.method])
            method = getattr(module, parser.method)

        except ImportError:
            raise BadRequest('No import named {0} found.'.format(parser.module))
        except AttributeError:
            raise BadRequest('No method named {0} in {1}.'.format(parser.method, parser.module))
        except:
            log.exception('Failed to parse document')
            raise

        svm = StoredValueManager(self.container)
        for key, doc in method(document):
            try:
                svm.stored_value_cas(key, doc)
                document_keys.append(key)
            except:
                log.error('Error parsing a row in document.')
        return document_keys


    def list_qc_references(self, data_product_id=''):
        ''' Performs a breadth-first traversal of the provenance for a data product in an attempt to collect all the document keys'''
        document_keys = []
        producer_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataProducer, id_only=True)
        if not len(producer_ids):
            return []
        producer_id = producer_ids.pop(0)
        def traversal(owner_id):
            def edges(resource_ids=[]):
                retval = []
                if not isinstance(resource_ids, list):
                    resource_ids = list(resource_ids)
                for resource_id in resource_ids:
                    retval.extend(self.clients.resource_registry.find_objects(subject=resource_id, predicate=PRED.hasParent,id_only=True)[0])
                return retval

            visited_resources = deque([producer_id] + edges([owner_id]))
            traversal_queue = deque()
            done = False
            t = None
            while not done:
                t = traversal_queue or deque(visited_resources)
                traversal_queue = deque()
                for e in edges(t):
                    if not e in visited_resources:
                        visited_resources.append(e)
                        traversal_queue.append(e)
                if not len(traversal_queue): done = True
            return list(visited_resources)

        for prod_id in traversal(producer_id):
            producer = self.clients.resource_registry.read(prod_id)
            if 'qc_keys' in producer.producer_context.configuration:
                document_keys.extend(producer.producer_context.configuration['qc_keys'])
        return document_keys

