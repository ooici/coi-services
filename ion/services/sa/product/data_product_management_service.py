#!/usr/bin/env python
__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.public import  log, IonObject
from pyon.util.containers import DotDict
from pyon.core.object import IonObjectBase
from pyon.core.exception import BadRequest, NotFound
from pyon.public import RT, OT, PRED, LCS, CFG
from pyon.util.ion_time import IonTime
from pyon.ion.resource import ExtendedResourceContainer
from pyon.event.event import EventPublisher
from pyon.util.arg_check import validate_is_instance, validate_is_not_none, validate_false, validate_true
from pyon.net.endpoint import RPCClient

from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.util.time_utils import TimeUtils
from ion.util.geo_utils import GeoUtils

from interface.services.sa.idata_product_management_service import BaseDataProductManagementService
from interface.objects import DataProduct, DataProductVersion, InformationStatus, DataProcess, DataProcessTypeEnum
from interface.objects import ComputedValueAvailability

from coverage_model import QuantityType, ParameterContext, ParameterDictionary, NumexprFunction, ParameterFunctionType
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from pyon.datastore.datastore import DataStore
from pyon.datastore.datastore_query import DatastoreQueryBuilder, DQ

from lxml import etree
from datetime import datetime

import numpy as np
import string, StringIO
import networkx as nx
import matplotlib.pyplot as plt
from collections import deque
from pyon.core.governance import ORG_MANAGER_ROLE, DATA_OPERATOR, OBSERVATORY_OPERATOR, INSTRUMENT_OPERATOR, GovernanceHeaderValues, has_org_role
from pyon.core.exception import Inconsistent

import functools
from pyon.util.breakpoint import debug_wrapper


class DataProductManagementService(BaseDataProductManagementService):
    """ @author     Bill Bollenbacher
        @file       ion/services/sa/product/data_product_management_service.py
        @brief      Implementation of the data product management service
    """

    def on_init(self):
        self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)



    def create_data_product(self, data_product=None, stream_definition_id='', exchange_point='', dataset_id='', parent_data_product_id='', default_stream_configuration=None):
        """
        @param      data_product IonObject which defines the general data product resource
        @param      source_resource_id IonObject id which defines the source for the data
        @retval     data_product_id
        """
        data_product_id = self.create_data_product_(data_product)

        # WARNING: This creates a Stream as a side effect!!
        self.assign_stream_definition_to_data_product(data_product_id=data_product_id,
                                                      stream_definition_id=stream_definition_id,
                                                      exchange_point=exchange_point,
                                                      stream_configuration=default_stream_configuration)

        if dataset_id and parent_data_product_id:
            raise BadRequest('A parent dataset or parent data product can be specified, not both.')
        if dataset_id and not data_product_id:
            # TODO: Q: How can this ever be true?
            self.assign_dataset_to_data_product(data_product_id=data_product_id, dataset_id=dataset_id)
        if parent_data_product_id and not dataset_id:
            self.assign_data_product_to_data_product(data_product_id=data_product_id, parent_data_product_id=parent_data_product_id)
            dataset_ids, _ = self.clients.resource_registry.find_objects(parent_data_product_id, predicate=PRED.hasDataset, id_only=True)
            for dataset_id in dataset_ids:
                self.assign_dataset_to_data_product(data_product_id, dataset_id)
            if dataset_ids:
                self.create_catalog_entry(data_product_id)

      # Return the id of the new data product
        return data_product_id

    def create_data_product_(self, data_product=None):

        validate_is_not_none(data_product, 'A data product (ion object) must be passed to register a data product')

        # if the geospatial_bounds is set then calculate the geospatial_point_center
        if data_product and data_product.type_ == RT.DataProduct:
            data_product.geospatial_point_center = GeoUtils.calc_geospatial_point_center(data_product.geospatial_bounds)
            log.debug("create_data_product data_product.geospatial_point_center: %s" % data_product.geospatial_point_center)

        #--------------------------------------------------------------------------------
        # Register - create and store a new DataProduct resource using provided metadata
        #--------------------------------------------------------------------------------
        data_product_id = self.RR2.create(data_product, RT.DataProduct)

        return data_product_id

    def create_data_processes(self, data_product_id=''):
        '''
        For each data process launched also create a dataprocess for each parameter function in the data product
        '''
        data_product = self.read_data_product(data_product_id)

        # DataProduct -> StreamDefinition
        stream_def_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=True)
        pdict_ids = []
        # StreamDefinition -> ParameterDictionary
        for stream_def_id in stream_def_ids:
            pd_ids, _ = self.clients.resource_registry.find_objects(stream_def_id, PRED.hasParameterDictionary, id_only=True)
            pdict_ids.extend(pd_ids)


        pd_ids = []
        # ParameterDictionary -> ParameterContext
        for pdict_id in pdict_ids:
            pdef_ids, _ = self.clients.resource_registry.find_objects(pdict_id, PRED.hasParameterContext, id_only=True)
            pd_ids.extend(pdef_ids)


        pf_ids = []
        pc_name_map = {}
        # ParameterContext -> ParameterFunction
        for pd_id in pd_ids:
            pfunc_objs, _ = self.clients.resource_registry.find_objects(pd_id, PRED.hasParameterFunction, id_only=False)
            for pfunc_obj in pfunc_objs:
                pf_ids.append(pfunc_obj._id)
                pc_name_map[pfunc_obj._id] = pfunc_obj.name


        dpds = []
        dpd_name_map = {}
        # DataProcessDefinition -> ParameterFunction
        for pf_id in pf_ids:
            dpdef_objs, _ = self.clients.resource_registry.find_subjects(object=pf_id, 
                                                                        predicate=PRED.hasParameterFunction, 
                                                                        subject_type=RT.DataProcessDefinition, 
                                                                        id_only=False)
            for dpdef_obj in dpdef_objs:
                dpd_name_map[dpdef_obj._id] = pc_name_map[pf_id]
                dpds.append(dpdef_obj)

        for dpd in dpds:
            dp = DataProcess()
            #dp.name = 'Data Process %s for Data Product %s' % ( dpd.name, data_product.name )
            dp.name = dpd_name_map[dpd._id]
            # TODO: This is a stub until DPD is ready
            dp_id, _ = self.clients.resource_registry.create(dp)
            self.clients.resource_registry.create_association(dpd._id, PRED.hasDataProcess, dp_id)
            self.clients.resource_registry.create_association(dp_id, PRED.hasOutputProduct, data_product._id)

    def check_qc(self, data_product):
        '''
        Determine the relevant parameters that need QC applied and create parameters for the evaluations
        '''
        pass

    def assign_stream_definition_to_data_product(self, data_product_id='', stream_definition_id='', exchange_point='', stream_configuration=None):

        validate_is_not_none(data_product_id, 'A data product id must be passed to register a data product')
        validate_is_not_none(stream_definition_id, 'A stream definition id must be passed to assign to a data product')

        stream_def_obj = self.clients.pubsub_management.read_stream_definition(stream_definition_id)  # Validates and checks for param_dict
        parameter_dictionary = stream_def_obj.parameter_dictionary
        validate_is_not_none(parameter_dictionary, 'A parameter dictionary must be passed to register a data product')
        exchange_point = exchange_point or 'science_data'

        data_product = self.RR2.read(data_product_id)

        #if stream_definition_id:
        #@todo: What about topics?

        # Associate the StreamDefinition with the data product
        self.RR2.assign_stream_definition_to_data_product_with_has_stream_definition(stream_definition_id,
                                                                                     data_product_id)
        stream_name = ''
        stream_type = ''
        if stream_configuration is not None:
            stream_name = stream_configuration.stream_name
            stream_type = stream_configuration.stream_type


        stream_id, route = self.clients.pubsub_management.create_stream(name=data_product.name,
                                                                        exchange_point=exchange_point,
                                                                        description=data_product.description,
                                                                        stream_definition_id=stream_definition_id, 
                                                                        stream_name=stream_name,
                                                                        stream_type=stream_type)

        # Associate the Stream with the main Data Product and with the default data product version
        self.RR2.assign_stream_to_data_product_with_has_stream(stream_id, data_product_id)


    def assign_dataset_to_data_product(self, data_product_id='', dataset_id=''):
        validate_is_not_none(data_product_id, 'A data product id must be passed to assign a dataset to a data product')
        validate_is_not_none(dataset_id, 'A dataset id must be passed to assign a dataset to a data product')

        self.RR2.assign_dataset_to_data_product_with_has_dataset(dataset_id, data_product_id)

    def assign_data_product_to_data_product(self, data_product_id='', parent_data_product_id=''):
        validate_true(data_product_id, 'A data product id must be specified')
        validate_true(parent_data_product_id, 'A data product id must be specified')

        self.RR2.assign_data_product_to_data_product_with_has_data_product_parent(parent_data_product_id, data_product_id)


    def read_data_product(self, data_product_id=''):
        """
        method docstring
        """
        # Retrieve all metadata for a specific data product
        # Return data product resource

        data_product = self.RR2.read(data_product_id, RT.DataProduct)

        return data_product


    def update_data_product(self, data_product=None):
        """
        @todo document this interface!!!

        @param data_product    DataProduct
        @throws NotFound    object with specified id does not exist
        """

        # if the geospatial_bounds is set then calculate the geospatial_point_center
        if data_product and data_product.type_ == RT.DataProduct:
            data_product.geospatial_point_center = GeoUtils.calc_geospatial_point_center(data_product.geospatial_bounds)

        original = self.RR2.read(data_product._id)

        self.RR2.update(data_product, RT.DataProduct)
        if self._metadata_changed(original, data_product):
            self.update_catalog_entry(data_product._id)

    def _metadata_changed(self, original_dp, new_dp):
        from ion.processes.data.registration.registration_process import RegistrationProcess
        for field in RegistrationProcess.catalog_metadata:
            if hasattr(original_dp, field) and getattr(original_dp, field) != getattr(new_dp, field):
                return True
        return False




    def delete_data_product(self, data_product_id=''):

        #--------------------------------------------------------------------------------
        # suspend persistence
        #--------------------------------------------------------------------------------
        if self.is_persisted(data_product_id):
            self.suspend_data_product_persistence(data_product_id)
        #--------------------------------------------------------------------------------
        # remove stream associations
        #--------------------------------------------------------------------------------
        stream_ids, assoc_ids = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, True)
        for stream, assoc in zip(stream_ids,assoc_ids):
            self.clients.resource_registry.delete_association(assoc)
            self.clients.pubsub_management.delete_stream(stream_ids[0])

        #--------------------------------------------------------------------------------
        # retire the data product
        #--------------------------------------------------------------------------------
        self.RR2.lcs_delete(data_product_id, RT.DataProduct)


    def force_delete_data_product(self, data_product_id=''):


        #get the assoc producers before deleteing the links
        producer_ids = self.RR2.find_data_producer_ids_of_data_product_using_has_data_producer(data_product_id)

        for producer_id in producer_ids:
            self.RR2.delete(producer_id)

        self.RR2.force_delete(data_product_id, RT.DataProduct)


    def find_data_products(self, filters=None):
        """
        method docstring
        """
        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)

        ret, _ = self.clients.resource_registry.find_resources(RT.DataProduct, None, None, False)
        return ret

    def get_data_product_updates(self, data_product_id_list=None, since_timestamp="" ):

        # For a list of data products, retrieve events since the given timestamp. The return is a dict
        # of dp_id to a dict containing dataset_id, updated: boolean, current geospatial bounds, current temporal bounds.

        event_objs = None
        response_data = {}

        # get the passed parameters. At least the data product id and start_time
        # should be specified
        if data_product_id_list == None or len(data_product_id_list) == 0:
            raise  BadRequest("Please pass a valid data_product_id")

        # Build query structure for fetching events for the data products passed
        try:
            dqb = DatastoreQueryBuilder(datastore=DataStore.DS_EVENTS, profile=DataStore.DS_PROFILE.EVENTS)

            filter_origins = dqb.in_(DQ.EA_ORIGIN, *data_product_id_list)
            filter_types = dqb.in_(DQ.ATT_TYPE, "ResourceModifiedEvent")
            filter_mindate = dqb.gte(DQ.RA_TS_CREATED, since_timestamp)
            where = dqb.and_(filter_origins, filter_types, filter_mindate)

            order_by = dqb.order_by([["ts_created", "desc"]])  # Descending order by time
            dqb.build_query(where=where, order_by=order_by, limit=100000, skip=0, id_only=False)
            query = dqb.get_query()

            event_objs = self.container.event_repository.event_store.find_by_query(query)

        except Exception as ex:
            log.error("Error querying for events for specified data products: %s", ex.message)
            event_objs = []

        # Start populating the response structure
        for data_product_id in data_product_id_list:
            response_data[data_product_id] = {#"dataset_id" : None,
                                              "updated" : False,
                                              "current_geospatial_bounds" : None,
                                              "current_temporal_bounds" : None}

            """ Commented till Maurice decides he needs the dataset ids in the response

            # Need dataset id in response data
            ds_ids,_ = self.clients.resource_registry.find_objects(subject=data_product_id,
                predicate=PRED.hasDataset,
                id_only=True)
            if (ds_ids and len(ds_ids) > 0):
                response_data[data_product_id]["dataset_id"] = ds_ids[0]
            """

            # if we find any UPDATE event for this data_product_id. This type of dumb iteration
            # is slow but since the returned events for all data_product_ids in the list are returned
            # as one big list, there is no way to narrow down the search for UPDATE events
            for event_obj in event_objs:
                if event_obj.origin == data_product_id and event_obj.sub_type == "UPDATE":
                    response_data[data_product_id]["updated"] = True
                    continue

            # Get information about the current geospatial and temporal bounds
            dp_obj = self.clients.resource_registry.read(data_product_id)
            if dp_obj:
                response_data[data_product_id]["current_geospatial_bounds"] = dp_obj.geospatial_bounds
                response_data[data_product_id]["current_temporal_bounds"] = dp_obj.nominal_datetime


        return response_data


    def create_dataset_for_data_product(self, data_product_id=''):
        '''
        Create a dataset for a data product
        '''
        #-----------------------------------------------------------------------------------------
        # Step 1: Collect related resources

        data_product_obj = self.RR2.read(data_product_id)

        validate_is_not_none(data_product_obj, "The data product id should correspond to a valid registered data product.")

        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest('Specified DataProduct has no streams associated with it')
        stream_id = stream_ids[0]

        stream_defs, _ = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition,id_only=True)
        if not stream_defs:
            raise BadRequest("Data Product stream is without a stream definition")
        stream_def_id = stream_defs[0]

        stream_def = self.clients.pubsub_management.read_stream_definition(stream_def_id)  # additional read necessary to fill in the pdict

        parent_data_product_ids, _ = self.clients.resource_registry.find_objects(data_product_id, predicate=PRED.hasDataProductParent, id_only=True)
        if len(parent_data_product_ids) == 1:  # This is a child data product
            raise BadRequest("Child Data Products shouldn't be activated")

        child_data_product_ids, _ = self.clients.resource_registry.find_subjects(object=data_product_id, predicate=PRED.hasDataProductParent, id_only=True)


        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, predicate=PRED.hasDataset, id_only=True)

        #-----------------------------------------------------------------------------------------
        # Step 2: Create and associate Dataset (coverage)


        if not dataset_ids:
            # No datasets are currently linked which means we need to create a new one
            dataset_id = self.clients.dataset_management.create_dataset(name= 'dataset_%s' % stream_id,
                                                                        stream_id=stream_id,
                                                                        parameter_dict=stream_def.parameter_dictionary)

            # link dataset with data product. This creates the association in the resource registry
            self.RR2.assign_dataset_to_data_product_with_has_dataset(dataset_id, data_product_id)

            # Late binding of dataset with existing child data products
            for child_dp_id in child_data_product_ids:
                self.assign_dataset_to_data_product(child_dp_id, dataset_id)
            
            # register the dataset for externalization

            self.create_catalog_entry(data_product_id=data_product_id)
            child_products, _ = self.clients.resource_registry.find_subjects(object=data_product_id, predicate=PRED.hasDataProductParent, id_only=True)
            for child_product in child_products:
                self.create_catalog_entry(data_product_id=data_product_id)
        else:
            dataset_id = dataset_ids[0]
        return dataset_id


    def activate_data_product_persistence(self, data_product_id=''):
        """Persist data product data into a data set

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """
        #-----------------------------------------------------------------------------------------
        # Step 1: Collect related resources

        data_product_obj = self.RR2.read(data_product_id)

        validate_is_not_none(data_product_obj, "The data product id should correspond to a valid registered data product.")
        
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest('Specified DataProduct has no streams associated with it')
        stream_id = stream_ids[0]

        dataset_id = self.create_dataset_for_data_product(data_product_id)


        #-----------------------------------------------------------------------------------------
        # Step 2: Configure and start ingestion with lookup values

        # grab the ingestion configuration id from the data_product in order to use to persist it
        if data_product_obj.dataset_configuration_id:
            ingestion_configuration_id = data_product_obj.dataset_configuration_id
        else:
            ingestion_configuration_id = self.clients.ingestion_management.list_ingestion_configurations(id_only=True)[0]

        # Identify lookup tables
        config = DotDict()
        if self._has_lookup_values(data_product_id):
            config.process.input_product = data_product_id
            config.process.lookup_docs = self._get_lookup_documents(data_product_id)

        # persist the data stream using the ingestion config id and stream id

        # find datasets for the data product
        dataset_id = self.clients.ingestion_management.persist_data_stream(stream_id=stream_id,
                                                ingestion_configuration_id=ingestion_configuration_id,
                                                dataset_id=dataset_id,
                                                config=config)

        #--------------------------------------------------------------------------------
        # todo: dataset_configuration_obj contains the ingest config for now...
        # Update the data product object and sent event
        #--------------------------------------------------------------------------------
        data_product_obj.dataset_configuration_id = ingestion_configuration_id
        self.update_data_product(data_product_obj)

        self._publish_persist_event(data_product_id=data_product_id, persist_on = True)
        self.create_data_processes(data_product_id)

    def is_persisted(self, data_product_id=''):
        # Is the data product currently persisted into a data set?
        try:
            if data_product_id:
                stream_id = self.RR2.find_stream_id_of_data_product_using_has_stream(data_product_id)
                return self.clients.ingestion_management.is_persisted(stream_id)
        except NotFound:
            pass

        return False

    def _publish_persist_event(self, data_product_id=None, persist_on=True):
        try:
            if data_product_id:
                if persist_on:
                    persist_type = 'PERSIST_ON'
                    description = 'Data product is persisted.'
                else:
                    persist_type= 'PERSIST_OFF'
                    description= 'Data product is not currently persisted'

                pub = EventPublisher(OT.InformationContentStatusEvent, process=self)
                event_data = dict(origin_type=RT.DataProduct,
                                  origin=data_product_id or "",
                                  sub_type=persist_type,
                                  status = InformationStatus.NORMAL,
                                  description = description)
                pub.publish_event(**event_data)
        except Exception as ex:
            log.error("Error publishing InformationContentStatusEvent for data product: %s", data_product_id)

    def suspend_data_product_persistence(self, data_product_id=''):
        """Suspend data product data persistence into a data set, multiple options

        @param data_product_id    str
        @throws NotFound    object with specified id does not exist
        """

        #--------------------------------------------------------------------------------
        # retrieve the data_process object
        #--------------------------------------------------------------------------------
        data_product_obj = self.clients.resource_registry.read(data_product_id)

        validate_is_not_none(data_product_obj, 'Should not have been empty')
        validate_is_instance(data_product_obj, DataProduct)

        parent_dp_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataProductParent, id_only=True)

        if not data_product_obj.dataset_configuration_id:
            if parent_dp_ids:
                # It's a derived data product, we're done here
                return
            raise NotFound("Data Product %s dataset configuration does not exist" % data_product_id)

        #--------------------------------------------------------------------------------
        # get the Stream associated with this data product; if no stream then create one, if multiple streams then Throw
        #streams = self.data_product.find_stemming_stream(data_product_id)
        #--------------------------------------------------------------------------------

        # if this data product is not currently being persisted, then just flag with a warning.
        if self.is_persisted(data_product_id):

            try:
                log.debug("Attempting to find stream")
                stream_id = self.RR2.find_stream_id_of_data_product_using_has_stream(data_product_id)
                log.debug("stream found")
                validate_is_not_none(stream_id, 'Data Product %s must have one stream associated' % str(data_product_id))

                self.clients.ingestion_management.unpersist_data_stream(stream_id=stream_id,
                                                    ingestion_configuration_id=data_product_obj.dataset_configuration_id)

                self._publish_persist_event(data_product_id=data_product_id, persist_on=False)

            except NotFound:
                if data_product_obj.lcstate == LCS.DELETED:
                    log.debug("stream not found, but assuming it was from a deletion")
                    log.error("Attempted to suspend_data_product_persistence on a retired data product")
                else:
                    log.debug("stream not found, assuming error")
                    raise

        else:
            log.warning('Data product is not currently persisted, no action taken: %s', data_product_id)

    def add_parameter_to_data_product(self, parameter_context_id='', data_product_id=''):
        pc = self.clients.dataset_management.read_parameter_context(parameter_context_id)

        stream_def_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=False)
        stream_def = stream_def_ids[0]
        pdict_ids, _ = self.clients.resource_registry.find_objects(stream_def._id, PRED.hasParameterDictionary,id_only=True)
        pdict_id = pdict_ids[0]
        self.clients.resource_registry.create_association(subject=pdict_id, predicate=PRED.hasParameterContext, object=parameter_context_id)
        if stream_def.available_fields:
            stream_def.available_fields.append(pc.name)
            self.clients.resource_registry.update(stream_def)

        datasets, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)
        if not datasets:
            raise BadRequest("No associated dataset, please ensure that this data product is activated")
        
        dataset_id = datasets[0]

        self.clients.dataset_management.add_parameter_to_dataset(parameter_context_id, dataset_id)

        #TODO: we need a better way of updating the XML files
        config = DotDict()
        config.op = 'register_datasets'
        self.container.spawn_process('refresh_catalog', 'ion.processes.bootstrap.registration_bootstrap', 'RegistrationBootstrap', config)


        #--------------------------------------------------------------------------------
        # detach the dataset from this data product
        #--------------------------------------------------------------------------------
#        dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
#        for dataset_id in dataset_ids:
#            self.data_product.unlink_data_set(data_product_id, dataset_id)

    def _get_reference_designator(self, data_product_id=''):
        '''
        Returns the reference designator for a data product if it has one
        '''

        device_ids, _ = self.clients.resource_registry.find_subjects(object=data_product_id, predicate=PRED.hasOutputProduct, subject_type=RT.InstrumentDevice, id_only=True)
        if not device_ids: 
            raise BadRequest("No instrument device associated with this data product")
        device_id = device_ids[0]

        sites, _ = self.clients.resource_registry.find_subjects(object=device_id, predicate=PRED.hasDevice, subject_type=RT.InstrumentSite, id_only=False)
        if not sites:
            raise BadRequest("No site is associated with this data product")
        site = sites[0]
        rd = site.reference_designator
        return rd


    def get_data_product_stream_definition(self, data_product_id=''):
        self.read_data_product(data_product_id)
        streams, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        for stream in streams:
            stream_defs, _ = self.clients.resource_registry.find_objects(subject=stream, predicate=PRED.hasStreamDefinition, id_only=True)
            if stream_defs:
                return stream_defs[0]


    def get_data_product_provenance(self, data_product_id=''):

        # Retrieve information that characterizes how this data was produced
        # Return in a dictionary

        # There are two parts to the provenance tree as returned by this method. The first part is
        # a path that follows the DataProduct to its parent DataProduct .. all the way to the InstrumentAgent
        # The second part is along the parameters contained within the DataProducts. The Provenance along the
        # parameters can follow its own pathways.
        self.provenance_results = {}

        validate_is_not_none(data_product_id, 'A data product identifier must be passed to create a provenance report')

        # Walk up the DataProduct tree
        def resource_traversal(resource_id, result):

            #Get data product object to verify it exists and what type it is
            if resource_id not in result.keys():
                result[resource_id] = {'parents' : {}, 'type' : None, 'parameter_provenance': {}}
            #parent_info = []

            # determine the type of resource. This will determine what parents to look for
            resource_obj = self.clients.resource_registry.read(resource_id)
            if resource_obj == None:
                raise BadRequest('Resource object does not exist.')
                return
            result[resource_id]['type'] = resource_obj.type_

            if result[resource_id]['type'] == "InstrumentDevice" or \
               result[resource_id]['type'] == 'PlatformDevice':
                # Do nothing. We have reached the top of the tree
                return

            # if the resource is a dataproduct, check for parent dataproduct, data process or Instrument/Platform/Dataset Agent
            if result[resource_id]['type'] == "DataProduct":
                # If its a derived data product, it should have a parent
                parent_data_product_ids,_ = self.clients.resource_registry.find_objects(subject=resource_id,
                                                                            object_type=RT.DataProduct,
                                                                            predicate=PRED.hasDataProductParent,
                                                                            id_only=True)

                # recurse if we found data product parents
                if(parent_data_product_ids !=None and len(parent_data_product_ids) > 0):
                    for _id in parent_data_product_ids:
                        result[resource_id]['parents'][_id] = {'data_process_definition_id' : None,
                                                                'data_process_definition_name' : None,
                                                                'data_process_definition_rev' : None}

                    #recurse
                    for _id in parent_data_product_ids:
                        resource_traversal(_id, result)
                    #return

                # Code reaches here if no parents were found.
                # Try the hasOutputProduct association with a dataprocess (from a transform
                # func or retrieve process etc)
                parent_data_process_ids,_ = self.clients.resource_registry.find_subjects(object=resource_id,
                                                                            subject_type=RT.DataProcess,
                                                                            predicate=PRED.hasOutputProduct,
                                                                            id_only=True)

                # Add the data Process definitions as parents and their input data product as the parent
                if (parent_data_process_ids != None and len(parent_data_process_ids) > 0):
                    for parent_process_id in parent_data_process_ids:
                        parent_dpd_objs,_ = self.clients.resource_registry.find_subjects(object=parent_process_id,
                                                                            subject_type=RT.DataProcessDefinition,
                                                                            predicate=PRED.hasDataProcess,
                                                                            id_only=False)

                        if (parent_dpd_objs == None or len(parent_dpd_objs) == 0):
                            raise BadRequest('Could not locate Data Process Definition')
                            return

                        #Check to see what type of data_process_type was associated with it. If its a TRANSFORM_PROCESS
                        # or RETRIEVE_PROCESS, treat it as a parent. If its a PARAMETER_FUNCTION, add it to the parameter
                        # provenance.

                        if parent_dpd_objs[0].data_process_type == DataProcessTypeEnum.TRANSFORM_PROCESS or \
                           parent_dpd_objs[0].data_process_type == DataProcessTypeEnum.RETRIEVE_PROCESS :
                            # Whats the input data product ?
                            input_to_data_process_ids,_ = self.clients.resource_registry.find_objects(subject=parent_process_id,
                                                                                                    object_type=RT.DataProduct,
                                                                                                    predicate=PRED.hasInputProduct,
                                                                                                    id_only=True)

                            # Add information about the parent
                            for _id in input_to_data_process_ids:
                                result[resource_id]['parents'][_id] = {'data_process_definition_id' : parent_dpd_objs[0]._id,
                                                                    'data_process_definition_name' : parent_dpd_objs[0].name,
                                                                    'data_process_definition_rev' : parent_dpd_objs[0]._rev}

                            # recurse
                            for _id in input_to_data_process_ids:
                                resource_traversal(_id, result)

                        # In case of a parameter function, follow parameter provenance
                        if parent_dpd_objs[0].data_process_type == DataProcessTypeEnum.PARAMETER_FUNCTION:
                            input_params = parent_dpd_objs[0].parameters

                            result[resource_id]['parameter_provenance'][parent_process_id] = \
                                                                                {'data_process_definition_id' : parent_dpd_objs[0]._id,
                                                                                'data_process_definition_name' : parent_dpd_objs[0].name,
                                                                                'data_process_definition_rev' : parent_dpd_objs[0]._rev,
                                                                                'parameters' : input_params}

                    #return



                # If code reaches here, we still have not found any parents, maybe we have reached a parsed data product,
                # in which case we want to follow a link to an InstrumentDevice or PlatformDeveice
                # via a hasOutputProduct predicate
                instrument_device_ids,_ = self.clients.resource_registry.find_subjects(object=resource_id,
                                                                                    subject_type=RT.InstrumentDevice,
                                                                                    predicate=PRED.hasOutputProduct,
                                                                                    id_only=True)
                platform_device_ids,_ = self.clients.resource_registry.find_subjects(object=resource_id,
                                                                                    subject_type=RT.PlatformDevice,
                                                                                    predicate=PRED.hasOutputProduct,
                                                                                    id_only=True)
                source_device_ids = instrument_device_ids + platform_device_ids

                if (source_device_ids != None and len(source_device_ids) > 0):
                    for _id in source_device_ids:
                        result[resource_id]['parents'][_id]={'data_process_definition_id' : None,
                                                             'data_process_definition_name' : None,
                                                             'data_process_definition_rev' : None}

                    for _id in source_device_ids:
                        resource_traversal(_id, result)
                    #return
                else:
                    # log an error for not being able to find the source instrument
                    log.error("Could not locate the source device for :" + resource_id)


        resource_traversal(data_product_id, self.provenance_results)

        # We are actually interested in the DataProcessDefinitions for the DataProcess. Find those
        return self.provenance_results


    def get_data_product_parameter_provenance(self, data_product_id='', parameter_name=''):
        # Provides an audit trail for modifications to the original data
        provenance_image = StringIO.StringIO()

        #first get the assoc stream definition
        stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStreamDefinition, object_type=RT.StreamDefinition)
        if not stream_def_ids:
            raise BadRequest('No stream definitions found for this data product: %s', data_product_id)
        else:
            param_dict_ids, _ = self.clients.resource_registry.find_objects(subject=stream_def_ids[0], predicate=PRED.hasParameterDictionary, object_type=RT.ParameterDictionary)
            if not param_dict_ids:
                raise BadRequest('No parameter dictionary found for this data product: %s', data_product_id)
            else:
                #context = self.clients.dataset_management.read_parameter_context_by_name(parameter_context_id)
                pdict = DatasetManagementService.get_parameter_dictionary(param_dict_ids[0]._id)
                context = pdict.get_context(parameter_name)
                #log.debug('get_data_product_parameter_provenance  context: %s ', context)
                if hasattr(context, 'param_type'):
                    graph = context.param_type.get_dependency_graph()
                    pos=nx.spring_layout(graph)
                    nx.draw(graph, pos, font_size=10)

                    plt.savefig(provenance_image)
                    provenance_image.seek(0)
                else:
                    raise BadRequest('Invalid paramter context found for this data product: %s', data_product_id)

        return provenance_image.getvalue()

    def _registration_rpc(self, op, data_product_id):
        procs,_ = self.clients.resource_registry.find_resources(restype=RT.Process, id_only=True)
        pid = None
        for p in procs:
            if 'registration_worker' in p:
                pid = p
        if not pid: 
            log.warning('No registration worker found')
            return
        rpc_cli = RPCClient(to_name=pid)
        return rpc_cli.request({'data_product_id':data_product_id}, op=op)

    def create_catalog_entry(self, data_product_id=''):
        # Stub
        return self._registration_rpc('create_entry',data_product_id) 

    def read_catalog_entry(self, data_product_id=''):
        # Stub
        return self._registration_rpc('read_entry', data_product_id)

    def update_catalog_entry(self, data_product_id=''):
        # Stub
        return self._registration_rpc('update_entry', data_product_id)

    def delete_catalog_entry(self, data_product_id=''):
        # Stub
        return self._registration_rpc('delete_entry', data_product_id)


    ############################
    #
    #  Data Product Collections
    #
    ############################


    def create_data_product_collection(self, data_product_id='', collection_name='', collection_description=''):
        """Define a  set of an existing data products that represent an improvement in the quality or
        understanding of the information.
        """
        validate_is_not_none(data_product_id, 'A data product identifier must be passed to create a data product version')

        dpv = DataProductVersion()
        dpv.name = 'base'
        dpv.description = 'the base version on which subsequent versions are built'
        dpv.data_product_id = data_product_id

        dp_collection_obj = IonObject(RT.DataProductCollection, name=collection_name, description=collection_description, version_list=[dpv])

        data_product_collection_id, rev = self.clients.resource_registry.create(dp_collection_obj)
        self.clients.resource_registry.create_association( subject=data_product_collection_id, predicate=PRED.hasVersion, object=data_product_id)

        return data_product_collection_id



    def update_data_product_collection(self, data_product_collection=None):
        """@todo document this interface!!!

        @param data_product_collection    DataProductCollection
        @throws NotFound    object with specified id does not exist
        """


        self.RR2.update(data_product_collection, RT.DataProductCollection)

        #TODO: any changes to producer? Call DataAcquisitionMgmtSvc?

        return

    def read_data_product_collection(self, data_product_collection_id=''):
        """Retrieve data product information

        @param data_product_collection_id    str
        @retval data_product    DataProductVersion
        """
        result = self.RR2.read(data_product_collection_id, RT.DataProductCollection)

        return result

    def delete_data_product_collection(self, data_product_collection_id=''):
        """Remove a version of an data product.

        @param data_product_collection_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """

        self.RR2.lcs_delete(data_product_collection_id, RT.DataProductCollection)

    def force_delete_data_product_collection(self, data_product_collection_id=''):

        # if not yet deleted, the first execute delete logic
        dp_obj = self.read_data_product_collection(data_product_collection_id)
        if dp_obj.lcstate != LCS.DELETED:
            self.delete_data_product_collection(data_product_collection_id)

        self.RR2.force_delete(data_product_collection_id, RT.DataProductCollection)

    def add_data_product_version_to_collection(self, data_product_id='', data_product_collection_id='', version_name='', version_description=''):
        dp_collection_obj =self.clients.resource_registry.read(data_product_collection_id)

        #retrieve the stream definition for both the new data product to add to this collection and the base data product for this collection
        new_data_product_obj = self.clients.resource_registry.read(data_product_id)
        new_data_product_streams, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_is_not_none(new_data_product_streams, 'The data product to add to the collection must have an associated stream')
        new_data_product_streamdefs, _ = self.clients.resource_registry.find_objects(subject=new_data_product_streams[0], predicate=PRED.hasStreamDefinition, object_type=RT.StreamDefinition, id_only=True)

        base_data_product_id = dp_collection_obj.version_list[0].data_product_id
        base_data_product_obj = self.clients.resource_registry.read(base_data_product_id)
        base_data_product_streams, _ = self.clients.resource_registry.find_objects(subject=base_data_product_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        validate_is_not_none(base_data_product_streams, 'The base data product in the collection must have an associated stream')
        base_data_product_streamdefs, _ = self.clients.resource_registry.find_objects(subject=base_data_product_streams[0], predicate=PRED.hasStreamDefinition, object_type=RT.StreamDefinition, id_only=True)
        if not self.clients.pubsub_management.compare_stream_definition(stream_definition1_id=new_data_product_streamdefs[0], stream_definition2_id=base_data_product_streamdefs[0]):
            raise BadRequest("All Data Products in a collection must have equivelent stream definitions.")

        #todo: validate that the spatial/temporal domain match the base data product

        dpv = DataProductVersion()
        dpv.name = version_name
        dpv.description = version_description
        dpv.data_product_id = data_product_id

        dp_collection_obj.version_list.append(dpv)
        self.clients.resource_registry.update(dp_collection_obj)

        self.clients.resource_registry.create_association( subject=data_product_collection_id, predicate=PRED.hasVersion, object=data_product_id)

        return


    def get_current_version(self, data_product_collection_id=''):
        data_product_collection_obj = self.clients.resource_registry.read(data_product_collection_id)
        count = len (data_product_collection_obj.version_list)
        dpv_obj = data_product_collection_obj.version_list[count - 1]
        return dpv_obj.data_product_id


    def get_base_version(self, data_product_collection_id=''):
        data_product_collection_obj = self.clients.resource_registry.read(data_product_collection_id)
        dpv_obj = data_product_collection_obj.version_list[0]
        return dpv_obj.data_product_id


    def execute_data_product_lifecycle(self, data_product_id="", lifecycle_event=""):
        """
        declare a data_product to be in a given state
        @param data_product_id the resource id
        """
        return self.RR2.advance_lcs(data_product_id, lifecycle_event)



    def get_data_product_group_list(self, org_id=''):
        group_names = set()

        res_ids, keys = self.clients.resource_registry.find_resources_ext(RT.DataProduct, attr_name="ooi_product_name", id_only=True)
        for key in keys:
            group_name = key.get('attr_value', None)
            if group_name:
                group_names.add(group_name)

        return sorted(list(group_names))


    def _get_dataset_id(self, data_product_id=''):
        # find datasets for the data product
        dataset_id = ''
        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasDataset, RT.Dataset, id_only=True)
        if dataset_ids:
            dataset_id = dataset_ids[0]
        else:
            raise NotFound('No Dataset is associated with DataProduct %s' % data_product_id)
        return dataset_id


    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################



    def get_data_product_extension(self, data_product_id='', ext_associations=None, ext_exclude=None, user_id=''):
        #Returns an DataProductExtension object containing additional related information

        if not data_product_id:
            raise BadRequest("The data_product_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_product = extended_resource_handler.create_extended_resource_container(
            extended_resource_type=OT.DataProductExtension,
            resource_id=data_product_id,
            computed_resource_type=OT.DataProductComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        #Loop through any attachments and remove the actual content since we don't need
        #   to send it to the front end this way
        #TODO - see if there is a better way to do this in the extended resource frame work.
        if hasattr(extended_product, 'attachments'):
            for att in extended_product.attachments:
                if hasattr(att, 'content'):
                    delattr(att, 'content')

        #extract the list of upstream data products from the provenance results
#        dp_list = []
#        for key, value in extended_product.computed.provenance.value.iteritems():
#            for producer_id, dataprodlist in value['inputs'].iteritems():
#                for dataprod in dataprodlist:
#                    dp_list.append( self.clients.resource_registry.read(dataprod) )
#        extended_product.provenance_product_list = list ( set(dp_list) ) #remove dups in list

        #set the data_ingestion_datetime from get_data_datetime
        if extended_product.computed.data_datetime.status == ComputedValueAvailability.PROVIDED :
            extended_product.data_ingestion_datetime =  extended_product.computed.data_datetime.value[1]

        #get the dataset size in MB
        extended_product.computed.product_download_size_estimated = self._get_product_dataset_size(data_product_id)
        #covert to bytes for stored_data_size attribute
        extended_product.computed.stored_data_size.value = int(extended_product.computed.product_download_size_estimated.value * 1048576)
        extended_product.computed.stored_data_size.status = extended_product.computed.product_download_size_estimated.status
        extended_product.computed.stored_data_size.reason = extended_product.computed.product_download_size_estimated.reason


        # divide up the active and past user subscriptions
        active = []
        nonactive = []
        for notification_obj in extended_product.computed.active_user_subscriptions.value:
            if notification_obj.lcstate == LCS.DELETED:
                nonactive.append(notification_obj)
            else:
                active.append(notification_obj)

        extended_product.computed.active_user_subscriptions.value = active
        extended_product.computed.past_user_subscriptions.value = nonactive
        extended_product.computed.past_user_subscriptions.status = ComputedValueAvailability.PROVIDED
        extended_product.computed.number_active_subscriptions.value = len(active)
        extended_product.computed.number_active_subscriptions.status = ComputedValueAvailability.PROVIDED

        # replace list of lists with single list
        replacement_data_products = []
        for inner_list in extended_product.process_input_data_products:
            if inner_list:
                for actual_data_product in inner_list:
                    if actual_data_product:
                        replacement_data_products.append(actual_data_product)
        extended_product.process_input_data_products = replacement_data_products

        from ion.util.extresource import strip_resource_extension, get_matchers, matcher_ParameterContext
        matchers = get_matchers([matcher_ParameterContext])
        strip_resource_extension(extended_product, matchers=matchers)

        return extended_product


    def get_data_datetime(self, data_product_id=''):
        # Returns a temporal bounds object of the span of data product life span (may exist without getting a granule)
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        ret.status = ComputedValueAvailability.NOTAVAILABLE

        try:
            dataset_id = self._get_dataset_id(data_product_id)
            bounds = self.clients.dataset_management.dataset_bounds(dataset_id)
            if 'time' in bounds and len(bounds['time']) == 2 :
                log.debug("get_data_datetime bounds['time']: %s"  % str(dataset_id))
                timeStart = IonTime(bounds['time'][0]  -  IonTime.JAN_1970)
                timeEnd = IonTime(bounds['time'][1]  -  IonTime.JAN_1970)
                ret.value = [str(timeStart), str(timeEnd)]
                ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"
        except Exception as e:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not calculate time range for this data product"

        return ret


    def _get_product_dataset_size(self, data_product_id=''):
        # Returns the size of the full data product if downloaded/presented in a given presentation form
        ret = IonObject(OT.ComputedFloatValue)
        ret.value = 0
        try:
            dataset_id = self._get_dataset_id(data_product_id)
            size_in_bytes = self.clients.dataset_management.dataset_size(dataset_id, in_bytes=False)
            ret.status = ComputedValueAvailability.PROVIDED
            ret.value = size_in_bytes
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"
        except Exception as e:
            raise e

        return ret


    def get_data_contents_updated(self, data_product_id=''):
        # the datetime when the contents of the data were last modified in any way.
        # This is distinct from modifications to the data product attributes
        ret = IonObject(OT.ComputedStringValue)
        ret.value = ""
        ret.status = ComputedValueAvailability.NOTAVAILABLE
        ret.reason = "Currently need to retrieve form the coverage"

        return ret


    def get_parameters(self, data_product_id=''):
        # The set of Parameter objects describing each variable in this data product
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        try:
            stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStreamDefinition, id_only=True)
            if not stream_def_ids:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "There is no StreamDefinition associated with this DataProduct"
                return ret
            stream_def = self.clients.pubsub_management.read_stream_definition(stream_definition_id=stream_def_ids[0])

            param_dict_ids, _ = self.clients.resource_registry.find_objects(subject=stream_def_ids[0], predicate=PRED.hasParameterDictionary, id_only=True)
            if not param_dict_ids:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "There is no ParameterDictionary associated with this DataProduct"
            else:
                ret.status = ComputedValueAvailability.PROVIDED
                if stream_def.available_fields:
                    retval = [i for i in self.clients.dataset_management.read_parameter_contexts(param_dict_ids[0]) if i.name in stream_def.available_fields]
                else:
                    retval = self.clients.dataset_management.read_parameter_contexts(param_dict_ids[0])
                retval = filter(lambda x : 'visible' not in x.parameter_context or x.parameter_context['visible'], retval)
                ret.value = retval
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
        except Exception as e:
            raise e

        return ret

    def get_data_url(self, data_product_id=''):
        # The unique pointer to this set of data
        ret = IonObject(OT.ComputedStringValue)
        ret.value  = ""

        erddap_host = CFG.get_safe('server.erddap.host','localhost')
        errdap_port = CFG.get_safe('server.erddap.port','8080')
        try:
            ret.value  = string.join( ["http://", erddap_host, ":", str(errdap_port),"/erddap/tabledap/", "data", str(data_product_id), ".html"],'')
            ret.status = ComputedValueAvailability.PROVIDED
            log.debug("get_data_url: data_url: %s", ret.value)
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Dataset for this Data Product could not be located"

        return ret

    def get_provenance(self, data_product_id=''):
        # Provides an audit trail for modifications to the original data
        ret = IonObject(OT.ComputedDictValue)
        ret.status = ComputedValueAvailability.NOTAVAILABLE
        ret.reason = "Provenance not currently used."

        ret = IonObject(OT.ComputedDictValue)

#        try:
#            ret.value = self.get_data_product_provenance(data_product_id)
#            ret.status = ComputedValueAvailability.PROVIDED
#        except NotFound:
#            ret.status = ComputedValueAvailability.NOTAVAILABLE
#            ret.reason = "Error in DataProuctMgmtService:get_data_product_provenance"
#        except Exception as e:
#            raise e

        return ret



    def get_active_user_subscriptions(self, data_product_id=''):
        # The UserSubscription objects for this data product
        ret = IonObject(OT.ComputedListValue)
        ret.value = []
        try:
            ret.value = self.clients.user_notification.get_subscriptions(resource_id=data_product_id, include_nonactive=True)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Product subscription infromation not provided by UserNotificationService"
        except Exception as e:
            raise e

        return ret


    def get_last_granule(self, data_product_id=''):
        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated
        ret = IonObject(OT.ComputedDictValue)
        ret.value = {}
        try:
            dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
            if not dataset_ids:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "No dataset associated with this data product"
                return ret

            stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStreamDefinition, id_only=True)
            if not stream_def_ids:
                ret.status = ComputedValueAvailability.NOTAVAILABLE
                ret.reason = "No stream definition associated with this data product"
                return ret

            #stream_def_id = stream_def_ids[0]

            #replay_granule = self.clients.data_retriever.retrieve_last_data_points(dataset_ids[0], number_of_points=1, delivery_format=stream_def_id)
            #replay_granule = self.clients.data_retriever.retrieve_last_granule(dataset_ids[0])
            rdt = ParameterHelper.rdt_for_data_product(data_product_id)
            values = self.clients.dataset_management.dataset_latest(dataset_ids[0])
            for k,v in values.iteritems():
                if k in rdt:
                    rdt[k] = [v]

            retval = {}
            for k,v in rdt.iteritems():
                if hasattr(rdt.context(k),'visible') and not rdt.context(k).visible:
                    continue
                if k.endswith('_qc') and not k.endswith('glblrng_qc'):
                    continue
                element = np.atleast_1d(rdt[k]).flatten()[0]
                if element == rdt._pdict.get_context(k).fill_value:
                    retval[k] = '%s: Empty' % k
                elif rdt._pdict.get_context(k).uom and 'seconds' in rdt._pdict.get_context(k).uom:
                    units = rdt._pdict.get_context(k).uom
                    element = np.atleast_1d(rdt[k]).flatten()[0]
                    unix_ts = TimeUtils.units_to_ts(units, element)
                    dtg = datetime.utcfromtimestamp(unix_ts)
                    try:
                        retval[k] = '%s: %s' %(k,dtg.strftime('%Y-%m-%dT%H:%M:%SZ'))
                    except:
                        retval[k] = '%s: %s' %(k, element)
                elif isinstance(element, float) or (isinstance(element,np.number) and element.dtype.char in 'edfg'):
                    try:
                        precision = int(rdt.context(k).precision)
                    except ValueError:
                        precision = 5
                    except TypeError: # None results in a type error
                        precision = 5
                    formatted = ("{0:.%df}" % precision).format(round(element,precision))
                    retval[k] = '%s: %s' %(k, formatted)
                else:
                    retval[k] = '%s: %s' % (k,element)
            ret.value = retval
#                ret.value =  {k : str(rdt[k].tolist()[0]) for k,v in rdt.iteritems()}
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
        except Exception as e:
            raise e

        return ret


    def get_recent_granules(self, data_product_id=''):
        # Provides information for users who have in the past acquired this data product, but for which that acquisition was terminated
        ret = IonObject(OT.ComputedDictValue)
        ret.value = {}
        ret.status = ComputedValueAvailability.NOTAVAILABLE
#        try:
#            dataset_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataset, id_only=True)
#            if not dataset_ids:
#                ret.status = ComputedValueAvailability.NOTAVAILABLE
#                ret.reason = "No dataset associated with this data product"
#            else:
#                replay_granule = self.clients.data_retriever.retrieve_last_data_points(dataset_ids[0])
#                rdt = RecordDictionaryTool.load_from_granule(replay_granule)
#                ret.value =  {k : rdt[k].tolist() for k,v in rdt.iteritems()}
#                ret.status = ComputedValueAvailability.PROVIDED
#        except NotFound:
#            ret.status = ComputedValueAvailability.NOTAVAILABLE
#            ret.reason = "FIXME: this message should say why the calculation couldn't be done"
#        except Exception as e:
#            raise e

        return ret


    def get_is_persisted(self, data_product_id=''):
        # Returns True if data product is currently being persisted
        ret = IonObject(OT.ComputedIntValue)
        ret.value = self.is_persisted(data_product_id)
        ret.status = ComputedValueAvailability.PROVIDED

        return ret

    def _has_lookup_values(self, data_product_id):
        stream_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)
        if not stream_ids:
            raise BadRequest('No streams found for this data product')
        stream_def_ids, _ = self.clients.resource_registry.find_objects(subject=stream_ids[0], predicate=PRED.hasStreamDefinition, id_only=True)
        if not stream_def_ids:
            raise BadRequest('No stream definitions found for this stream')
        
        return self.clients.pubsub_management.has_lookup_values(stream_definition_id=stream_def_ids[0])

    def _get_lookup_documents(self, data_product_id):
        return self.clients.data_acquisition_management.list_qc_references(data_product_id)



    def _format_ion_time(self, ion_time=''):
        ion_time_obj = IonTime.from_string(ion_time)
        #todo: fix this and return str( ion_time_obj)
        return str(ion_time_obj)

    ############################
    #
    #  PREPARE UPDATE RESOURCES
    #
    ############################


    def prepare_data_product_support(self, data_product_id=''):
        """
        Returns the object containing the data to update an instrument device resource
        """

        #TODO - does this have to be filtered by Org ( is an Org parameter needed )
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(data_product_id, OT.DataProductPrepareSupport)



        #Fill out service request information for creating a platform device
        extended_resource_handler.set_service_requests(resource_data.create_request, 'data_product_management',
            'create_data_product_', { "data_product":  "$(data_product)" })

        #Fill out service request information for creating a platform device
        extended_resource_handler.set_service_requests(resource_data.update_request, 'data_product_management',
            'update_data_product', { "data_product":  "$(data_product)" })

        #Fill out service request information for activating a platform device
        extended_resource_handler.set_service_requests(resource_data.activate_request, 'data_product_management',
            'activate_data_product_persistence', { "data_product":  "$(data_product)" })

        #Fill out service request information for deactivating a platform device
        extended_resource_handler.set_service_requests(resource_data.deactivate_request, 'data_product_management',
            'suspend_data_product_persistence', { "data_product":  "$(data_product)" })

        #Fill out service request information for assigning a stream definition
        extended_resource_handler.set_service_requests(resource_data.associations['StreamDefinition'].assign_request, 'data_product_management',
            'assign_stream_definition_to_data_product', { "data_product_id": data_product_id,
                                                        "stream_definition_id": "$(stream_definition_id)",
                                                        "exchange_point": "$(exchange_point)" })


        #Fill out service request information for assigning a dataset
        extended_resource_handler.set_service_requests(resource_data.associations['Dataset'].assign_request, 'data_product_management',
            'assign_dataset_to_data_product', { "data_product_id": data_product_id,
                                                "dataset_id": "$(dataset_id)" })


        #Fill out service request information for assigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDeviceHasOutputProduct'].assign_request, 'data_acquisition_management',
            'assign_data_product', {"data_product_id": data_product_id,
                                    "input_resource_id": "$(instrument_device_id)"})


        #Fill out service request information for unassigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDeviceHasOutputProduct'].unassign_request, 'data_acquisition_management',
            'unassign_data_product', {"data_product_id": data_product_id,
                                      "input_resource_id": "$(instrument_device_id)" })



        #Fill out service request information for assigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['PlatformDevice'].assign_request, 'data_acquisition_management',
            'assign_data_product', {"data_product_id": data_product_id,
                                    "input_resource_id": "$(platform_device_id)"})


        #Fill out service request information for unassigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['PlatformDevice'].unassign_request, 'data_acquisition_management',
            'unassign_data_product', {"data_product_id": data_product_id,
                                      "input_resource_id": "$(platform_device_id)" })


        # DataProduct hasSource InstrumentDevice*
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDeviceHasSource'].assign_request,
                                                       'data_acquisition_management',
                                                       'assign_data_product_source',
                                                       {'data_product_id': data_product_id,
                                                        'source_id': '$(data_product_id)'}) # yes this is odd, but its the variable name we want to substitute based on resource_identifier (i'm not sure where that is set)

        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDeviceHasSource'].unassign_request,
                                                       'data_acquisition_management',
                                                       'unassign_data_product_source',
                                                       {'data_product_id': data_product_id,
                                                        'source_id': '$(data_product_id)'})

        resource_data.associations['InstrumentDeviceHasSource'].multiple_associations = True

        return resource_data

    def check_dpms_policy(self, process, message, headers):

        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=False)

        except Inconsistent, ex:
            return False, ex.message

        resource_id = message.data_product_id

        # Allow actor to suspend/activate persistence in an org where the actor has the appropriate role
        orgs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
        for org in orgs:
            if (has_org_role(gov_values.actor_roles, org.org_governance_name, [INSTRUMENT_OPERATOR, DATA_OPERATOR, ORG_MANAGER_ROLE, OBSERVATORY_OPERATOR])):
                log.error("returning true: "+str(gov_values.actor_roles))
                return True, ''

        log.error("returning false: "+str(gov_values.actor_roles))

        return False, '%s(%s) has been denied since the user does not have an appropriate role in any org to which the data product id %s belongs ' % (process.name, gov_values.op, resource_id)
