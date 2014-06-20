#!/usr/bin/env python
__author__ = 'Maurice Manning'


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
from interface.objects import ComputedValueAvailability, DataProductTypeEnum, Dataset, CoverageTypeEnum, ParameterContext
from interface.objects import DataProduct, DataProductVersion, InformationStatus, DataProcess, DataProcessTypeEnum, Device

from coverage_model import QuantityType, ParameterDictionary, NumexprFunction, ParameterFunctionType
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
import re

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
        Creates a data product Resource.

        @param data_product                 - The data product resource
        @param stream_definition_id         - The stream definition points to the parameter dictionary and defines the 
                                              parameters for this data product
        @param exchange_point               - Which exchange point on the broker to use for streaming purposes
        @param dataset_id                   - It's possible to create a data product from an already existing dataset, 
                                              this is the dataset to use and point to
        @param parent_data_product_id       - The id of the parent data product, this is for the case where a derived 
                                              data product is created
        @param default_stream_configuration - A configuration for how to name the streams coming from the agent
        """

        if data_product.category == DataProductTypeEnum.DEVICE:
            return self.create_device_data_product(data_product, stream_definition_id, default_stream_configuration)

        elif data_product.category == DataProductTypeEnum.SITE:
            return self.create_site_data_product(data_product, stream_definition_id)

        elif data_product.category == DataProductTypeEnum.DERIVED:
            return self.create_derived_data_product(data_product, parent_data_product_id, stream_definition_id)

        elif data_product.category == DataProductTypeEnum.EXTERNAL:
            return self.create_external_data_product(data_product, stream_definition_id)

        else:
            raise BadRequest("Unrecognized Data Product Type")


    def create_device_data_product(self, data_product=None, stream_definition_id='', stream_configuration=None):
        '''
        Creates a data product resource and a stream for the data product.
        '''

        if not data_product.category == DataProductTypeEnum.DEVICE:
            raise BadRequest("Attempted to create a Device Data Product without the proper type category")

        data_product_id = self.create_data_product_(data_product)

        # WARNING: This creates a Stream as a side effect!!
        self.assign_stream_definition_to_data_product(data_product_id=data_product_id,
                                                      stream_definition_id=stream_definition_id,
                                                      stream_configuration=stream_configuration)

        return data_product_id


    def create_derived_data_product(self, data_product=None, parent_data_product_id='', stream_definition_id=''):
        '''
        Creates a derived data product
        '''
        if not data_product.category == DataProductTypeEnum.DERIVED:
            raise BadRequest("Attempted to create a Derived Data Product without the proper type category")

        # Store the resource
        data_product_id = self.create_data_product_(data_product)

        # Associate the stream definition with the data product, BUT DONT MAKE A STREAM
        self.RR2.assign_stream_definition_to_data_product_with_has_stream_definition(stream_definition_id,
                                                                                     data_product_id)
        

        # Associate the data product to its parent
        self.assign_data_product_to_data_product(data_product_id=data_product_id, parent_data_product_id=parent_data_product_id)

        # Associate the dataset of the parent with this data product
        dataset_ids, _ = self.clients.resource_registry.find_objects(parent_data_product_id, predicate=PRED.hasDataset, id_only=True)
        for dataset_id in dataset_ids:
            self.assign_dataset_to_data_product(data_product_id, dataset_id)

        # If there were physical datasets
        if dataset_ids:
            self.create_catalog_entry(data_product_id)

        self._check_qc(data_product_id)

        return data_product_id

    def create_site_data_product(self, data_product=None, stream_definition_id=''):
        '''
        Creates a site data product
        '''
        if not data_product.category == DataProductTypeEnum.SITE:
            raise BadRequest("Attempted to create a Site Data Product without the proper type category")

        # Store the resource
        data_product_id = self.create_data_product_(data_product)

        # Associate the stream definition with the data product, BUT DONT MAKE A STREAM
        self.RR2.assign_stream_definition_to_data_product_with_has_stream_definition(stream_definition_id,
                                                                                     data_product_id)

        return data_product_id

    def create_external_data_product(self, data_product=None, stream_definition_id=''):
        '''
        Creates an external data product
        '''
        if not data_product.category == DataProductTypeEnum.EXTERNAL:
            raise BadRequest("Attempted to create a External Data Product without the proper type category")

        # Store the resource
        data_product_id = self.create_data_product_(data_product)

        # Associate the stream definition with the data product, BUT DONT MAKE A STREAM
        self.RR2.assign_stream_definition_to_data_product_with_has_stream_definition(stream_definition_id,
                                                                                     data_product_id)

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

    def _check_qc(self, data_product_id):
        '''
        Creates the necessary QC parameters where the "qc_applications" attribute is specified.
        '''


        data_product = self.read_data_product(data_product_id)
        parameters = self.get_data_product_parameters(data_product_id)
        parameter_names = [p.name for p in parameters]
        pmap = {}

        # Make a map from the ooi short name to the parameter object
        for p in parameters:
            if p.ooi_short_name:
                pmap[re.sub(r'_L[0-2]', '', p.ooi_short_name)] = p


        for sname, qc_applicability in data_product.qc_applications.iteritems():

            parameter_list = self._generate_qc(pmap[sname], qc_applicability)
            for parameter in parameter_list:
                if parameter.name in parameter_names: # Parameter already exists
                    continue

                parameter_id = self.clients.dataset_management.create_parameter(parameter)
                self.add_parameter_to_data_product(parameter_id, data_product_id)

        return True


    def _generate_qc(self, parameter, qc_list):
        sname = parameter.ooi_short_name
        # DATAPROD_ALGORTHM_QC
        # drop the _L?
        sname = re.sub(r'_L[0-2]', '', sname)

        retval = []
        for qc_thing in qc_list:

            if qc_thing not in ('qc_glblrng', 'qc_gradtst', 'qc_trndtst', 'qc_spketst', 'qc_loclrng', 'qc_stuckvl'):
                log.warning("Invalid QC: %s", qc_thing)
                continue

            qc_thing = qc_thing.replace('qc_', '')
            new_param_name = '_'.join([sname, qc_thing.upper(), 'QC'])

            parameter = ParameterContext(new_param_name.lower(),
                                         parameter_type='quantity',
                                         value_encoding='int8',
                                         ooi_short_name=new_param_name,
                                         display_name=' '.join([sname, qc_thing.upper()]),
                                         units='1',
                                         description=' '.join([qc_thing.upper(), 'Quality Control for', sname]),
                                         fill_value=-88
                                         )
            retval.append(parameter)
        return retval

        
        '''
    { TEMPWAT_L1: [qc_glblrng, qc_gradtst],
      DENSITY_L2: [qc_glblrng] }
        '''

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

        if self._qc_application_changed(original, data_product):
            self._check_qc(data_product._id)

    def _metadata_changed(self, original_dp, new_dp):
        from ion.processes.data.registration.registration_process import RegistrationProcess
        for field in RegistrationProcess.catalog_metadata:
            if hasattr(original_dp, field) and getattr(original_dp, field) != getattr(new_dp, field):
                return True
        return False


    def _qc_application_changed(self, original_dp, new_dp):
        retval = original_dp.qc_applications != new_dp.qc_applications
        return retval



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
        if data_product_obj.category == DataProductTypeEnum.DEVICE:
            if not stream_ids:
                raise BadRequest('Specified DataProduct has no streams associated with it')
            stream_id = stream_ids[0]
        else:
            stream_id = None


        stream_defs, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasStreamDefinition,id_only=True)
        if not stream_defs:
            raise BadRequest("Data Product stream is without a stream definition")
        stream_def_id = stream_defs[0]

        parameter_dictionary_ids, _ = self.clients.resource_registry.find_objects(stream_def_id, PRED.hasParameterDictionary, id_only=True)
        if not parameter_dictionary_ids:
            raise BadRequest("Data Product stream is without a parameter dictionary")
        parameter_dictionary_id = parameter_dictionary_ids[0]

        parent_data_product_ids, _ = self.clients.resource_registry.find_objects(data_product_id, predicate=PRED.hasDataProductParent, id_only=True)
        if len(parent_data_product_ids) == 1:  # This is a child data product
            raise BadRequest("Child Data Products shouldn't be activated")

        child_data_product_ids, _ = self.clients.resource_registry.find_subjects(object=data_product_id, predicate=PRED.hasDataProductParent, id_only=True)


        dataset_ids, _ = self.clients.resource_registry.find_objects(data_product_id, predicate=PRED.hasDataset, id_only=True)

        #-----------------------------------------------------------------------------------------
        # Step 2: Create and associate Dataset (coverage)


        # If there's already a dataset, just return that
        if dataset_ids:
            return dataset_ids[0]

        dataset_id = self._create_dataset(data_product_obj, parameter_dictionary_id)

        # Also assign the stream to the dataset
        if stream_id:
            self.RR2.assign_stream_to_dataset_with_has_stream(stream_id, dataset_id)

        # link dataset with data product. This creates the association in the resource registry
        self.RR2.assign_dataset_to_data_product_with_has_dataset(dataset_id, data_product_id)

        # Link this dataset with the child data products AND
        # create catalog entries for the child data products
        for child_dp_id in child_data_product_ids:
            self.assign_dataset_to_data_product(child_dp_id, dataset_id)
            self.create_catalog_entry(data_product_id=data_product_id)
        
        # register the dataset for externalization

        self.create_catalog_entry(data_product_id=data_product_id)

        return dataset_id


    def _create_dataset(self, data_product, parameter_dictionary_id):
        # Device -> Simplex, Site -> Complex
        if data_product.category == DataProductTypeEnum.DEVICE:
            dataset = Dataset(name=data_product.name,
                              description='Dataset for Data Product %s' % data_product._id,
                              coverage_type=CoverageTypeEnum.SIMPLEX)
        elif data_product.category == DataProductTypeEnum.SITE:
            dataset = Dataset(name=data_product.name,
                              description='Dataset for Data Product %s' % data_product._id,
                              coverage_type=CoverageTypeEnum.COMPLEX)

        # No datasets are currently linked which means we need to create a new one
        dataset_id = self.clients.dataset_management.create_dataset(dataset,
                                                                    parameter_dictionary_id=parameter_dictionary_id)

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
        data_product = self.read_data_product(data_product_id)

        data_product = self.read_data_product(data_product_id)
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
        if datasets:
            dataset_id = datasets[0]
            self.clients.dataset_management.add_parameter_to_dataset(parameter_context_id, dataset_id)

            self.update_catalog_entry(data_product_id) 
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

    def get_data_product_parameters(self, data_product_id='', id_only=False):
        stream_defs, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=False)
        if not stream_defs:
            raise BadRequest("No Stream Definition Found for data product %s" % data_product_id)
        stream_def = stream_defs[0]

        pdicts, _ = self.clients.resource_registry.find_objects(stream_def._id, PRED.hasParameterDictionary, id_only=True)
        if not pdicts:
            raise BadRequest("No Parameter Dictionary Found for data product %s" % data_product_id)
        pdict_id = pdicts[0]
        parameters, _ = self.clients.resource_registry.find_objects(pdict_id, PRED.hasParameterContext, id_only=False)
        if not parameters:
            raise NotFound("No parameters are associated with this data product")

        # too complicated for one line of code
        #retval = { p.name : p._id for p in parameters if not filtered or (filtered and p in stream_def.available_fields) }
        param_id = lambda x, id_only : x._id if id_only else x
        retval = []
        for p in parameters:
            if (stream_def.available_fields and p.name in stream_def.available_fields) or not stream_def.available_fields:
                retval.append(param_id(p, id_only))
        return retval


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
        return self._registration_rpc('create_entry',data_product_id) 

    def read_catalog_entry(self, data_product_id=''):
        return self._registration_rpc('read_entry', data_product_id)

    def update_catalog_entry(self, data_product_id=''):
        return self._registration_rpc('update_entry', data_product_id)

    def delete_catalog_entry(self, data_product_id=''):
        return self._registration_rpc('delete_entry', data_product_id)


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

        # Set data product source device (WARNING: may not be unique)
        extended_product.source_device = None
        dp_source, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasSource, id_only=False)
        for dps  in dp_source:
            if isinstance(dps, Device):
                if extended_product.source_device == None:
                    extended_product.source_device = dps
                else:
                    log.warn("DataProduct %s has additional source device: %s", data_product_id, dps._id)

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
            data_product = self.container.resource_registry.read(data_product_id)

            if data_product.category == DataProductTypeEnum.EXTERNAL:                
                if len(data_product.reference_urls) == 1:
                    ret.value = data_product.reference_urls[0]
                    ret.status = ComputedValueAvailability.PROVIDED
                    log.debug("get_data_url: data_url: %s", ret.value)                                  

            else:                
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
