#!/usr/bin/env python

"""
@package  ion.services.sa.product.data_product_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from pyon.public import PRED, RT, OT
from pyon.core.exception import BadRequest, NotFound
from pyon.util.log import log
from lxml import etree


class DataProductImpl(ResourceSimpleImpl):
    """
    @brief Resource management for DataProduct resources
    """

    def _primary_object_name(self):
        return RT.DataProduct

    def _primary_object_label(self):
        return "data_product"

    def link_data_producer(self, data_product_id='', data_producer_id=''):
        return self._link_resources(data_product_id, PRED.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, data_product_id='', data_producer_id=''):
        return self._unlink_resources(data_product_id, PRED.hasDataProducer, data_producer_id)

    def link_data_set(self, data_product_id='', data_set_id=''):
        return self._link_resources(data_product_id, PRED.hasDataset, data_set_id)

    def unlink_data_set(self, data_product_id='', data_set_id=''):
        return self._unlink_resources(data_product_id, PRED.hasDataset, data_set_id)

    def link_stream(self, data_product_id='', stream_id=''):
        return self._link_resources(data_product_id, PRED.hasStream, stream_id)

    def unlink_stream(self, data_product_id='', stream_id=''):
        return self._unlink_resources(data_product_id, PRED.hasStream, stream_id)

    def link_parent(self, data_product_id='', parent_data_product_id=''):
        return self._link_resources(data_product_id, PRED.hasParent, parent_data_product_id)

    def unlink_parent(self, data_product_id='', parent_data_product_id=''):
        return self._unlink_resources(data_product_id, PRED.hasParent, parent_data_product_id)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(PRED.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasDataProducer, RT.DataProducer)

    def find_having_data_set(self, data_set_id):
        return self._find_having(PRED.hasDataset, data_set_id)

    def find_stemming_data_set(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasDataset, RT.DataSet)

    def find_having_stream(self, stream_id):
        return self._find_having(PRED.hasStream, stream_id)

    def find_stemming_stream(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasStream, RT.Stream)

    def find_having_parent(self, parent_data_product_id):
        return self._find_having(PRED.hasParent, parent_data_product_id)

    def find_stemming_parent(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasParent, RT.DataProduct)

    def _find_producers(self, data_product_id='', provenance_results=''):
        source_ids = []
        # get the link to the DataProducer resource
        log.debug("DataProductManagementService:_find_producers start %s" % str(data_product_id))
        producer_ids, _ = self.clients.resource_registry.find_objects(subject=data_product_id, predicate=PRED.hasDataProducer, id_only=True)
        for producer_id in producer_ids:
            # get the link to that resources parent DataProducer
            parent_ids, _ = self.clients.resource_registry.find_objects(subject=producer_id, predicate=PRED.hasParent, id_only=True)
            for parent_id in parent_ids:
                # get the producer that this DataProducer represents
                nxt_producer_ids, _ = self.clients.resource_registry.find_subjects( predicate=PRED.hasDataProducer, object=parent_id, id_only=True)
                producer_list = []
                inputs = {}

                for nxt_producer_id in nxt_producer_ids:
                    nxt_producer_obj = self.clients.resource_registry.read(nxt_producer_id)
                    log.debug("DataProductManagementService:_find_producers nxt_producer %s" % nxt_producer_obj.name)
                    #todo: check the type of resource; instrument, data process or extDataset'
                    #todo: check if this is a SiteDataProduct name=SiteDataProduct and desc=site_id
                    inputs_to_nxt_producer = self._find_producer_in_products(nxt_producer_id)
                    log.debug("DataProductManagementService:_find_producers inputs_to_nxt_producer %s", str(inputs_to_nxt_producer))
                    producer_list.append(nxt_producer_id)
                    inputs[nxt_producer_id] = inputs_to_nxt_producer
                    #provenance_results[data_product_id] = { 'producerctx':self._extract_producer_context(nxt_producer_id) , 'producer': nxt_producer_id, 'inputs': inputs_to_nxt_producer }
                    log.debug("DataProductManagementService:_find_producers self.provenance_results %s", str(provenance_results))
                    for input in inputs_to_nxt_producer:
                        self._find_producers(input, provenance_results)

                    provenance_results[data_product_id] = { 'producer': producer_list, 'inputs': inputs }

        log.debug("DataProductManagementService:_find_producers: %s" % str(source_ids))
        return

    def _extract_producer_context(self, producer_id=''):

        producer_obj = self.clients.resource_registry.read(producer_id)
        producertype = type(producer_obj).__name__

        context = {}
        if RT.DataProcess == producertype :
            context['DataProcess'] = str(producer_obj)
            data_proc_def_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasProcessDefinition, object_type=RT.DataProcessDefinition)
            for data_proc_def_obj in data_proc_def_objs:
                proc_def_type = type(data_proc_def_obj).__name__
                if RT.DataProcessDefinition == proc_def_type :
                    context['DataProcessDefinition'] = str(data_proc_def_obj)
                if RT.ProcessDefinition == proc_def_type :
                    context['ProcessDefinition'] = str(data_proc_def_obj)
            transform_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasTransform, object_type=RT.Transform)
            if transform_objs:
               context['Transform'] = str(transform_objs[0])
        if RT.InstrumentDevice == producertype :
            context['InstrumentDevice'] = str(producer_obj)
            inst_model_objs, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasModel, object_type=RT.InstrumentModel)
            if inst_model_objs:
                context['InstrumentModel'] = str(inst_model_objs[0])
        return context

    def _find_producer_in_products(self, producer_id=''):
        # get the link to the inout DataProduct resource
        product_ids, _ = self.clients.resource_registry.find_objects(   subject=producer_id,
                                                                            predicate=PRED.hasInputProduct,
                                                                            id_only=True)
        for product_id in product_ids:
            product_obj = self.clients.resource_registry.read(product_id)
            log.debug("DataProductManagementService:_find_producer_in_products: %s" % product_obj.name)

        return product_ids


    def _write_product_provenance_report(self, data_product_id='', provenance_results=''):

        results = ''

        if not data_product_id:
            raise BadRequest('Data Product Id %s must be provided' % str(data_product_id))
        if not provenance_results:
            raise BadRequest('Data Product provenance data %s must be provided' % str(provenance_results))

        #set up xml doc
        self.page = etree.Element('lineage')
        self.doc = etree.ElementTree(self.page)

        in_data_products = []
        next_input_set = []
        self._write_product_info(data_product_id, provenance_results)

        #get the set of inputs to the producer which created this data product
        for key, value in provenance_results[data_product_id]['inputs'].items():
            in_data_products.extend(value)
        log.debug("DataProductManagementService:_write_product_provenance_report in_data_products: %s" % str(in_data_products))

        while in_data_products:
            for in_data_product in in_data_products:
                # write the provenance for each of those products
                self._write_product_info(in_data_product, provenance_results)
                log.debug("DataProductManagementService:_write_product_provenance_report next_input_set: %s" % str(provenance_results[in_data_product]['inputs']))
                # provenance_results[in_data_product]['inputs'] contains a dict that is produce_id:[input_product_list]
                for key, value in provenance_results[in_data_product]['inputs'].items():
                    next_input_set.extend(value)
            #switch to the input for these producers
            in_data_products =  next_input_set
            next_input_set = []
            log.debug("DataProductManagementService:_write_product_provenance_report in_data_products (end loop): %s" % str(in_data_products))


        result = etree.tostring(self.page, pretty_print=True, encoding=None)

        log.debug("DataProductManagementService:_write_product_provenance_report result: %s" % str(result))

        return results


    def _write_product_info(self, data_product_id='', provenance_results=''):
        #--------------------------------------------------------------------------------
        # Data Product metadata
        #--------------------------------------------------------------------------------
        log.debug("DataProductManagementService:provenance_report data_product_id %s",  str(data_product_id))
        processing_step = etree.SubElement(self.page, 'processing_step')
        product_obj = self.clients.resource_registry.read(data_product_id)
        data_product_tag = etree.SubElement(processing_step, 'data_product')
        name = etree.SubElement(data_product_tag, "name")
        name.text = product_obj.name
        desc = etree.SubElement(data_product_tag, "description")
        desc.text = product_obj.description

        # Contacts List
        log.debug("DataProductManagementService:provenance_report  Contacts List" )
        contactlist = etree.SubElement(data_product_tag, "ContactList")
        if product_obj.contacts:
            for contact in product_obj.contacts:
                log.debug("DataProductManagementService:provenance_report  Contacts List contact %s", str(contact) )
                contacttag = etree.SubElement(contactlist, "Contact")
                first_name = etree.SubElement(contacttag, "first_name")
                first_name.text = contact.first_name
                name = etree.SubElement(contacttag, "name")
                name.text = contact.name
                address = etree.SubElement(contacttag, "address")
                address.text = contact.street_address
                city = etree.SubElement(contacttag, "city")
                city.text = contact.city
                postalcode = etree.SubElement(contacttag, "postalcode")
                postalcode.text = contact.postalcode
                state = etree.SubElement(contacttag, "state")
                state.text = contact.state
                country = etree.SubElement(contacttag, "country")
                country.text = contact.country
                phone = etree.SubElement(contacttag, "phone")
                phone.text = contact.phone
                email = etree.SubElement(contacttag, "email")
                email.text = contact.email

        # GeoSpatial bounds
        #todo: pull form coverage model

        # todo Construct data URL
        log.debug("DataProductManagementService:provenance_report  data URL ")
        dataurltag = etree.SubElement(data_product_tag, "data_url")
        dataurltag.text = "ooici.org/" + str(product_obj.type_) + "/"+ str(product_obj._id)

        # one or more of the topic categories from ISO 19115
        log.debug("DataProductManagementService:provenance_report  iso_topic_category ")
        iso_topic_category_tag = etree.SubElement(data_product_tag, "iso_topic_category")
        iso_topic_category_tag.text = str(product_obj.iso_topic_category)

        #
        quality_control_level_tag = etree.SubElement(data_product_tag, "quality_control_level")
        quality_control_level_tag.text = str(product_obj.quality_control_level)

        # # OOI data processing level; L0, L1, or L2.
        processing_level_code_tag = etree.SubElement(data_product_tag, "processing_level_code")
        processing_level_code_tag.text = str(product_obj.processing_level_code)


        #--------------------------------------------------------------------------------
        # Data Producer metadata
        #--------------------------------------------------------------------------------
        producer_dict = provenance_results[data_product_id]
        log.debug("DataProductManagementService:provenance_report  producer_dict %s ", str(producer_dict))
        producer_list = provenance_results[data_product_id]['producer']
        data_producer_list_tag = etree.SubElement(processing_step, 'data_producer_list')
        for producer_id in producer_list:
            log.debug("DataProductManagementService:reading producer  %s ", str(producer_id))
            producer_obj = self.clients.resource_registry.read(producer_id)
            data_producer_tag = etree.SubElement(data_producer_list_tag, 'data_producer')

            producer_name_tag = etree.SubElement(data_producer_tag, "name")
            producer_name_tag.text = producer_obj.name
            producer_description_tag = etree.SubElement(data_producer_tag, "description")
            producer_description_tag.text = producer_obj.description
            producer_type_tag = etree.SubElement(data_producer_tag, "type")
            producer_type_tag.text = producer_obj.type_

            #retrieve the assoc data producer resource
            data_producer_objs, producer_assns = self.clients.resource_registry.find_objects(subject=producer_id, predicate=PRED.hasDataProducer, id_only=False)
            if not data_producer_objs:
                raise BadRequest('No Data Producer resource associated with the Producer %s' % str(producer_id))
            data_producer_obj = data_producer_objs[0]


            producertype = type(producer_obj).__name__
            log.debug("DataProductManagementService:producertype  %s ", str(producertype))
            if data_producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            #if RT.InstrumentDevice == producertype :
                # retrieve specifics from InstrumentProducerContext
                activation_time_tag = etree.SubElement(data_producer_tag, "activation_time")
                activation_time_tag.text = data_producer_obj.producer_context.activation_time
                execution_configuration_tag = etree.SubElement(data_producer_tag, "execution_configuration")
                execution_configuration_tag.text = str(data_producer_obj.producer_context.execution_configuration)

                #get the assoc Site resource
                if data_producer_obj.producer_context.deployed_site_id:
                    site_obj = self.clients.resource_registry.read(data_producer_obj.producer_context.deployed_site_id)
                    #todo: get the correct deployment object to retrieve: output the temporal constraints and the device_mounting_positions
                    site_tag = etree.SubElement(data_producer_tag, "DeployedSite")
                    site_tag = etree.SubElement(data_producer_tag, "DeployedSite")
                    site_geospatial_constraint_tag = etree.SubElement(site_tag, "geospatial_constraint")
                    site_geospatial_constraint_tag.text = str(site_obj.geospatial_constraint)
                    site_temporal_constraint_tag = etree.SubElement(site_tag, "temporal_constraint")
                    site_temporal_constraint_tag.text = str(site_obj.temporal_constraint)

            #if RT.DataProcess == producertype :
            if data_producer_obj.producer_context.type_ == OT.DataProcessProducerContext :
                # retrieve specifics from DataProcessProducerContext
                activation_time_tag = etree.SubElement(data_producer_tag, "activation_time")
                activation_time_tag.text = data_producer_obj.producer_context.activation_time
                execution_configuration_tag = etree.SubElement(data_producer_tag, "execution_configuration")
                execution_configuration_tag.text = str(data_producer_obj.producer_context.execution_configuration)

            # add the input product names for these producers
            in_product_list = provenance_results[data_product_id]['inputs'][producer_id]
            if in_product_list:
                input_products_tag = etree.SubElement(data_producer_tag, "input_products")
                for in_product in in_product_list:
                    input_product_tag = etree.SubElement(input_products_tag, "input_product")
                    product_name_tag = etree.SubElement(input_product_tag, "name")
                    product_obj = self.clients.resource_registry.read(in_product)
                    product_name_tag.text = product_obj.name


            # check for attached deployment
            deployment_ids, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasDeployment, object_type=RT.Deployment)
            #todo: match when this prouct was produced with the correct deployment object

            for deployment_id in deployment_ids:
                #find the site
                deployment_sites, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasDeployment, object_type=RT.Deployment)


