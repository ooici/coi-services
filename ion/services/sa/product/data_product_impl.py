#!/usr/bin/env python

"""
@package  ion.services.sa.product.data_product_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from pyon.public import PRED, RT, OT
from pyon.core.object import IonObjectBase
from pyon.core.exception import BadRequest, NotFound
from pyon.util.log import log
from pyon.util.ion_time import IonTime
from lxml import etree

import string

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
        return self._find_stemming(data_product_id, PRED.hasDataset, RT.Dataset)

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
        log.debug("DataProductImpl:_find_producers start %s", data_product_id)
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
                    log.debug("DataProductImpl:_find_producers nxt_producer %s", nxt_producer_obj.name)
                    #todo: check the type of resource; instrument, data process or extDataset'
                    #todo: check if this is a SiteDataProduct name=SiteDataProduct and desc=site_id
                    inputs_to_nxt_producer = self._find_producer_in_products(nxt_producer_id)
                    log.debug("DataProductImpl:_find_producers inputs_to_nxt_producer %s", str(inputs_to_nxt_producer))
                    producer_list.append(nxt_producer_id)
                    inputs[nxt_producer_id] = inputs_to_nxt_producer
                    #provenance_results[data_product_id] = { 'producerctx':self._extract_producer_context(nxt_producer_id) , 'producer': nxt_producer_id, 'inputs': inputs_to_nxt_producer }
                    log.debug("DataProductImpl:_find_producers self.provenance_results %s", str(provenance_results))
                    for input in inputs_to_nxt_producer:
                        self._find_producers(input, provenance_results)

                    provenance_results[data_product_id] = { 'producer': producer_list, 'inputs': inputs }

        log.debug("DataProductImpl:_find_producers: %s", str(source_ids))
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
            log.debug("DataProductImpl:_find_producer_in_products: %s", product_obj.name)

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
        log.debug("DataProductImpl:_write_product_provenance_report in_data_products: %s",
                  str(in_data_products))

        while in_data_products:
            for in_data_product in in_data_products:
                # write the provenance for each of those products
                self._write_product_info(in_data_product, provenance_results)
                log.debug("DataProductImpl:_write_product_provenance_report next_input_set: %s",
                          str(provenance_results[in_data_product]['inputs']))
                # provenance_results[in_data_product]['inputs'] contains a dict that is produce_id:[input_product_list]
                for key, value in provenance_results[in_data_product]['inputs'].items():
                    next_input_set.extend(value)
            #switch to the input for these producers
            in_data_products =  next_input_set
            next_input_set = []
            log.debug("DataProductImpl:_write_product_provenance_report in_data_products (end loop): %s",
                      str(in_data_products))


        result = etree.tostring(self.page, pretty_print=True, encoding=None)

        log.debug("DataProductImpl:_write_product_provenance_report result: %s", str(result))

        return results



    def _write_object_info(self, data_obj=None, etree_node=None):

        fields, schema = data_obj.__dict__, data_obj._schema

        for att_name, attr_type in schema.iteritems():
#            log.debug("DataProductImpl:_write_product_info att_name %s",  str(att_name))
#            log.debug("DataProductImpl:_write_product_info attr_type %s",  str(attr_type))
#            log.debug("DataProductImpl:_write_product_info attr_type [type] %s",  str(attr_type['type']))
            attr_value = getattr(data_obj, att_name)
            log.debug("DataProductImpl:_write_product_info att_value %s",  str(attr_value))
            if isinstance(attr_value, IonObjectBase):
                log.debug("DataProductImpl:_write_product_info IonObjectBase  att_value %s", str(attr_value))
            if isinstance(fields[att_name], IonObjectBase):
                sub_elem = etree.SubElement(etree_node, att_name)
                log.debug("DataProductImpl:_write_product_info IonObjectBase  fields[att_name] %s", str(fields[att_name]))
                self._write_object_info(data_obj=attr_value, etree_node=sub_elem)
            elif attr_type['type'] == 'list' and attr_value:
                sub_elem = etree.SubElement(etree_node, att_name)
                for list_element in attr_value:
                    log.debug("DataProductImpl:_list_element %s",  str(list_element))
                    if isinstance(list_element, IonObjectBase):
                        self._write_object_info(data_obj=list_element, etree_node=sub_elem)

            elif attr_type['type'] == 'dict' and attr_value:
                sub_elem = etree.SubElement(etree_node, att_name)
                for key, val in attr_value.iteritems():
                    log.debug("DataProductImpl:dict key %s    val%s",  str(key), str(val) )
                    if isinstance(val, IonObjectBase):
                        self._write_object_info(data_obj=val, etree_node=sub_elem)
                    else:
                        log.debug("DataProductImpl:dict new simple elem key %s ",  str(key) )
                        dict_sub_elem = etree.SubElement(sub_elem, key)
                        dict_sub_elem.text = str(val)


            #special processing for timestamp elements:
            elif  attr_type['type'] == 'str' and  '_time' in att_name  :
                log.debug("DataProductImpl:_format_ion_time  att_name %s   attr_value %s ", str(att_name), str(attr_value))
                if len(attr_value) == 16 :
                    attr_value = self._format_ion_time(attr_value)
                sub_elem = etree.SubElement(etree_node, att_name)
                sub_elem.text = str(attr_value)
            else:
                sub_elem = etree.SubElement(etree_node, att_name)
                sub_elem.text = str(attr_value)



    def _write_product_info(self, data_product_id='', provenance_results=''):
        #--------------------------------------------------------------------------------
        # Data Product metadata
        #--------------------------------------------------------------------------------
        log.debug("DataProductImpl:provenance_report data_product_id %s",  str(data_product_id))
        processing_step = etree.SubElement(self.page, 'processing_step')
        product_obj = self.clients.resource_registry.read(data_product_id)
        data_product_tag = etree.SubElement(processing_step, 'data_product')

        self._write_object_info(data_obj=product_obj, etree_node=data_product_tag)


        #--------------------------------------------------------------------------------
        # Data Producer metadata
        #--------------------------------------------------------------------------------
        producer_dict = provenance_results[data_product_id]
        log.debug("DataProductImpl:provenance_report  producer_dict %s ", str(producer_dict))
        producer_list = provenance_results[data_product_id]['producer']
        data_producer_list_tag = etree.SubElement(processing_step, 'data_producer_list')
        for producer_id in producer_list:
            log.debug("DataProductImpl:reading producer  %s ", str(producer_id))
            producer_obj = self.clients.resource_registry.read(producer_id)
            data_producer_tag = etree.SubElement(data_producer_list_tag, 'data_producer')
            self._write_object_info(data_obj=producer_obj, etree_node=data_producer_tag)


            #retrieve the assoc data producer resource
            data_producer_objs, producer_assns = self.clients.resource_registry.find_objects(subject=producer_id, predicate=PRED.hasDataProducer, id_only=False)
            if not data_producer_objs:
                raise BadRequest('No Data Producer resource associated with the Producer %s' % str(producer_id))
            data_producer_obj = data_producer_objs[0]
            sub_elem = etree.SubElement(data_producer_tag, 'data_producer_config')
            log.debug("DataProductImpl:data_producer_obj  %s ", str(data_producer_obj))
            self._write_object_info(data_obj=data_producer_obj, etree_node=sub_elem)

            # add the input product names for these producers
            in_product_list = provenance_results[data_product_id]['inputs'][producer_id]
            if in_product_list:
                input_products_tag = etree.SubElement(data_producer_tag, "input_products")
                for in_product in in_product_list:
                    input_product_tag = etree.SubElement(input_products_tag, "input_product")
                    #product_name_tag = etree.SubElement(input_product_tag, "name")
                    product_obj = self.clients.resource_registry.read(in_product)
                    self._write_object_info(data_obj=product_obj, etree_node=input_product_tag)
                    #product_name_tag.text = product_obj.name


            # check for attached deployment
            deployment_ids, _ = self.clients.resource_registry.find_objects( subject=producer_id, predicate=PRED.hasDeployment, object_type=RT.Deployment, id_only=True)
            #todo: match when this prouct was produced with the correct deployment object
            if deployment_ids:
                data_producer_deploys_tag = etree.SubElement(data_producer_tag, 'data_producer_deployments')
                for deployment_id in deployment_ids:
                    deployment_tag = etree.SubElement(data_producer_deploys_tag, 'deployment')
                    deployment_obj = self.clients.resource_registry.read(deployment_id)
                    #find the site
                    self._write_object_info(data_obj=deployment_obj, etree_node=deployment_tag)
                    deployment_site_ids, _ = self.clients.resource_registry.find_subjects( subject_type=RT.InstrumentSite, predicate=PRED.hasDeployment, object=deployment_id, id_only=True)
                    for deployment_site_id in deployment_site_ids:
                        deploy_site_tag = etree.SubElement(deployment_tag, 'deployment_site')
                        site_obj = self.clients.resource_registry.read(deployment_site_id)
                        self._write_object_info(data_obj=site_obj, etree_node=deploy_site_tag)

            # check for lookup table attachments
            att_ids = self.clients.resource_registry.find_attachments(producer_id, keyword="DataProcessInput", id_only=True)
            if att_ids:
                data_producer_lookups_tag = etree.SubElement(data_producer_tag, 'data_producer_attachments')
                for att_id in att_ids:
                    lookup_tag = etree.SubElement(data_producer_lookups_tag, 'attachment')
                    attach_obj = self.clients.resource_registry.read(att_id)
                    self._write_object_info(data_obj=attach_obj, etree_node=lookup_tag)



    def _format_ion_time(self, ion_time=''):
        ion_time_obj = IonTime.from_string(ion_time)
        #todo: fix this and return str( ion_time_obj)
        return str(ion_time_obj)
