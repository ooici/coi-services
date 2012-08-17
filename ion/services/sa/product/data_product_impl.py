#!/usr/bin/env python

"""
@package  ion.services.sa.product.data_product_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from pyon.public import PRED, RT
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
                for nxt_producer_id in nxt_producer_ids:
                    nxt_producer_obj = self.clients.resource_registry.read(nxt_producer_id)
                    log.debug("DataProductManagementService:_find_producers nxt_producer %s" % nxt_producer_obj.name)
                    #todo: check the type of resource; instrument, data process or extDataset'
                    #todo: check if this is a SiteDataProduct name=SiteDataProduct and desc=site_id
                    inputs_to_nxt_producer = self._find_producer_in_products(nxt_producer_id)
                    log.debug("DataProductManagementService:_find_producers inputs_to_nxt_producer %s", str(inputs_to_nxt_producer))
                    provenance_results[data_product_id] = { 'producerctx':self._extract_producer_context(nxt_producer_id) , 'producer': nxt_producer_id, 'inputs': inputs_to_nxt_producer }
                    log.debug("DataProductManagementService:_find_producers self.provenance_results %s", str(provenance_results))
                    for input in inputs_to_nxt_producer:
                        self._find_producers(input, provenance_results)
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
        page = etree.Element('lineage')
        doc = etree.ElementTree(page)

        processing_step = etree.SubElement(page, 'processing_step')
        product_obj = self.clients.resource_registry.read(data_product_id)
        data_product_tag = etree.SubElement(processing_step, 'data_product')
        name = etree.SubElement(data_product_tag, "name")
        name.text = product_obj.name
        desc = etree.SubElement(data_product_tag, "description")
        desc.text = product_obj.description

        result = etree.tostring(page, pretty_print=False, encoding=None)

        log.debug("DataProductManagementService:_write_product_provenance_report result: %s" % str(result))

        return results

        

