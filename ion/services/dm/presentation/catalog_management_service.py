#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/presentation/catalog_management_service.py
@description Catalog Management Service
'''

from pyon.public import PRED,RT
from pyon.util.log import log
from pyon.util.arg_check import validate_is_instance
from pyon.core.exception import BadRequest
from interface.objects import Catalog, Index, View
from interface.services.dm.icatalog_management_service import BaseCatalogManagementService
import heapq

class CatalogManagementService(BaseCatalogManagementService):
    """
    Catalog Management Service
    Manages and presents catalog resources
    """


    def create_catalog(self, catalog_name='', keywords=None):
        """A catalog is a new data set that aggregates and presents datasets in a specific way.
        @param catalog_name    str
        @retval catalog_id    str
        """
        # Ensure unique catalog names 
        res, _ = self.clients.resource_registry.find_resources(name=catalog_name, id_only=True)
        if len(res) > 0:
            raise BadRequest('The catalog resource with name: %s, already exists.' % catalog_name)
        
        if keywords is None:
            keywords = []

        catalog_res = Catalog(name=catalog_name, catalog_fields=keywords)
        index_found = False
        available_fields = set()
        catalog_id, _ = self.clients.resource_registry.create(catalog_res)
        catalog_res = self.read_catalog(catalog_id)

        indexes = self.clients.index_management.list_indexes(id_only=False).values()
        last_resort = []
        for index in indexes:
            index_fields = set(index.options.attribute_match) | set(index.options.range_fields) | set(index.options.geo_fields)
            if set(keywords).issubset(index_fields):
                if len(index_fields) > (100): # Index is quite large, save for last resort
                    heapq.heappush(last_resort,(len(index_fields), index))
                    continue
                self.clients.resource_registry.create_association(subject=catalog_id, predicate=PRED.hasIndex, object=index._id)
                index_found = True
                available_fields = available_fields | index_fields
        if not index_found and last_resort:
            _, index = heapq.heappop(last_resort)
            self.clients.resource_registry.create_association(subject=catalog_id, predicate=PRED.hasIndex, object=index._id)
            index_found = True

        if not index_found:
            #@todo: notify the client that an empty catalog was formed
            pass

        catalog_res.available_fields = list(available_fields)

        self.update_catalog(catalog_res)
        return catalog_id

    def update_catalog(self, catalog=None):
        """@todo document this interface!!!

        @param catalog    Catalog
        @retval success    bool
        """
        self.clients.resource_registry.update(catalog)
        return True

    def read_catalog(self, catalog_id=''):
        """Read catalog resources

        @param catalog_id    str
        @retval catalog    Catalog
        """
        return self.clients.resource_registry.read(catalog_id)

    def delete_catalog(self, catalog_id=''):
        """@todo document this interface!!!

        @param catalog_id    str
        @retval success    bool
        """
        objs, assocs = self.clients.resource_registry.find_associations_mult(subjects=[catalog_id], id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)

        self.clients.resource_registry.delete(catalog_id)

        return True

    def add_indexes(self, catalog_id='', index_ids=None):
        """Add an index to the specified catalog

        @param index_ids    list
        @retval success    bool
        """
        validate_is_instance(index_ids,list, "A list of index IDs was not provided.")
        
        for index_id in index_ids:
            self.clients.resource_registry.create_association(subject=catalog_id, predicate=PRED.hasIndex,object=index_id)

        return True


    def list_indexes(self, catalog_id='', id_only=True):
        """List the indexes for the specified catalog

        @param catalog_id    str
        @retval success    list
        """
        index_ids,assocs = self.clients.resource_registry.find_objects(subject=catalog_id, predicate=PRED.hasIndex, id_only=id_only)
        return index_ids
