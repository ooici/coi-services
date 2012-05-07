#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.site_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class SiteImpl(ResourceSimpleImpl):
    """
    @brief resource management for Site resources
    """

    def link_site(self, site_id='', site_child_id=''):
        return self._link_resources(site_id, PRED.hasSite, site_child_id)

    def unlink_site(self, site_id='', site_child_id=''):
        return self._unlink_resources(site_id, PRED.hasSite, site_child_id)

    def find_having_site(self, site_child_id):
        return self._find_having(PRED.hasSite, site_child_id)

    # special methods, sort of a hack
    def link_parent(self, site_id='', parent_id=''):
        return self._link_resources_single_subject(parent_id, PRED.hasSite, site_id)

    def unlink_parent(self, site_id='', parent_id=''):
        return self._unlink_resources(parent_id, PRED.hasSite, site_id)
