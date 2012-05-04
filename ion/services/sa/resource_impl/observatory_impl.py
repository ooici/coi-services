#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.observatory_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl
from pyon.core.exception import NotFound

class ObservatoryImpl(ResourceSimpleImpl):
    """
    @brief resource management for Observatory resources
    """

    def _primary_object_name(self):
        return RT.Observatory

    def _primary_object_label(self):
        return "observatory"

    def link_site(self, observatory_id='', site_id=''):
        return self._link_resources(observatory_id, PRED.hasSite, site_id)

    def unlink_site(self, observatory_id='', site_id=''):
        return self._unlink_resources(observatory_id, PRED.hasSite, site_id)

    def find_having_site(self, site_id):
        return self._find_having(PRED.hasSite, site_id)

    def find_stemming_site(self, site_id):
        return self._find_stemming(site_id, PRED.hasSite, RT.Subsite)
