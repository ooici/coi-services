#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.management.org_impl
@author   Ian Katz
"""


#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT

from ion.services.sa.resource_impl.resource_simple_impl import ResourceSimpleImpl

class OrgImpl(ResourceSimpleImpl):
    """
    @brief resource management for Org resources
    """

    def _primary_object_name(self):
        return RT.Org

    def _primary_object_label(self):
        return "org"

    def link_observatory(self, org_id='', observatory_id=''):
        return self._link_resources(org_id, PRED.hasObservatory, observatory_id)

    def unlink_observatory(self, org_id='', observatory_id=''):
        return self._unlink_resources(org_id, PRED.hasObservatory, observatory_id)

    def find_having_observatory(self, observatory_id):
        return self._find_having(PRED.hasObservatory, observatory_id)

    def find_stemming_observatory(self, observatory_id):
        return self._find_stemming(observatory_id, PRED.hasObservatory, RT.Observatory)

