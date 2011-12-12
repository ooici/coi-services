#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

from pyon.util.log import log
from interface.services.sa.idata_process_management_service  import BaseDataProcessManagementService
from pyon.datastore.datastore import DataStore
from pyon.public import CFG, IonObject, log, RT, AT, LCS
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound, Conflict

class DataProcessManagementService(BaseDataProcessManagementService):
    """ @author     Alon Yaari
        @file       ion/services/sa/data_process_management/data_process_management_service.py
        @brief      Implementation of the data process management service
    """

    def create_data_process(self, data_process={}, transform={}, data_product_in=[], data_product_out={}):
        """
        @param      data_process dict defining the general resource and the specific transform
        @param      transform    dict specifying which existing transform definition to apply
        @param      data_product_in     ordered list of data product IDs matching inputs
        @param      data_product_out    ID of data product to publish process output to
        @retval     data_process_id     ID of the created data process object
        """
        # Before calling create, the following must exist:
        #       - output data product
        #       - input data product(s)
        #       - definition of the data transform being applied

        result, asso = self.clients.resource_registry.find_by_name(data_product["name"], "DataProcess", True)
        if len(result) != 0:
            raise BadRequest("Data process '%s' already exists" % data_product["name"])

        pass

    def update_data_process(self, data_process={}):
        """
        @param      data_process dict which defines the general resource and the specific transform
        @retval     {success: True or False}
        """
        pass

    def read_data_process(self, data_process_id=''):
        """
        @param      data_process_id: ID of the data process resource of interest
        @retval     data_process dict populated with values known about the requested process
        """
        pass

    def delete_data_process(self, data_process_id=''):
        """
        @param      data_process_id: ID of the data process resource to be stopped
        @retval     {success: True or False}
        """
        pass

    def find_data_process(self, filters={}):
        """
        @param      filters: dict of parameters used to filter down the list of possible data proc.
        @retval
        """
        # Return Value
        # ------------
        # data_process_list: []
        #
        pass

    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass
