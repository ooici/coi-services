#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

import time

from interface.services.sa.idata_process_management_service \
    import BaseDataProcessManagementService
from pyon.public import   log, RT, AT
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound


class DataProcessManagementService(BaseDataProcessManagementService):
    """ @author Alon Yaari
        @file   ion/services/sa/
                    data_process_management/data_process_management_service.py
        @brief  Implementation of the data process management service
    """

    def create_data_process(self, data_process={},
                            in_subscription_id='',
                            out_data_product_id=''):
        """
        @param  data_process        dict defining the general resource
                                        and specific transform
        @param  in_subscription_id  ordered list of data product IDs
                                        matching inputs
        @param  out_data_product_id ID of data product to publish
                                        process output to
        @retval data_process_id     ID of the created data process object
        """
        log.debug("DataProcessManagementService:create_data_process: " +
                  str(data_process))

        # Validation
        if not data_process.atbd_reference:
            raise BadRequest("Invalid ATBD reference " +
                             str(data_process.atbd_reference))
            # TODO: Check that ATBD reference is valid
        # TODO: Replace dict definition with a transform object definition

        # Assemble transform input data
        transform_name = str(data_process.atbd_reference) + \
                         " - " + \
                         str(out_data_product_id) + \
                         time.ctime()
        transform_object = IonObject(RT.Transform, name=transform_name)
        transform_object.atbd_reference = data_process.atbd_reference
        transform_object.sourcecode = data_process.sourcecode
        transform_object.in_subscription_id = in_subscription_id
        transform_object.out_data_product_id = out_data_product_id

        # Register the transform with the transform mgmt service
        transform_id =\
            self.clients.transform_management_service.\
                create_transform(transform_object)

        # Create and store a new DataProcess with the resource registry
        data_process_id, version =\
            self.clients.resource_registry.\
                create(data_process)

        # TODO: Flesh details of transform mgmt svc schedule and bind methods
        self.clients.transform_management_service.\
            schedule_transform(transform_id)
        self.clients.transform_management_service.\
            bind_transform(transform_id)

        # Register data process as a producer
        self.clients.data_acquisition_management.\
            register_process(data_process_id)

        # Associations
        self.clients.resource_registry.create_association(data_process_id,
                                                          AT.hasInputProduct,
                                                          in_subscription_id)
        self.clients.resource_registry.create_association(data_process_id,
                                                          AT.hasOutputProduct,
                                                          out_data_product_id)
        self.clients.resource_registry.create_association(data_process_id,
                                                          AT.hasTransform,
                                                          transform_id)

        return data_process_id

    def update_data_process(self, data_process={}):
        """
        @param      data_process dict which defines the general resource
                        and the specific transform
        @retval     {success: True or False}
        """
        log.debug("DataProcessManagementService:update_data_process: " +
                  str(data_process))
        data_process_id = self.clients.resource_registry.update(data_process)
        transform_ids, _ = self.clients.resource_registry.\
            find_associations(data_process_id, AT.hasTransform)
        if not transform_ids:
            raise NotFound("No transform associated with data process ID " +
                           str(data_process_id))
        goodUpdate = True
        for x in transform_ids:
            transform_obj = self.clients.transform_management_service.\
                read_transform(x)
            transform_obj.atbd_reference = data_process.atbd_reference
            transform_obj.sourcecode = data_process.sourcecode
            transform_obj.in_subscription_id = data_process.in_subscription_id
            transform_obj.out_data_product_id = data_process.out_data_product_id
            goodUpdate = goodUpdate & \
                         self.clients.transform_management_service.\
                            update_transform(transform_obj)
        return goodUpdate

    def read_data_process(self, data_process_id=''):
        """
        @param  data_process_id: ID of the data process resource of interest
        @retval data_process object
        """
        log.debug("DataProcessManagementService:read_data_process: " +
                  str(data_process_id))
        data_process_obj, _ = self.clients.resource_registry.\
            read(data_process_id)
        if not data_process_obj:
            raise NotFound("Data Process does not exist " +
                            str(data_process_id))
        return data_process_obj

    def delete_data_process(self, data_process_id=''):
        """
        @param  data_process_id: ID of the data process resource to be stopped
        @retval {success: True or False}
        """
        log.debug("DataProcessManagementService:delete_data_process: " +
                  str(data_process_id))
        if not data_process_id:
            raise BadRequest("Delete failed.  Missing data_process_id.")

        # Delete associations of the data process
        associations, _ = self.clients.resource_registry.\
            find_associations(data_process_id, None)
        if associations:
            for x in associations:
                self.clients.resource_registry.delete_association(x)

        # Delete the data process object
        data_process_obj = self.clients.resource_registry.read(data_process_id)
        if not data_process_obj:
            raise NotFound("Data Process (ID: " +
                           data_process_id +
                           ") does not exist")
        self.clients.resource_registry.delete(data_process_obj)
        return {"success": True}

    def find_data_process(self, filters={}):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc.
        @retval
        """
        log.debug("DataProcessManagementService:find_data_process")
        # Return Value
        # ------------
        # data_process_list: []
        #

    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass

    def prepare_transform(self, in_transform):
        """
        @param      in_transform    transform object as passed in
        #retval     transform
        """
