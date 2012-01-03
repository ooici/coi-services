#!/usr/bin/env python

"""
@package ion.services.sa.data_process_management.data_process_management_service
@author  Alon Yaari
"""

import time
from interface.services.sa.idata_process_management_service \
    import BaseDataProcessManagementService
from pyon.public import   log, RT, AT
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound

from ion.services.sa.data_process_management.data_process_dryer import DataProcessDryer


class DataProcessManagementService(BaseDataProcessManagementService):
    """ @author Alon Yaari
        @file   ion/services/sa/
                    data_process_management/data_process_management_service.py
        @brief  Implementation of the data process management service
    """

    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error

        self.override_clients(self.clients)

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR   = self.clients.resource_registry
            
        if hasattr(self.clients, "transform_management_service"):
            self.TMS  = self.clients.transform_management_service


        #farm everything out to the dryers

        self.data_process = DataProcessDryer(self.clients)




    def create_data_process_definition(self, data_process_definition={}):
        """
        @param      data_process_definition: dict with parameters to define
                        the data process def.
        @retval     data_process_definition_id: ID of the newly registered
                        data process def.
        """
        pass

    def update_data_process_definition(self, data_process_definition={}):
        """
        @param      data_process_definition: dict with parameters to update
                        the data process def.
        @retval     {"success": boolean}
        """
        pass

    def read_data_process_definition(self, data_process_definition_id=''):
        """
        @param      data_process_definition_id: ID of the data process
                        definition that defines the transform to read
        @retval     data process definition object
        """
        pass

    def delete_data_process_definition(self, data_process_definition_id=''):
        """
        @param      data_process_definition_id: ID of the data process
                        definition that defines the transform to delete
        @retval     {"success": boolean}
        """
        pass

    def find_data_process_definitions(self, filters={}):
        """
        @param      filters: dict of parameters to filter down
                        the list of possible data proc. defs
        @retval     list[] of data process definition IDs
        """
        pass

    def create_data_process(self,
                            data_process_definition_id='',
                            in_subscription_id='',
                            out_data_product_id=''):
        """
        @param  data_process_definition_id: Object with definition of the
                    transform to apply to the input data product
        @param  in_subscription_id: ID of the input data product
        @param  out_data_product_id: ID of the output data product
        @retval data_process_id: ID of the newly created data process object
        @throws BadRequest if missing any input values
        """
        inform = "Input Data Product:       "+str(in_subscription_id)+\
                 "Transformed by:           "+str(data_process_definition_id)+\
                 "To create output Product: "+str(out_data_product_id)
        log.debug("DataProcessManagementService:create_data_process()\n" +
                  inform)

        # Validate inputs
        if not data_process_definition_id:
            raise BadRequest("Missing data definition.")
        data_def_obj = \
            self.read_data_process_definition(data_process_definition_id)
        if not data_def_obj.process_source:
            raise BadRequest("Data definition has invalid process source.")
        if not in_subscription_id:
            raise BadRequest("Missing input data product ID.")
        if not out_data_product_id:
            raise BadRequest("Missing output data product ID.")

        # Assemble transform input data
        transform_name = str(data_process.atbd_reference) + \
                         " - " + \
                         str(out_data_product_id) + \
                         time.ctime()
        transform_object = IonObject(RT.Transform, name=transform_name)
        transform_object.process_definition_id = data_process_definition_id
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

    def update_data_process(self,
                            data_process_id,
                            data_process_definition_id='',
                            in_subscription_id='',
                            out_data_product_id=''):
        """
        @param  data_process_id: ID of the data process object to update
        @param  data_process_definition_id: Object with definition of the
                    updated transform to apply to the input data product
        @param  in_subscription_id: Updated ID of the input data product
        @param  out_data_product_id: Updated ID of data product to publish
                    process output to
        @retval {"success": boolean}
        """
        log.debug("DataProcessManagementService:update_data_process: " +
                  str(data_process))

        # Validate inputs
        if not data_process_id:
            raise BadRequest("Missing ID of data process to update.")
        if not data_process_definition_id \
            and not in_subscription_id \
            and not out_data_product_id:
            raise BadRequest("No values provided to update.")
        if data_process_definition_id:
            data_def_obj =\
                self.read_data_process_definition(data_process_definition_id)
            if not data_def_obj.process_source:
                raise BadRequest("Data definition has invalid process source.")

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
            if data_process_definition_id:
                transform_obj.process_definition_id = \
                    data_process_definition_id
            if in_subscription_id:
                transform_obj.in_subscription_id = \
                    data_process.in_subscription_id
            if out_data_product_id:
                transform_obj.out_data_product_id = \
                    data_process.out_data_product_id
            goodUpdate = goodUpdate & \
                         self.clients.transform_management_service.\
                            update_transform(transform_obj)
        return goodUpdate

    def read_data_process(self, data_process_id=""):
        """
        @param  data_process_id: ID of the data process resource of interest
        @retval data_process_definition_id: ID of the definition of the updated
                  transform being applied to the input data product
        @retval in_subscription_id: ID of the input data product
        @retval out_data_product_id: ID of the output data product
        """
        log.debug("DataProcessManagementService:read_data_process: " +
                  str(data_process_id))
        data_process_obj, _ = self.clients.resource_registry.\
            read(data_process_id)
        if not data_process_obj:
            raise NotFound("Data Process does not exist " +
                            str(data_process_id))
        return data_process_obj.data_process_definition_id, \
               data_process_obj.in_subscription_id, \
               data_process_obj.out_data_product_id

    def delete_data_process(self, data_process_id=""):
        """
        @param      data_process_id: ID of the data process resource to delete
        @retval     {"success": boolean}
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
        if not filters:
            data_process_list , _ = self.clients.resource_registry.\
                find_resources = (RT.DataProcess, None, None, True)
        return data_process_list

    def attach_process(self, process=''):
        """
        @param      process: Should this be the data_process_id?
        @retval
        """
        # TODO: Determine the proper input param
        pass

