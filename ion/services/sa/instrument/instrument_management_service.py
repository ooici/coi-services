#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


#from pyon.public import Container
from pyon.public import LCS #, LCE
from pyon.public import RT, PRED
from pyon.core.bootstrap import IonObject
#from pyon.core.exception import BadRequest #, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log

from ion.services.sa.resource_impl.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.resource_impl.instrument_agent_instance_impl import InstrumentAgentInstanceImpl
from ion.services.sa.resource_impl.instrument_model_impl import InstrumentModelImpl
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl

from ion.services.sa.resource_impl.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.resource_impl.platform_agent_instance_impl import PlatformAgentInstanceImpl
from ion.services.sa.resource_impl.platform_model_impl import PlatformModelImpl
from ion.services.sa.resource_impl.platform_device_impl import PlatformDeviceImpl

from ion.services.sa.resource_impl.sensor_model_impl import SensorModelImpl
from ion.services.sa.resource_impl.sensor_device_impl import SensorDeviceImpl


# TODO: these are for methods which may belong in DAMS and DPMS
from ion.services.sa.resource_impl.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.data_producer_impl import DataProducerImpl


from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access

    """
    def on_init(self):
        #suppress a few "variable declared but not used" annoying pyflakes errors
        IonObject("Resource") 
        log 

        self.override_clients(self.clients)

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        # we hide these behind checks even though we expect them so that
        # the resource_impl_metatests will work
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry
            
        if hasattr(self.clients, "data_acquisition_management"):
            self.DAMS  = self.clients.data_acquisition_management

        if hasattr(self.clients, "data_product_management"):
            self.DPMS  = self.clients.data_product_management

        #farm everything out to the impls

        self.instrument_agent           = InstrumentAgentImpl(self.clients)
        self.instrument_agent_instance  = InstrumentAgentInstanceImpl(self.clients)
        self.instrument_model           = InstrumentModelImpl(self.clients)
        self.instrument_device          = InstrumentDeviceImpl(self.clients)

        self.platform_agent           = PlatformAgentImpl(self.clients)
        self.platform_agent_instance  = PlatformAgentInstanceImpl(self.clients)
        self.platform_model           = PlatformModelImpl(self.clients)
        self.platform_device          = PlatformDeviceImpl(self.clients)

        self.sensor_model    = SensorModelImpl(self.clients)
        self.sensor_device   = SensorDeviceImpl(self.clients)

        #TODO: may not belong in this service
        self.data_product   = DataProductImpl(self.clients)
        self.data_producer  = DataProducerImpl(self.clients)


    ##########################################################################
    #
    # INSTRUMENT AGENT INSTANCE
    #
    ##########################################################################

    def create_instrument_agent_instance(self, instrument_agent_instance=None):
        """
        create a new instance
        @param instrument_agent_instance the object to be created as a resource
        @retval instrument_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent_instance.create_one(instrument_agent_instance)

    def update_instrument_agent_instance(self, instrument_agent_instance=None):
        """
        update an existing instance
        @param instrument_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent_instance.update_one(instrument_agent_instance)


    def read_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_instance_id the id of the object to be fetched
        @retval InstrumentAgentInstance resource
        """
        return self.instrument_agent_instance.read_one(instrument_agent_instance_id)

    def delete_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_agent_instance.delete_one(instrument_agent_instance_id)

    def find_instrument_agent_instances(self, filters=None):
        """

        """
        return self.instrument_agent_instance.find_some(filters)




    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################

    def create_instrument_agent(self, instrument_agent=None):
        """
        create a new instance
        @param instrument_agent the object to be created as a resource
        @retval instrument_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent.create_one(instrument_agent)

    def update_instrument_agent(self, instrument_agent=None):
        """
        update an existing instance
        @param instrument_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent.update_one(instrument_agent)


    def read_instrument_agent(self, instrument_agent_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_id the id of the object to be fetched
        @retval InstrumentAgent resource
        """
        return self.instrument_agent.read_one(instrument_agent_id)

    def delete_instrument_agent(self, instrument_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_agent.delete_one(instrument_agent_id)

    def find_instrument_agents(self, filters=None):
        """

        """
        return self.instrument_agent.find_some(filters)



    ##########################################################################
    #
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument_model(self, instrument_model=None):
        """
        create a new instance
        @param instrument_model the object to be created as a resource
        @retval instrument_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_model.create_one(instrument_model)

    def update_instrument_model(self, instrument_model=None):
        """
        update an existing instance
        @param instrument_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_model.update_one(instrument_model)


    def read_instrument_model(self, instrument_model_id=''):
        """
        fetch a resource by ID
        @param instrument_model_id the id of the object to be fetched
        @retval InstrumentModel resource
        """
        return self.instrument_model.read_one(instrument_model_id)

    def delete_instrument_model(self, instrument_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_model.delete_one(instrument_model_id)

    def find_instrument_models(self, filters=None):
        """

        """
        return self.instrument_model.find_some(filters)





    ##########################################################################
    #
    # PHYSICAL INSTRUMENT
    #
    ##########################################################################


    def setup_data_production_chain(self, instrument_device_id=''):
        """
        create a data product (L0) for the instrument, and establish provenance
        between the corresponding data producers
        """

        #get instrument object and instrument's data producer
        inst_obj = self.instrument_device.read_one(instrument_device_id)
        inst_pducers = self.instrument_device.find_stemming_data_producer(instrument_device_id)
        inst_pducer_id = inst_pducers[0]
        log.debug("instrument data producer id='%s'" % inst_pducer_id)

        #create a new data product
        dpms_pduct_obj = IonObject(RT.DataProduct,
                                   name=str(inst_obj.name + " L0 Product"),
                                   description=str("L0 DataProduct for " + inst_obj.name))

        pduct_id = self.DPMS.create_data_product(dpms_pduct_obj)


        # get data product's data producer (via association)
        prod_pducers = self.data_product.find_stemming_data_producer(pduct_id)
        prod_pducer_id = prod_pducers[0]
        
        # (TODO: there should only be one assoc_id.  what error to raise?)
        # TODO: what error to raise if there are no assoc ids?

        # instrument data producer is the parent of the data product producer
        self.data_producer.link_input_data_producer(prod_pducer_id, inst_pducer_id)

        #TODO: error checking



    def create_instrument_device(self, instrument_device=None):
        """
        create a new instance
        @param instrument_device the object to be created as a resource
        @retval instrument_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        instrument_device_id = self.instrument_device.create_one(instrument_device)
        self.DAMS.register_instrument(instrument_device_id)
        
        #TODO: create data producer and product
        self.setup_data_production_chain(instrument_device_id)

        return instrument_device_id

    def update_instrument_device(self, instrument_device=None):
        """
        update an existing instance
        @param instrument_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_device.update_one(instrument_device)


    def read_instrument_device(self, instrument_device_id=''):
        """
        fetch a resource by ID
        @param instrument_device_id the id of the object to be fetched
        @retval InstrumentDevice resource

        """
        return self.instrument_device.read_one(instrument_device_id)

    def delete_instrument_device(self, instrument_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.instrument_device.delete_one(instrument_device_id)

    def find_instrument_devices(self, filters=None):
        """

        """
        return self.instrument_device.find_some(filters)



    ##
    ##
    ##  DIRECT ACCESS
    ##
    ##

    def request_direct_access(self, instrument_device_id=''):
        """

        """

        # determine whether id is for physical or logical instrument
        # look up instrument if not

        # Validate request; current instrument state, policy, and other

        # Retrieve and save current instrument settings

        # Request DA channel, save reference

        # Return direct access channel
        raise NotImplementedError()
        pass

    def stop_direct_access(self, instrument_device_id=''):
        """

        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass







    ##########################################################################
    #
    # PLATFORM AGENT INSTANCE
    #
    ##########################################################################

    def create_platform_agent_instance(self, platform_agent_instance=None):
        """
        create a new instance
        @param platform_agent_instance the object to be created as a resource
        @retval platform_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent_instance.create_one(platform_agent_instance)

    def update_platform_agent_instance(self, platform_agent_instance=None):
        """
        update an existing instance
        @param platform_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent_instance.update_one(platform_agent_instance)


    def read_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        fetch a resource by ID
        @param platform_agent_instance_id the id of the object to be fetched
        @retval PlatformAgentInstance resource
        """
        return self.platform_agent_instance.read_one(platform_agent_instance_id)

    def delete_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_agent_instance.delete_one(platform_agent_instance_id)

    def find_platform_agent_instances(self, filters=None):
        """

        """
        return self.platform_agent_instance.find_some(filters)






    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################


    def create_platform_agent(self, platform_agent=None):
        """
        create a new instance
        @param platform_agent the object to be created as a resource
        @retval platform_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent.create_one(platform_agent)

    def update_platform_agent(self, platform_agent=None):
        """
        update an existing instance
        @param platform_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.platform_agent.update_one(platform_agent)


    def read_platform_agent(self, platform_agent_id=''):
        """
        fetch a resource by ID
        @param platform_agent_id the id of the object to be fetched
        @retval PlatformAgent resource

        """
        return self.platform_agent.read_one(platform_agent_id)

    def delete_platform_agent(self, platform_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_agent.delete_one(platform_agent_id)

    def find_platform_agents(self, filters=None):
        """

        """
        return self.platform_agent.find_some(filters)



    ##########################################################################
    #
    # PLATFORM MODEL
    #
    ##########################################################################


    def create_platform_model(self, platform_model=None):
        """
        create a new instance
        @param platform_model the object to be created as a resource
        @retval platform_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_model.create_one(platform_model)

    def update_platform_model(self, platform_model=None):
        """
        update an existing instance
        @param platform_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_model.update_one(platform_model)


    def read_platform_model(self, platform_model_id=''):
        """
        fetch a resource by ID
        @param platform_model_id the id of the object to be fetched
        @retval PlatformModel resource

        """
        return self.platform_model.read_one(platform_model_id)

    def delete_platform_model(self, platform_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_model.delete_one(platform_model_id)

    def find_platform_models(self, filters=None):
        """

        """
        return self.platform_model.find_some(filters)




    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device=None):
        """
        create a new instance
        @param platform_device the object to be created as a resource
        @retval platform_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_device.create_one(platform_device)

    def update_platform_device(self, platform_device=None):
        """
        update an existing instance
        @param platform_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.platform_device.update_one(platform_device)


    def read_platform_device(self, platform_device_id=''):
        """
        fetch a resource by ID
        @param platform_device_id the id of the object to be fetched
        @retval PlatformDevice resource

        """
        return self.platform_device.read_one(platform_device_id)

    def delete_platform_device(self, platform_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.platform_device.delete_one(platform_device_id)

    def find_platform_devices(self, filters=None):
        """

        """
        return self.platform_device.find_some(filters)






    ##########################################################################
    #
    # SENSOR MODEL
    #
    ##########################################################################


    def create_sensor_model(self, sensor_model=None):
        """
        create a new instance
        @param sensor_model the object to be created as a resource
        @retval sensor_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_model.create_one(sensor_model)

    def update_sensor_model(self, sensor_model=None):
        """
        update an existing instance
        @param sensor_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.sensor_model.update_one(sensor_model)


    def read_sensor_model(self, sensor_model_id=''):
        """
        fetch a resource by ID
        @param sensor_model_id the id of the object to be fetched
        @retval SensorModel resource

        """
        return self.sensor_model.read_one(sensor_model_id)

    def delete_sensor_model(self, sensor_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.sensor_model.delete_one(sensor_model_id)

    def find_sensor_models(self, filters=None):
        """

        """
        return self.sensor_model.find_some(filters)



    ##########################################################################
    #
    # PHYSICAL SENSOR
    #
    ##########################################################################



    def create_sensor_device(self, sensor_device=None):
        """
        create a new instance
        @param sensor_device the object to be created as a resource
        @retval sensor_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_device.create_one(sensor_device)

    def update_sensor_device(self, sensor_device=None):
        """
        update an existing instance
        @param sensor_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.sensor_device.update_one(sensor_device)


    def read_sensor_device(self, sensor_device_id=''):
        """
        fetch a resource by ID
        @param sensor_device_id the id of the object to be fetched
        @retval SensorDevice resource

        """
        return self.sensor_device.read_one(sensor_device_id)

    def delete_sensor_device(self, sensor_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        return self.sensor_device.delete_one(sensor_device_id)

    def find_sensor_devices(self, filters=None):
        """

        """
        return self.sensor_device.find_some(filters)



    ##########################################################################
    #
    # ASSOCIATIONS
    #
    ##########################################################################


    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.instrument_device.link_model(instrument_device_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.instrument_device.unlink_model(instrument_device_id, instrument_model_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.link_model(instrument_agent_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_model(instrument_agent_id, instrument_model_id)


    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.link_model(sensor_device_id, sensor_model_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.unlink_model(sensor_device_id, sensor_model_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.link_model(platform_device_id, platform_model_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.unlink_model(platform_device_id, platform_model_id)

    def assign_instrument_device_to_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.link_instrument(platform_device_id, instrument_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.unlink_instrument(platform_device_id, instrument_device_id)

    def assign_logical_instrument_to_instrument_device(self, logical_instrument_id='', instrument_device_id=''):
        self.instrument_device.link_assignment(instrument_device_id, logical_instrument_id)

    def unassign_logical_instrument_from_instrument_device(self, logical_instrument_id='', instrument_device_id=''):
        self.instrument_device.unlink_assignment(instrument_device_id, logical_instrument_id)

    def assign_logical_platform_to_platform_device(self, logical_platform_id='', platform_device_id=''):
        self.platform_device.link_assignment(platform_device_id, logical_platform_id)

    def unassign_logical_platform_from_platform_device(self, logical_platform_id='', platform_device_id=''):
        self.platform_device.unlink_assignment(platform_device_id, logical_platform_id)

    def assign_platform_agent_instance_to_platform_agent(self, platform_agent_instance_id='', platform_agent_id=''):
        self.platform_agent.link_instance(platform_agent_id, platform_agent_instance_id)

    def unassign_platform_agent_instance_from_platform_agent(self, platform_agent_instance_id='', platform_agent_id=''):
        self.platform_agent.unlink_instance(platform_agent_id, platform_agent_instance_id)

    def assign_instrument_agent_instance_to_instrument_agent(self, instrument_agent_instance_id='', instrument_agent_id=''):
        self.instrument_agent.link_instance(instrument_agent_id, instrument_agent_instance_id)

    def unassign_instrument_agent_instance_from_instrument_agent(self, instrument_agent_instance_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_instance(instrument_agent_id, instrument_agent_instance_id)


    ############################
    #
    #  ASSOCIATION FIND METHODS
    #
    ############################


    def find_instrument_model_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_model(instrument_device_id)

    def find_instrument_device_by_instrument_model(self, instrument_model_id=''):
        return self.instrument_device.find_having_model(instrument_model_id)

    def find_platform_model_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_model(platform_device_id)

    def find_platform_device_by_platform_model(self, platform_model_id=''):
        return self.platform_device.find_having_model(platform_model_id)

    def find_instrument_model_by_instrument_agent(self, instrument_agent_id=''):
        return self.instrument_agent.find_stemming_model(instrument_agent_id)

    def find_instrument_agent_by_instrument_model(self, instrument_model_id=''):
        return self.instrument_agent.find_having_model(instrument_model_id)

    def find_instrument_device_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_instrument(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.platform_device.find_having_instrument(instrument_device_id)

    def find_instrument_device_by_logical_instrument(self, logical_instrument_id=''):
        return self.instrument_device.find_having_assignment(logical_instrument_id)

    def find_logical_instrument_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_assignment(instrument_device_id)

    def find_platform_device_by_logical_platform(self, logical_platform_id=''):
        return self.platform_device.find_having_assignment(logical_platform_id)

    def find_logical_platform_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_assignment(platform_device_id)



    ############################
    #
    #  SPECIALIZED FIND METHODS
    #
    ############################

    def find_data_product_by_instrument_device(self, instrument_device_id=''):
        log.debug("FIND DATA PRODUCT BY INSTRUMNET DEVICE")
        #init return value, a list of data sets
        data_products = []

        #init working set of data producers to walk
        data_producers = []
        pducers = self.instrument_device.find_stemming_data_producer(instrument_device_id)
        data_producers += pducers


        #iterate through all un-processed data producers (could also do recursively)
        while 0 < len(data_producers):
           producer_id = data_producers.pop()
           log.debug("Analyzing data producer '%s'" % producer_id)
           #get any products that are associated with this data producer and return them
           new_data_products = self.data_product.find_having_data_producer(producer_id)
           #get any producers that receive input from this data producer
           new_data_producers = self.data_producer.find_having_input_data_producer(producer_id)

           log.debug("Got %d new products, %d new producers" % (len(new_data_products), 
                                                                len(new_data_producers)))

           data_products  += new_data_products
           data_producers += new_data_producers

        return data_products

    def find_data_product_by_platform_device(self, platform_device_id=''):
        ret = []
        for i in self.find_instrument_device_by_platform_device(platform_device_id):
            data_products = self.find_data_product_by_instrument_device(i)
            for d in data_products:
                if not d in ret:
                    ret.append(d)

        return ret
        


    ############################
    #
    #  LIFECYCLE TRANSITIONS
    #
    ############################


    def set_instrument_agent_lifecycle(self, instrument_agent_id="", lifecycle_state=""):
       """
       declare a instrument_agent to be in a given state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, lifecycle_state)

    def set_instrument_agent_instance_lifecycle(self, instrument_agent_instance_id="", lifecycle_state=""):
       """
       declare a instrument_agent_instance to be in a given state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, lifecycle_state)

    def set_instrument_model_lifecycle(self, instrument_model_id="", lifecycle_state=""):
       """
       declare a instrument_model to be in a given state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, lifecycle_state)

    def set_instrument_device_lifecycle(self, instrument_device_id="", lifecycle_state=""):
       """
       declare an instrument_device to be in a given state
       @param instrument_device_id the resource id
       """
       return self.instrument_device.advance_lcs(instrument_device_id, lifecycle_state)

    def set_platform_agent_lifecycle(self, platform_agent_id="", lifecycle_state=""):
       """
       declare a platform_agent to be in a given state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, lifecycle_state)

    def set_platform_agent_instance_lifecycle(self, platform_agent_instance_id="", lifecycle_state=""):
       """
       declare a platform_agent_instance to be in a given state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, lifecycle_state)

    def set_platform_model_lifecycle(self, platform_model_id="", lifecycle_state=""):
       """
       declare a platform_model to be in a given state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, lifecycle_state)

    def set_platform_device_lifecycle(self, platform_device_id="", lifecycle_state=""):
       """
       declare a platform_device to be in a given state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, lifecycle_state)

    def set_sensor_model_lifecycle(self, sensor_model_id="", lifecycle_state=""):
       """
       declare a sensor_model to be in a given state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, lifecycle_state)

    def set_sensor_device_lifecycle(self, sensor_device_id="", lifecycle_state=""):
       """
       declare a sensor_device to be in a given state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, lifecycle_state)

