#!/usr/bin/env python

"""
@package  ion.services.sa.instrument_management.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


#from pyon.public import Container
from pyon.public import LCS
#from pyon.public import AT
from pyon.core.bootstrap import IonObject
#from pyon.core.exception import BadRequest #, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
#from pyon.util.log import log

from ion.services.sa.instrument_management.instrument_agent_worker import InstrumentAgentWorker
from ion.services.sa.instrument_management.instrument_agent_instance_worker import InstrumentAgentInstanceWorker
from ion.services.sa.instrument_management.instrument_model_worker import InstrumentModelWorker
from ion.services.sa.instrument_management.instrument_device_worker import InstrumentDeviceWorker

from ion.services.sa.instrument_management.platform_agent_worker import PlatformAgentWorker
from ion.services.sa.instrument_management.platform_agent_instance_worker import PlatformAgentInstanceWorker
from ion.services.sa.instrument_management.platform_model_worker import PlatformModelWorker
from ion.services.sa.instrument_management.platform_device_worker import PlatformDeviceWorker

from ion.services.sa.instrument_management.sensor_model_worker import SensorModelWorker
from ion.services.sa.instrument_management.sensor_device_worker import SensorDeviceWorker


######
"""
now TODO

 - docstrings


Later TODO (need new methods spec'd out)

 - logical and physical stuff?
 - direct access
 - platform direct access
 - attachments
 -

"""
######




from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access

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
            self.RR    = self.clients.resource_registry
            
        if hasattr(self.clients, "data acquisition_management_service"):
            self.DAMS  = self.clients.data_acquisition_management_service

        #farm everything out to the workers

        self.instrument_agent           = InstrumentAgentWorker(self.clients)
        self.instrument_agent_instance  = InstrumentAgentInstanceWorker(self.clients)
        self.instrument_model           = InstrumentModelWorker(self.clients)
        self.instrument_device          = InstrumentDeviceWorker(self.clients)

        self.platform_agent           = PlatformAgentWorker(self.clients)
        self.platform_agent_instance  = PlatformAgentInstanceWorker(self.clients)
        self.platform_model           = PlatformModelWorker(self.clients)
        self.platform_device          = PlatformDeviceWorker(self.clients)

        self.sensor_model    = SensorModelWorker(self.clients)
        self.sensor_device   = SensorDeviceWorker(self.clients)



    ##########################################################################
    #
    # INSTRUMENT AGENT INSTANCE
    #
    ##########################################################################

    def create_instrument_agent_instance(self, instrument_agent_instance={}):
        """
        create a new instance
        @param instrument_agent_instance the object to be created as a resource
        @retval instrument_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent_instance.create_one(instrument_agent_instance)

    def update_instrument_agent_instance(self, instrument_agent_instance={}):
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

    def find_instrument_agent_instances(self, filters={}):
        """

        """
        return self.instrument_agent_instance.find_some(filters)




    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################

    def create_instrument_agent(self, instrument_agent={}):
        """
        create a new instance
        @param instrument_agent the object to be created as a resource
        @retval instrument_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_agent.create_one(instrument_agent)

    def update_instrument_agent(self, instrument_agent={}):
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

    def find_instrument_agents(self, filters={}):
        """

        """
        return self.instrument_agent.find_some(filters)

    #FIXME: args need to change
    def assign_instrument_agent(self, instrument_agent_id='', instrument_id='', instrument_agent_instance={}):
        """
        @todo the arguments for this function seem incorrect and/or mismatched
        """
        raise NotImplementedError()
        #return self.instrument_agent.assign(instrument_agent_id, instrument_id, instrument_agent_instance)

    #FIXME: args need to change
    def unassign_instrument_agent(self, instrument_agent_id='', instrument_id=''):

        """
        @todo the arguments for this function seem incorrect and/or mismatched

        """
        raise NotImplementedError()
        #return self.instrument_agent.unassign(instrument_agent_id, instrument_device_id, instrument_agent_instance)




    ##########################################################################
    #
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument_model(self, instrument_model={}):
        """
        create a new instance
        @param instrument_model the object to be created as a resource
        @retval instrument_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_model.create_one(instrument_model)

    def update_instrument_model(self, instrument_model={}):
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

    def find_instrument_models(self, filters={}):
        """

        """
        return self.instrument_model.find_some(filters)

    def assign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """
        Assign a model to a device type
        @param instrument_model_id the model id
        @param instrument_device_id the device id
        @retval success whether there was success
        """
        return self.instrument_device.link_model(instrument_device_id, instrument_model_id)

    def unassign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """
        Assign a model from a device type
        @param instrument_model_id the model id
        @param instrument_device_id the device id
        @retval success whether there was success

        """
        return self.instrument_device.unlink_model(instrument_device_id, instrument_model_id)








    ##########################################################################
    #
    # PHYSICAL INSTRUMENT
    #
    ##########################################################################

    def create_instrument_device(self, instrument_device={}):
        """
        create a new instance
        @param instrument_device the object to be created as a resource
        @retval instrument_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_device.create_one(instrument_device)

    def update_instrument_device(self, instrument_device={}):
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

    def find_instrument_devices(self, filters={}):
        """

        """
        return self.instrument_device.find_some(filters)




    ##################### INSTRUMENT LIFECYCLE ADVANCEMENT_ACTIONS




    def plan_instrument_device(self, name='', description='', instrument_model_id=''):
        """
        Plan an instrument: at this point, we know only its name, description, and model
        @todo this state may no longer be valid due to changes in available lifecycle states
        """

        #create the new resource
        new_inst_obj = IonObject("InstrumentDevice",
                                 name=name,
                                 description=description)
        instrument_device_id = self.instrument_device.create_one(instrument_device=new_inst_obj)

        #associate the model
        self.link_model(instrument_device_id, instrument_model_id)

        #move the association
        self.instrument_device.advance_lcs(instrument_device_id, LCS.PLANNED)

        return self.instrument_device._return_create("instrument_device_id", instrument_device_id)


    def acquire_instrument_device(self, instrument_device_id='', serialnumber='', firmwareversion='', hardwareversion=''):
        """
        When physical instrument is acquired, create all data products
        @todo this state may no longer be valid due to changes in available lifecycle states
        """

        #read instrument
        inst_obj = self.instrument_device.read(instrument_device_id=instrument_device_id)

        #update instrument with new params
        inst_obj.serialnumber     = serialnumber
        inst_obj.firmwareversion  = firmwareversion
        inst_obj.hardwareversion  = hardwareversion

        #FIXME: check this for an error
        self.instrument_device.update(instrument_device_id, inst_obj)


        #get data producer id from data acquisition management service
        pducer_id = self.DAMS.register_instrument(instrument_id=instrument_device_id)

        # associate data product with instrument
        self.instrument_device.link_data_producer(instrument_device_id, pducer_id)

        # set up the rest of the associations
        setup_dp_result = self.instrument_device.setup_data_production_chain(instrument_device_id)

        #FIXME: lifecycle state transition?

        return self.instrument_device._return_update(setup_dp_result)


    def develop_instrument_device(self, instrument_device_id='', instrument_agent_id=''):
        """
        Assign an instrument agent (just the type, not the instance) to an instrument
        @todo this state may no longer be valid due to changes in available lifecycle states
        """

        #FIXME: only valid in 'ACQUIRED' state!

        #FIXME: what to associate here?

        self.instrument_device.advance_lcs(instrument_device_id, LCS.DEVELOPED)

        #FIXME: error checking

        return self.instrument_device._return_update(True)


    def commission_instrument_device(self, instrument_device_id='', platform_device_id=''):
        """
        @todo this state may no longer be valid due to changes in available lifecycle states

        """
        #FIXME: only valid in 'DEVELOPED' state!

        #FIXME: there seems to be no association between instruments and platforms

        self.instrument_device.advance_lcs(instrument_device_id, LCS.COMMISSIONED)

        return self.instrument_device._return_update(True)


    def decommission_instrument_device(self, instrument_device_id=''):
        """
        @todo this state may no longer be valid due to changes in available lifecycle states

        """
        #FIXME: only valid in 'COMMISSIONED' state!

        #FIXME: there seems to be no association between instruments and platforms
        self.instrument_device.advance_lcs(instrument_device_id, LCS.DEVELOPED)

        return self.instrument_device_return_update(True)


    def activate_instrument_device(self, instrument_device_id='', instrument_agent_instance_id=''):
        """
        @todo this state may no longer be valid due to changes in available lifecycle states

        """
        #FIXME: only valid in 'COMMISSIONED' state!

        #FIXME: validate somehow

        self.instrument_device.link_agent_instance(instrument_device_id, instrument_agent_instance_id)

        self.instrument_device.advance_lcs(instrument_device_id, LCS.ACTIVE)

        self.instrument_device._return_activate(True)

    def deactivate_instrument_device(self, instrument_device_id=''):
        """
        @todo this state may no longer be valid due to changes in available lifecycle states

        """

        #FIXME: only valid in 'ACTIVE' state!

        #FIXME: remove association

        self.instrument_device.advance_lcs(instrument_device_id, LCS.DEVELOPED)

        return self.instrument_device._return_update(True)

    def retire_instrument_device(self, instrument_device_id=''):
        """
        Retire an instrument
        @todo this state may no longer be valid due to changes in available lifecycle states
        """

        #FIXME: what happens to logical instrument, platform, etc

        self.instrument_device.advance_lcs(instrument_device_id, LCS.RETIRED)

        return self.instrument_device._return_update(True)




    def assign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        @todo the arguments to this function have the wrong names

        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        logical_instrument_id = ''
        return self.instrument_device.link_assignment(instrument_device_id, logical_instrument_id)

    def unassign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        @todo the arguments to this function have the wrong names

        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        logical_instrument_id = ''
        return self.instrument_device.unlink_assignment(instrument_device_id, logical_instrument_id)





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

    def create_platform_agent_instance(self, platform_agent_instance={}):
        """
        create a new instance
        @param platform_agent_instance the object to be created as a resource
        @retval platform_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent_instance.create_one(platform_agent_instance)

    def update_platform_agent_instance(self, platform_agent_instance={}):
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

    def find_platform_agent_instances(self, filters={}):
        """

        """
        return self.platform_agent_instance.find_some(filters)






    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################


    def create_platform_agent(self, platform_agent={}):
        """
        create a new instance
        @param platform_agent the object to be created as a resource
        @retval platform_agent_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_agent.create_one(platform_agent)

    def update_platform_agent(self, platform_agent={}):
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

    def find_platform_agents(self, filters={}):
        """

        """
        return self.platform_agent.find_some(filters)

    #FIXME: args need to change
    def assign_platform_agent(self, platform_agent_id='', platform_id='', platform_agent_instance={}):
        """
        @todo the arguments for this function seem incorrect and/or mismatched
        """
        raise NotImplementedError()
        #return self.platform_agent.assign(platform_agent_id, platform_id, platform_agent_instance)

    #FIXME: args need to change
    def unassign_platform_agent(self, platform_agent_id='', platform_id=''):

        """
        @todo the arguments for this function seem incorrect and/or mismatched

        """
        raise NotImplementedError()
        #return self.platform_agent.unassign(platform_agent_id, platform_device_id, platform_agent_instance)




    ##########################################################################
    #
    # PLATFORM MODEL
    #
    ##########################################################################


    def create_platform_model(self, platform_model={}):
        """
        create a new instance
        @param platform_model the object to be created as a resource
        @retval platform_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_model.create_one(platform_model)

    def update_platform_model(self, platform_model={}):
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

    def find_platform_models(self, filters={}):
        """

        """
        return self.platform_model.find_some(filters)

    def assign_platform_model(self, platform_model_id='', platform_device_id=''):
        """
        @todo this seems backwards
        """
        return self.platform_model.assign(platform_model_id, platform_device_id)

    def unassign_platform_model(self, platform_model_id='', platform_device_id=''):
        """
        @todo this seems backwards
        """
        return self.platform_model.assign(platform_model_id, platform_device_id)






    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device={}):
        """
        create a new instance
        @param platform_device the object to be created as a resource
        @retval platform_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.platform_device.create_one(platform_device)

    def update_platform_device(self, platform_device={}):
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

    def find_platform_devices(self, filters={}):
        """

        """
        return self.platform_device.find_some(filters)






    ##########################################################################
    #
    # SENSOR MODEL
    #
    ##########################################################################


    def create_sensor_model(self, sensor_model={}):
        """
        create a new instance
        @param sensor_model the object to be created as a resource
        @retval sensor_model_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_model.create_one(sensor_model)

    def update_sensor_model(self, sensor_model={}):
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

    def find_sensor_models(self, filters={}):
        """

        """
        return self.sensor_model.find_some(filters)

    def assign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """
        @todo this seems backwards... should be device, model
        """
        return self.sensor_model.assign(sensor_model_id, sensor_device_id)

    def unassign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """
        @todo this seems backwards... should be device, model

        """
        return self.sensor_model.unassign(sensor_model_id, sensor_device_id)




    ##########################################################################
    #
    # PHYSICAL SENSOR
    #
    ##########################################################################



    def create_sensor_device(self, sensor_device={}):
        """
        create a new instance
        @param sensor_device the object to be created as a resource
        @retval sensor_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.sensor_device.create_one(sensor_device)

    def update_sensor_device(self, sensor_device={}):
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

    def find_sensor_devices(self, filters={}):
        """

        """
        return self.sensor_device.find_some(filters)


