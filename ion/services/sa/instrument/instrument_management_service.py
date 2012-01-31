#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


#from pyon.public import Container
from pyon.public import LCS, LCE
#from pyon.public import PRED
from pyon.core.bootstrap import IonObject
#from pyon.core.exception import BadRequest #, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
#from pyon.util.log import log

from ion.services.sa.instrument.instrument_agent_impl import InstrumentAgentImpl
from ion.services.sa.instrument.instrument_agent_instance_impl import InstrumentAgentInstanceImpl
from ion.services.sa.instrument.instrument_model_impl import InstrumentModelImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl

from ion.services.sa.instrument.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.instrument.platform_agent_instance_impl import PlatformAgentInstanceImpl
from ion.services.sa.instrument.platform_model_impl import PlatformModelImpl
from ion.services.sa.instrument.platform_device_impl import PlatformDeviceImpl

from ion.services.sa.instrument.sensor_model_impl import SensorModelImpl
from ion.services.sa.instrument.sensor_device_impl import SensorDeviceImpl


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
            
        if hasattr(self.clients, "data_acquisition_management_service"):
            self.DAMS  = self.clients.data_acquisition_management_service

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

    def create_instrument_device(self, instrument_device=None):
        """
        create a new instance
        @param instrument_device the object to be created as a resource
        @retval instrument_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        return self.instrument_device.create_one(instrument_device)

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


    def assign_X_to_Y(self, X_id='', Y_id=''):
        self.Y.link_Z(Y_id, X_id)

    def unassign_X_from_Y(self, X_id='', Y_id=''):
        self.Y.unlink_Z(Y_id, X_id)

    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.link_model(sensor_device_id, sensor_model_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.unlink_model(sensor_device_id, sensor_model_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.link_model(platform_device_id, platform_model_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.unlink_model(platform_device_id, platform_model_id)

    def assign_instrument_device_to_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        raise NotImplementedError("Should be assign logical instrument!")
        self.instrument_device.link_assignment(instrument_device_id, logical_instrument_id)

    def unassign_instrument_device_from_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        raise NotImplementedError("Should be assign logical instrument!")
        self.instrument_device.unlink_assignment(instrument_device_id, logical_instrument_id)

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
    #  LIFECYCLE TRANSITIONS
    #
    ############################


    def declare_instrument_agent_planned(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the PLANNED state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.PLANNED)

    def declare_instrument_agent_developed(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the DEVELOPED state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.DEVELOPED)

    def declare_instrument_agent_integrated(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the INTEGRATED state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.INTEGRATED)

    def declare_instrument_agent_discoverable(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the DISCOVERABLE state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.DISCOVERABLE)

    def declare_instrument_agent_available(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the AVAILABLE state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.AVAILABLE)

    def declare_instrument_agent_retired(self, instrument_agent_id=""):
       """
       declare a instrument_agent to be in the RETIRED state
       @param instrument_agent_id the resource id
       """
       return self.instrument_agent.advance_lcs(instrument_agent_id, LCS.RETIRED)

    def declare_instrument_agent_instance_planned(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the PLANNED state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.PLANNED)

    def declare_instrument_agent_instance_developed(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the DEVELOPED state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.DEVELOPED)

    def declare_instrument_agent_instance_integrated(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the INTEGRATED state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.INTEGRATED)

    def declare_instrument_agent_instance_discoverable(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the DISCOVERABLE state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.DISCOVERABLE)

    def declare_instrument_agent_instance_available(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the AVAILABLE state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.AVAILABLE)

    def declare_instrument_agent_instance_retired(self, instrument_agent_instance_id=""):
       """
       declare a instrument_agent_instance to be in the RETIRED state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, LCS.RETIRED)

    def declare_instrument_model_planned(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the PLANNED state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.PLANNED)

    def declare_instrument_model_developed(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the DEVELOPED state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.DEVELOPED)

    def declare_instrument_model_integrated(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the INTEGRATED state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.INTEGRATED)

    def declare_instrument_model_discoverable(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the DISCOVERABLE state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.DISCOVERABLE)

    def declare_instrument_model_available(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the AVAILABLE state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.AVAILABLE)

    def declare_instrument_model_retired(self, instrument_model_id=""):
       """
       declare a instrument_model to be in the RETIRED state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, LCS.RETIRED)

    def declare_platform_agent_planned(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the PLANNED state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.PLANNED)

    def declare_platform_agent_developed(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the DEVELOPED state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.DEVELOPED)

    def declare_platform_agent_integrated(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the INTEGRATED state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.INTEGRATED)

    def declare_platform_agent_discoverable(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the DISCOVERABLE state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.DISCOVERABLE)

    def declare_platform_agent_available(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the AVAILABLE state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.AVAILABLE)

    def declare_platform_agent_retired(self, platform_agent_id=""):
       """
       declare a platform_agent to be in the RETIRED state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, LCS.RETIRED)

    def declare_platform_agent_instance_planned(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the PLANNED state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.PLANNED)

    def declare_platform_agent_instance_developed(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the DEVELOPED state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.DEVELOPED)

    def declare_platform_agent_instance_integrated(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the INTEGRATED state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.INTEGRATED)

    def declare_platform_agent_instance_discoverable(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the DISCOVERABLE state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.DISCOVERABLE)

    def declare_platform_agent_instance_available(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the AVAILABLE state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.AVAILABLE)

    def declare_platform_agent_instance_retired(self, platform_agent_instance_id=""):
       """
       declare a platform_agent_instance to be in the RETIRED state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, LCS.RETIRED)

    def declare_platform_model_planned(self, platform_model_id=""):
       """
       declare a platform_model to be in the PLANNED state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.PLANNED)

    def declare_platform_model_developed(self, platform_model_id=""):
       """
       declare a platform_model to be in the DEVELOPED state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.DEVELOPED)

    def declare_platform_model_integrated(self, platform_model_id=""):
       """
       declare a platform_model to be in the INTEGRATED state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.INTEGRATED)

    def declare_platform_model_discoverable(self, platform_model_id=""):
       """
       declare a platform_model to be in the DISCOVERABLE state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.DISCOVERABLE)

    def declare_platform_model_available(self, platform_model_id=""):
       """
       declare a platform_model to be in the AVAILABLE state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.AVAILABLE)

    def declare_platform_model_retired(self, platform_model_id=""):
       """
       declare a platform_model to be in the RETIRED state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, LCS.RETIRED)

    def declare_platform_device_planned(self, platform_device_id=""):
       """
       declare a platform_device to be in the PLANNED state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.PLANNED)

    def declare_platform_device_developed(self, platform_device_id=""):
       """
       declare a platform_device to be in the DEVELOPED state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.DEVELOPED)

    def declare_platform_device_integrated(self, platform_device_id=""):
       """
       declare a platform_device to be in the INTEGRATED state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.INTEGRATED)

    def declare_platform_device_discoverable(self, platform_device_id=""):
       """
       declare a platform_device to be in the DISCOVERABLE state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.DISCOVERABLE)

    def declare_platform_device_available(self, platform_device_id=""):
       """
       declare a platform_device to be in the AVAILABLE state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.AVAILABLE)

    def declare_platform_device_retired(self, platform_device_id=""):
       """
       declare a platform_device to be in the RETIRED state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, LCS.RETIRED)

    def declare_sensor_model_planned(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the PLANNED state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.PLANNED)

    def declare_sensor_model_developed(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the DEVELOPED state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.DEVELOPED)

    def declare_sensor_model_integrated(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the INTEGRATED state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.INTEGRATED)

    def declare_sensor_model_discoverable(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the DISCOVERABLE state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.DISCOVERABLE)

    def declare_sensor_model_available(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the AVAILABLE state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.AVAILABLE)

    def declare_sensor_model_retired(self, sensor_model_id=""):
       """
       declare a sensor_model to be in the RETIRED state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, LCS.RETIRED)

    def declare_sensor_device_planned(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the PLANNED state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.PLANNED)

    def declare_sensor_device_developed(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the DEVELOPED state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.DEVELOPED)

    def declare_sensor_device_integrated(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the INTEGRATED state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.INTEGRATED)

    def declare_sensor_device_discoverable(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the DISCOVERABLE state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.DISCOVERABLE)

    def declare_sensor_device_available(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the AVAILABLE state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.AVAILABLE)

    def declare_sensor_device_retired(self, sensor_device_id=""):
       """
       declare a sensor_device to be in the RETIRED state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, LCS.RETIRED)

