#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


#from pyon.public import Container
#from pyon.public import AT
from pyon.core.bootstrap import IonObject #even though pyflakes complains
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

from ion.services.sa.instrument_management.logical_instrument_worker import LogicalInstrumentWorker
from ion.services.sa.instrument_management.logical_platform_worker import LogicalPlatformWorker


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
    
    def on_init(self):
        self.RR    = self.clients.resource_registry
        self.DAMS  = self.clients.data_acquisition_management_service

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

        self.logical_instrument  = LogicalInstrumentWorker(self.clients)
        self.logical_platform    = LogicalPlatformWorker(self.clients)

        IonObject() # suppress pyflakes error
    
    ##########################################################################
    #
    # INSTRUMENT AGENT
    #
    ##########################################################################

    def create_instrument_agent(self, instrument_agent={}):
        """
        method docstring
        """
        return self.instrument_agent.create_one(instrument_agent)

    def update_instrument_agent(self, instrument_agent={}):
        """
        method docstring
        """
        return self.instrument_agent.update_one(instrument_agent)
        

    def read_instrument_agent(self, instrument_agent_id=''):
        """
        method docstring
        """
        return self.instrument_agent.read_one(instrument_agent_id)

    def delete_instrument_agent(self, instrument_agent_id=''):
        """
        method docstring
        """
        return self.instrument_agent.delete_one(instrument_agent_id)

    def find_instrument_agents(self, filters={}):
        """
        method docstring
        """
        return self.instrument_agent.find_some(filters)

    #FIXME: args need to change
    def assign_instrument_agent(self, instrument_agent_id='', instrument_device_id='', instrument_agent_instance={}):
        """
        method docstring
        """
        raise NotImplementedError()
        return self.instrument_agent.assign(instrument_agent_id, instrument_device_id, instrument_agent_instance)

    #FIXME: args need to change
    def unassign_instrument_agent(self, instrument_agent_id='', instrument_device_id='', instrument_agent_instance={}):

        """
        method docstring
        """
        raise NotImplementedError()
        return self.instrument_agent.unassign(instrument_agent_id, instrument_device_id, instrument_agent_instance)




    
    ##########################################################################
    #
    # INSTRUMENT MODEL
    #
    ##########################################################################

    def create_instrument_model(self, instrument_model={}):
        """
        method docstring
        """
        return self.instrument_model.create_one(instrument_model)

    def update_instrument_model(self, instrument_model={}):
        """
        method docstring
        """
        return self.instrument_model.update_one(instrument_model)
        

    def read_instrument_model(self, instrument_model_id=''):
        """
        method docstring
        """
        return self.instrument_model.read_one(instrument_model_id)

    def delete_instrument_model(self, instrument_model_id=''):
        """
        method docstring
        """
        return self.instrument_model.delete_one(instrument_model_id)

    def find_instrument_models(self, filters={}):
        """
        method docstring
        """
        return self.instrument_model.find_some(filters)

    def assign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_model.assign(instrument_model_id, instrument_device_id)

    def unassign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_model.assign(instrument_model_id, instrument_device_id)








    ##########################################################################
    #
    # PHYSICAL INSTRUMENT
    #
    ##########################################################################

    def create_instrument_device(self, instrument_device={}):
        """
        method docstring
        """
        return self.instrument_device.create_one(instrument_device)

    def update_instrument_device(self, instrument_device={}):
        """
        method docstring
        """
        return self.instrument_device.update_one(instrument_device)
        

    def read_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_device.read_one(instrument_device_id)

    def delete_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_device.delete_one(instrument_device_id)

    def find_instrument_devices(self, filters={}):
        """
        method docstring
        """
        return self.instrument_device.find_some(filters)




    ##################### INSTRUMENT LIFECYCLE METHODS

    def plan_instrument_device(self, name='', description='', instrument_model_id=''):
        """
        Plan an instrument: at this point, we know only its name, description, and model
        """
        return self.instrument_device.plan(name, description, instrument_model_id)

    def acquire_instrument_device(self, instrument_device_id='', serialnumber='', firmwareversion='', hardwareversion=''):
        """
        When physical instrument is acquired, create all data products
        """
        return self.instrument_device.acquire(instrument_device_id, serialnumber, firmwareversion, hardwareversion)

    def develop_instrument_device(self, instrument_device_id='', instrument_agent_id=''):
        """
        Assign an instrument agent (just the type, not the instance) to an instrument
        """
        return self.instrument_device.develop(instrument_device_id, instrument_agent_id)

    def commission_instrument_device(self, instrument_device_id='', platform_device_id=''):
        """
        method docstring
        """
        return self.instrument_device.commission(instrument_device_id, platform_device_id)

    def decommission_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_device.decommission(instrument_device_id)

    def activate_instrument_device(self, instrument_device_id='', instrument_agent_instance_id=''):
        """
        method docstring
        """
        return self.instrument_device.activate(instrument_device_id, instrument_agent_instance_id)

    def deactivate_instrument_device(self, instrument_device_id=''):
        """
        method docstring
        """
        return self.instrument_device.deactivate(instrument_device_id)        

    def retire_instrument_device(self, instrument_device_id=''):
        """
        Retire an instrument
        """
        return self.instrument_device.retire(instrument_device_id)
        
        return self._return_update(True)




    def assign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass

    def unassign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass




    
    ##########################################################################
    #
    # LOGICAL INSTRUMENT
    #
    ##########################################################################

    def create_logical_instrument(self, logical_instrument={}):
        """
        method docstring
        """
        return self.logical_instrument.create_one(logical_instrument)

    def update_logical_instrument(self, logical_instrument={}):
        """
        method docstring
        """
        return self.logical_instrument.update_one(logical_instrument)
        

    def read_logical_instrument(self, logical_instrument_id=''):
        """
        method docstring
        """
        return self.logical_instrument.read_one(logical_instrument_id)

    def delete_logical_instrument(self, logical_instrument_id=''):
        """
        method docstring
        """
        return self.logical_instrument.delete_one(logical_instrument_id)

    def find_logical_instruments(self, filters={}):
        """
        method docstring
        """
        return self.logical_instrument.find_some(filters)




    ##
    ##
    ##  DIRECT ACCESS
    ##
    ##

    def request_direct_access(self, instrument_device_id=''):
        """
        method docstring
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
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        raise NotImplementedError()
        pass










    
    ##########################################################################
    #
    # PLATFORM AGENT
    #
    ##########################################################################


    def create_platform_agent(self, platform_agent={}):
        """
        method docstring
        """
        return self.platform_agent.create_one(platform_agent)

    def update_platform_agent(self, platform_agent={}):
        """
        method docstring
        """
        return self.platform_agent.update_one(platform_agent)
        

    def read_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """
        return self.platform_agent.read_one(platform_agent_id)

    def delete_platform_agent(self, platform_agent_id=''):
        """
        method docstring
        """
        return self.platform_agent.delete_one(platform_agent_id)

    def find_platform_agents(self, filters={}):
        """
        method docstring
        """
        return self.platform_agent.find_some(filters)

    #FIXME: args need to change
    def assign_platform_agent(self, platform_agent_id='', platform_device_id='', platform_agent_instance={}):
        """
        method docstring
        """
        raise NotImplementedError()
        return self.platform_agent.assign(platform_agent_id, platform_device_id, platform_agent_instance)

    #FIXME: args need to change
    def unassign_platform_agent(self, platform_agent_id='', platform_device_id='', platform_agent_instance={}):

        """
        method docstring
        """
        raise NotImplementedError()
        return self.platform_agent.unassign(platform_agent_id, platform_device_id, platform_agent_instance)



    
    ##########################################################################
    #
    # PLATFORM MODEL
    #
    ##########################################################################


    def create_platform_model(self, platform_model={}):
        """
        method docstring
        """
        return self.platform_model.create_one(platform_model)

    def update_platform_model(self, platform_model={}):
        """
        method docstring
        """
        return self.platform_model.update_one(platform_model)
        

    def read_platform_model(self, platform_model_id=''):
        """
        method docstring
        """
        return self.platform_model.read_one(platform_model_id)

    def delete_platform_model(self, platform_model_id=''):
        """
        method docstring
        """
        return self.platform_model.delete_one(platform_model_id)

    def find_platform_models(self, filters={}):
        """
        method docstring
        """
        return self.platform_model.find_some(filters)

    def assign_platform_model(self, platform_model_id='', platform_device_id=''):
        """
        method docstring
        """
        return self.platform_model.assign(platform_model_id, platform_device_id)

    def unassign_platform_model(self, platform_model_id='', platform_device_id=''):
        """
        method docstring
        """
        return self.platform_model.assign(platform_model_id, platform_device_id)






    ##########################################################################
    #
    # PHYSICAL PLATFORM
    #
    ##########################################################################



    def create_platform_device(self, platform_device={}):
        """
        method docstring
        """
        return self.platform_device.create_one(platform_device)

    def update_platform_device(self, platform_device={}):
        """
        method docstring
        """
        return self.platform_device.update_one(platform_device)
        

    def read_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        return self.platform_device.read_one(platform_device_id)

    def delete_platform_device(self, platform_device_id=''):
        """
        method docstring
        """
        return self.platform_device.delete_one(platform_device_id)

    def find_platform_devices(self, filters={}):
        """
        method docstring
        """
        return self.platform_device.find_some(filters)





    
    ##########################################################################
    #
    # SENSOR MODEL
    #
    ##########################################################################


    def create_sensor_model(self, sensor_model={}):
        """
        method docstring
        """
        return self.sensor_model.create_one(sensor_model)

    def update_sensor_model(self, sensor_model={}):
        """
        method docstring
        """
        return self.sensor_model.update_one(sensor_model)
        

    def read_sensor_model(self, sensor_model_id=''):
        """
        method docstring
        """
        return self.sensor_model.read_one(sensor_model_id)

    def delete_sensor_model(self, sensor_model_id=''):
        """
        method docstring
        """
        return self.sensor_model.delete_one(sensor_model_id)

    def find_sensor_models(self, filters={}):
        """
        method docstring
        """
        return self.sensor_model.find_some(filters)

    def assign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """
        method docstring
        """
        return self.sensor_model.assign(sensor_model_id, sensor_device_id)

    def unassign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """
        method docstring
        """
        return self.sensor_model.unassign(sensor_model_id, sensor_device_id)




    ##########################################################################
    #
    # PHYSICAL SENSOR
    #
    ##########################################################################



    def create_sensor_device(self, sensor_device={}):
        """
        method docstring
        """
        return self.sensor_device.create_one(sensor_device)

    def update_sensor_device(self, sensor_device={}):
        """
        method docstring
        """
        return self.sensor_device.update_one(sensor_device)
        

    def read_sensor_device(self, sensor_device_id=''):
        """
        method docstring
        """
        return self.sensor_device.read_one(sensor_device_id)

    def delete_sensor_device(self, sensor_device_id=''):
        """
        method docstring
        """
        return self.sensor_device.delete_one(sensor_device_id)

    def find_sensor_devices(self, filters={}):
        """
        method docstring
        """
        return self.sensor_device.find_some(filters)





    


