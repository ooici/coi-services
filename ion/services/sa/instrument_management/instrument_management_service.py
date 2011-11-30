#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


#from pyon.public import Container
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log



from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):




    ##########################################################################
    #
    # Instrument
    #
    ##########################################################################

    def create_instrument_device(self, instrument_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {instrument_device_id: ''}
        #
        pass

    def update_instrument_device(self, instrument_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_device(self, instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # instrument_device: {}
        #
        pass

    def delete_instrument_device(self, instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_devices(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # instrument_device_list: []
        #
        pass

    def assign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_device(self, instrument_id='', instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def activate_instrument_device(self, instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def request_direct_access(self, instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def stop_direct_access(self, instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def request_command(self, instrument_device_id='', command={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def assign_instrument_agent(self, instrument_agent_id='', instrument_id='', instrument_agent_instance={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_agent(self, instrument_agent_id='', instrument_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_instrument_model(self, instrument_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {instrument_model_id: ''}
        #
        pass

    def update_instrument_model(self, instrument_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_instrument_model(self, instrument_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # instrument_model: {}
        #
        pass

    def delete_instrument_model(self, instrument_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_instrument_model(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # instrument_model_list: []
        #
        pass

    def assign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_instrument_model(self, instrument_model_id='', instrument_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


    ##########################################################################
    #
    # Sensor
    #
    ##########################################################################

    def create_sensor_device(self, sensor_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {sensor_device_id: ''}
        #
        pass

    def update_sensor_device(self, sensor_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_sensor_device(self, sensor_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # sensor_device: {}
        #
        pass

    def delete_sensor_device(self, sensor_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_sensor_device(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # sensor_device_list: []
        #
        pass

    def create_sensor_model(self, sensor_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {sensor_model_id: ''}
        #
        pass

    def update_sensor_model(self, sensor_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_sensor_model(self, sensor_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # sensor_model: {}
        #
        pass

    def delete_sensor_model(self, sensor_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_sensor_model(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # sensor_model_list: []
        #
        pass

    def assign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_sensor_model(self, sensor_model_id='', sensor_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass


    ##########################################################################
    #
    # Platform
    #
    ##########################################################################

    def create_platform_device(self, platform_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {platform_device_id: ''}
        #
        pass

    def update_platform_device(self, platform_device={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_platform_device(self, platform_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # platform_device: {}
        #
        pass

    def delete_platform_device(self, platform_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform_devices(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # platform_device_list: []
        #
        pass

    def assign_platform_agent(self, platform_agent_id='', platform_id='', platform_agent_instance={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_agent(self, platform_agent_id='', platform_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_platform_model(self, platform_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {platform_model_id: ''}
        #
        pass

    def update_platform_model(self, platform_model={}):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_platform_model(self, platform_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # platform_model: {}
        #
        pass

    def delete_platform_model(self, platform_model_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_platform_model(self, filters={}):
        """method docstring
        """
        # Return Value
        # ------------
        # platform_model_list: []
        #
        pass

    def assign_platform_model(self, platform_model_id='', platform_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unassign_platform_model(self, platform_model_id='', platform_device_id=''):
        """method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass





