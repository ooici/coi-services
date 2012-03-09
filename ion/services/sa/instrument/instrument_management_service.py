#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.instrument_management_service
@author   Maurice Manning
@author   Ian Katz
"""


from pyon.public import Container
from pyon.public import LCS #, LCE
from pyon.public import RT, PRED
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent,BadRequest, NotFound
#from pyon.datastore.datastore import DataStore
#from pyon.net.endpoint import RPCClient
from pyon.util.log import log

#for agent mgmt
from interface.objects import ProcessDefinition, ProcessSchedule, ProcessTarget
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG

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

# TODO: these are for methods which may belong in DAMS/DPMS/MFMS
from ion.services.sa.resource_impl.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.data_producer_impl import DataProducerImpl
from ion.services.sa.resource_impl.logical_instrument_impl import LogicalInstrumentImpl

from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG


from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService

class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access

    """
    def on_init(self):
        #suppress a few "variable declared but not used" annoying pyflakes errors
        IonObject("Resource")

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

        if hasattr(self.clients, "pubsub_management"):
            self.PSMS = self.clients.pubsub_management

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
        self.data_product        = DataProductImpl(self.clients)
        self.data_producer       = DataProducerImpl(self.clients)
        self.logical_instrument  = LogicalInstrumentImpl(self.clients)


    ##########################################################################
    #
    # INSTRUMENT AGENT INSTANCE
    #
    ##########################################################################

    def create_instrument_agent_instance(self, instrument_agent_instance=None, instrument_agent_id="", instrument_device_id=""):
        """
        create a new instance
        @param instrument_agent_instance the object to be created as a resource
        @retval instrument_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """

        instrument_agent_obj = self.read_instrument_agent(instrument_agent_id)

        instrument_device_obj = self.read_instrument_device(instrument_device_id)

        instrument_agent_instance_id = self.instrument_agent_instance.create_one(instrument_agent_instance)

        self.assign_instrument_agent_instance_to_instrument_agent(instrument_agent_instance_id, instrument_agent_id)

        self.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)

        return instrument_agent_instance_id

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

        associations, _ = self.clients.resource_registry.find_associations(RT.InstrumentAgent, PRED.hasInstance, instrument_agent_instance_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)

        associations, _ = self.clients.resource_registry.find_associations(RT.InstrumentDevice, PRED.hasAgentInstance, RT.instrument_agent_instance_id, True)
        for association in associations:
            self.clients.resource_registry.delete_association(association)

        self.instrument_agent_instance.delete_one(instrument_agent_instance_id)

        return

    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instument agent instance and return the id
        """

        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        #retrieve the associated instrument device
        inst_device_ids, _ = self.clients.resource_registry.find_subjects(RT.InstrumentDevice, PRED.hasAgentInstance, instrument_agent_instance_id, True)
        if not inst_device_ids:
            raise NotFound("No Instrument Device attached to this Instrument Agent Instance " + str(instrument_agent_instance_id))
        if len(inst_device_ids) > 1:
            raise BadRequest("Instrument Agent Instance should only have ONE Instrument Device" + str(instrument_agent_instance_id))
        instrument_device_id = inst_device_ids[0]

        #retrieve the instrument model
        model_ids = self.instrument_device.find_stemming_model(instrument_device_id)
        if not model_ids:
            raise NotFound("No Instrument Model  attached to this Instrument Device " + str(instrument_device_id))

        instrument_model_id = model_ids[0]
        log.debug("activate_instrument:instrument_model %s"  +  str(instrument_model_id))


        #retrieve the associated instrument agent
        agent_ids = self.instrument_agent.find_having_model(instrument_model_id)
        if not agent_ids:
            raise NotFound("No Instrument Agent  attached to this Instrument Model " + str(instrument_model_id))

        instrument_agent_id = agent_ids[0]
        log.debug("Getting instrument agent '%s'" % instrument_agent_id)

        # retrieve the instrument agent information
        instrument_agent_obj = self.clients.resource_registry.read(instrument_agent_id)

        #retrieve the associated process definition
        process_def_ids, _ = self.clients.resource_registry.find_objects(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        if not process_def_ids:
            raise NotFound("No Process Definition  attached to this Instrument Agent " + str(instrument_agent_id))
        if len(process_def_ids) > 1:
            raise BadRequest("Instrument Agent should only have ONE Process Definition" + str(instrument_agent_id))

        process_definition_id = process_def_ids[0]
        log.debug("activate_instrument: agent process definition %s"  +  str(process_definition_id))

        # retrieve the process definition information
        process_def_obj = self.clients.resource_registry.read(process_definition_id)
        if not process_def_obj:
            raise NotFound("ProcessDefinition %s does not exist" % process_definition_id)


        out_streams = {}
        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(instrument_device_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if not data_product_ids:
            raise NotFound("No output Data Products attached to this Instrument Device " + str(instrument_device_id))

        for product_id in data_product_ids:
            stream_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasStream, RT.Stream, True)

            log.debug("activate_instrument:output stream ids: %s"  +  str(stream_ids))
            #One stream per product ...for now.
            if not stream_ids:
                raise NotFound("No Stream  attached to this Data Product " + str(product_id))
            if len(stream_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Stream" + str(product_id))

            # retrieve the stream
            stream_obj = self.clients.resource_registry.read(stream_ids[0])
            if not stream_obj:
                raise NotFound("Stream %s does not exist" % stream_ids[0])

            log.debug("activate_instrument:output stream name: %s"  +  str(stream_obj.name))
            out_streams[stream_obj.name] = stream_ids[0]


        # todo: this is hardcoded to the SBE37 model; need to abstract the driver configuration when more instruments are coded
        # todo: how to tell which prod is raw and which is parsed? Check the name?
        stream_config = {"ctd_raw":out_streams["ctd_raw"], "ctd_parsed":out_streams["ctd_parsed"]}
        # Driver configuration.
        driver_config = {
            'svr_addr': instrument_agent_instance_obj.svr_addr,
            'cmd_port':instrument_agent_instance_obj.cmd_port,
            'evt_port':instrument_agent_instance_obj.evt_port,
            'dvr_mod': instrument_agent_instance_obj.driver_module,
            'dvr_cls': instrument_agent_instance_obj.driver_class,
            'comms_config': {
                SBE37Channel.CTD: {
                    'method':instrument_agent_instance_obj.comms_method,
                    'device_addr': instrument_agent_instance_obj.comms_device_address,
                    'device_port': instrument_agent_instance_obj.comms_device_port,
                    'server_addr': instrument_agent_instance_obj.comms_server_address,
                    'server_port': instrument_agent_instance_obj.comms_server_port
                    }
                }
            }

        # Create agent config.
        agent_config = {
            'driver_config' : driver_config,
            'stream_config' : stream_config,
            'resource_id': instrument_device_id   #id of instrument or platform device
        }
        log.debug("activate_instrument: agent_config %s ", str(agent_config))

        pid = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                               schedule=None,
                                                               configuration=agent_config)
        log.debug("activate_instrument: schedule_process %s", pid)


        # add the process id and update the resource
        instrument_agent_instance_obj.agent_process_id = pid
        self.update_instrument_agent_instance(instrument_agent_instance_obj)

        return


    def stop_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Deactivate the instrument agent instance
        """
        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(instrument_agent_instance_obj.agent_process_id)

        return


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
        instrument_agent_id = self.instrument_agent.create_one(instrument_agent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.services.mi.instrument_agent'
        process_definition.executable['class'] = 'InstrumentAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)
        log.debug("create_instrument_agent: create_process_definition id %s"  +  str(process_definition_id))

        #associate the agent and the process def
        self.clients.resource_registry.create_association(instrument_agent_id,  PRED.hasProcessDefinition, process_definition_id)

        return instrument_agent_id

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
        #retrieve the associated process definition
        process_def_ids, _ = self.clients.resource_registry.find_objects(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        if not process_def_ids:
            raise NotFound("No Process Definition  attached to this Instrument Agent " + str(instrument_agent_id))
        if len(process_def_ids) > 1:
            raise BadRequest("Instrument Agent should only have ONE Process Definition" + str(instrument_agent_id))

        assoc_ids, _ = self.clients.resource_registry.find_associations(instrument_agent_id, PRED.hasProcessDefinition, RT.ProcessDefinition, True)
        self.clients.resource_registry.delete_association(assoc_ids[0])

        self.clients.process_dispatcher.delete_process_definition(process_def_ids[0])

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
        create data products for all the streams defined by this instrument's model/agent
        """

        #get instrument object and instrument's data producer
        inst_obj = self.instrument_device.read_one(instrument_device_id)

        models = self.find_instrument_model_by_instrument_device(instrument_device_id)

        if 1 != len(models):
            raise Inconsistent("The instrument device had %d associated models, expected 1" % len(models))

        #todo: stream association should be with agent, not model... but that's for later
        #
        #agents = self.find_instrument_agent_by_instrument_model(models[0])
        #
        #if 1 != len(agents):
        #    raise Inconsistent("The instrument model had %d associated agents, expected 1" % len(agents))
        #stream_definition_ids = self.find_stream_definition_by_instrument_agent(agents[0])

        stream_definition_ids = self.find_stream_definition_by_instrument_model(models[0])

        for stream_definition_id in stream_definition_ids:
            log.debug("Getting name of stream with id='%s'" % stream_definition_id)

            streamdef_obj = self.PSMS.read_stream_definition(stream_definition_id)
            names = (streamdef_obj.name, inst_obj.name)
            log.debug("Creating data product named '%s for %s'" % names)
            data_product_obj = IonObject(RT.DataProduct,
                                       name=str("%s for %s" % names),
                                       description=str("Product for '%s' instrument's '%s' stream" % names))

            #create data product with specified stream definition
            data_product_id = self.DPMS.create_data_product(data_product_obj, stream_definition_id)

            #associate this data product with the instrument
            log.debug("Associating data product's producers with instrument via DAMS")
            self.DAMS.assign_data_product(instrument_device_id, data_product_id)


    def create_instrument_device(self, instrument_device=None):
        """
        create a new instance
        @param instrument_device the object to be created as a resource
        @retval instrument_device_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        instrument_device_id = self.instrument_device.create_one(instrument_device)

        #register the instrument as a data producer
        self.DAMS.register_instrument(instrument_device_id)

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


    def deploy_instrument_device_to_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        self.instrument_device.link_deployment(instrument_device_id, logical_instrument_id)

    def undeploy_instrument_device_from_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        self.instrument_device.unlink_deployment(instrument_device_id, logical_instrument_id)

    def deploy_platform_device_to_logical_platform(self, platform_device_id='', logical_platform_id=''):
        self.platform_device.link_deployment(platform_device_id, logical_platform_id)

    def undeploy_platform_device_from_logical_platform(self, platform_device_id='', logical_platform_id=''):
        self.platform_device.unlink_deployment(platform_device_id, logical_platform_id)


    def deploy_as_primary_instrument_device_to_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        self.instrument_device.link_primary_deployment(instrument_device_id, logical_instrument_id)

    def undeploy_primary_instrument_device_from_logical_instrument(self, instrument_device_id='', logical_instrument_id=''):
        self.instrument_device.unlink_primary_deployment(instrument_device_id, logical_instrument_id)

    def deploy_as_primary_platform_device_to_logical_platform(self, platform_device_id='', logical_platform_id=''):
        self.platform_device.link_primary_deployment(platform_device_id, logical_platform_id)

    def undeploy_primary_platform_device_from_logical_platform(self, platform_device_id='', logical_platform_id=''):
        self.platform_device.unlink_primary_deployment(platform_device_id, logical_platform_id)




    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        #todo: for when instrument agents are the owners of stream def associations
        #agents = self.find_instrument_agent_by_instrument_model(instrument_model_id)
        #if 0 == len(agents):
        #    raise BadRequest("Tried to assign a model to an instrument, but the model didn't have an agent")

        self.instrument_device.link_model(instrument_device_id, instrument_model_id)
        self.setup_data_production_chain(instrument_device_id)

    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        raise Inconsistent("Unassigning an instrument model from an instrument device (cleanly) has not been defined")
        #todo
        #self.retire_data_production_chain(instrument_device_id)
        self.instrument_device.unlink_model(instrument_device_id, instrument_model_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.link_model(instrument_agent_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_model(instrument_agent_id, instrument_model_id)

    def assign_stream_definition_to_instrument_model(self, stream_definition_id='', instrument_model_id=''):
        self.instrument_model.link_stream_definition(instrument_model_id, stream_definition_id)

    def unassign_stream_definition_from_instrument_model(self, stream_definition_id='', instrument_model_id=''):
        self.instrument_model.unlink_stream_definition(instrument_model_id, stream_definition_id)

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

    def assign_platform_agent_instance_to_platform_agent(self, platform_agent_instance_id='', platform_agent_id=''):
        self.platform_agent.link_instance(platform_agent_id, platform_agent_instance_id)

    def unassign_platform_agent_instance_from_platform_agent(self, platform_agent_instance_id='', platform_agent_id=''):
        self.platform_agent.unlink_instance(platform_agent_id, platform_agent_instance_id)

    def assign_instrument_agent_instance_to_instrument_agent(self, instrument_agent_instance_id='', instrument_agent_id=''):
        self.instrument_agent.link_instance(instrument_agent_id, instrument_agent_instance_id)

    def unassign_instrument_agent_instance_from_instrument_agent(self, instrument_agent_instance_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_instance(instrument_agent_id, instrument_agent_instance_id)

    def assign_instrument_agent_instance_to_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_agent.link_device_instance(instrument_device_id, instrument_agent_instance_id)

    def unassign_instrument_agent_instance_from_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_agent.unlink_device_instance(instrument_device_id, instrument_agent_instance_id)

        
        def verify_dp_origin(supplied_dps, assigned_dps, instrument_id, instrument_label):
            """
            check that the supplied dps (data products) are in the set of what's actually assigned
            @param supplied_dps list of data product ids
            @param assigned_dps list of data product ids
            @param instrument_id a logical or instrument device id
            """
            badones = []
            for p in supplied_dps:
                if not p in assigned_dps:
                    badones.append(p)
                    if 0 < len(badones):
                        raise BadRequest("want to assign %s's data products, but the following were supplied " +
                                         "that don't seem to come from %s '%s': [%s]" %
                                         (instrument_label, instrument_label, instrument_id, ", ".join(badones)))


        log.info("Checking consistency of existing logical/instrument assignments")
        existing_assignments = self.instrument_device.find_having_assignment(logical_instrument_id)
        if 1 < len(existing_assignments):
            raise Inconsistent("There is more than 1 instrument device associated with logical instrument '%s'" %
                               logical_instrument_id)

        log.info("Checking whether supplied logical/instrument arguments are proper")
        if 0 < len(existing_assignments):
            if not old_instrument_device_id:
                raise BadRequest(("Tried to assign logical instrument '%s' for the first time, but it is already " + 
                                  "assigned to instrument device '%s'") % (logical_instrument_id, existing_assignments[0]))
            elif old_instrument_device_id != existing_assignments[0]:
                raise BadRequest(("Tried to reassign logical instrument '%s' from instrument device '%s' but it is " +
                                  "actually associated to instrument device '%s'") % 
                                 (logical_instrument_id, old_instrument_device_id, existing_assignments[0]))


        # log.info("Checking whether supplied data products are proper")
        #  existing_logical_data_products = self.logical_instrument.find_stemming_data_product(logical_instrument_id)
        #
        #TODO: need a check that all the logical data products are being provided for
        #
        # log.info("Checking whether all logical data products are provided")
        # if len(logical_data_product_ids) != len(existing_logical_data_products):
        #     raise BadRequest("tried to assign logical instrument but only provided %d of %d " +
        #                      "data products" % (len(logical_data_product_ids), len(existing_logical_data_products)))
        #
        # log.info("Checking that supplied logical data products are properly rooted")
        # verify_dp_origin(logical_data_product_ids,
        #                  existing_logical_data_products,
        #                  logical_instrument_id,
        #                  "logical_instrument")


        
        if old_instrument_device_id:
            log.info("Checking that the data product to be dissociated are properly rooted")
            verify_dp_origin(old_instrument_data_product_ids,
                             self.find_data_product_by_instrument_device(old_instrument_device_id),
                             old_instrument_device_id,
                             "instrument_device")

            log.info("Checking that all data products to be dissociated have been supplied")
            if len(logical_data_product_ids) != len(old_instrument_data_product_ids):
                raise BadRequest("Can't unmap %d instrument data products from %d logical products" %
                                 (len(old_instrument_data_product_ids), len(logical_data_product_ids)))


        log.info("Checking that supplied instrument data products are properly rooted")
        verify_dp_origin(new_instrument_data_product_ids,
                         self.find_data_product_by_instrument_device(new_instrument_device_id),
                         new_instrument_device_id,
                         "instrument_device")

        log.info("Checking that all data products to be associated have been supplied")
        if len(logical_data_product_ids) != len(new_instrument_data_product_ids):
            raise BadRequest("Can't map %d instrument data products to %d logical products" %
                             (len(new_instrument_data_product_ids), len(logical_data_product_ids)))

        log.info("Assigning the instruments themselves")
        if "" != old_instrument_device_id:
            self.instrument_device.unlink_assignment(old_instrument_device_id, logical_instrument_id)
        self.instrument_device.link_assignment(new_instrument_device_id, logical_instrument_id)


        # functions to link and unlink data products as appropriate

        def link_logical_dp_to_instrument_dp(logical_dp_id, inst_dp_id):
            # TODO: this should be a function call, probably to DPMS,
            #       which sets up inst_dp to copy its data stream
            #       directly into the logical_dp
            pass

        def unlink_logical_dp_from_instrument_dp(logical_dp_id, inst_dp_id):
            #TODO: undo the above
            pass


        if old_instrument_device_id:
            log.info("Unlinking existing instrument data product(s) from logical instrument's product(s)")
            map(unlink_logical_dp_from_instrument_dp, logical_data_product_ids, old_instrument_data_product_ids)

        log.info("Linking new instrument data products with logical instrument's product(s)")
        map(link_logical_dp_to_instrument_dp, logical_data_product_ids, new_instrument_data_product_ids)





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

    def find_stream_definition_by_instrument_model(self, instrument_model_id=''):
        return self.instrument_model.find_stemming_stream_definition(instrument_model_id)

    def find_instrument_model_by_stream_definition(self, stream_definition_id=''):
        return self.instrument_model.find_having_stream_definition(stream_definition_id)

    def find_instrument_device_by_instrument_agent_instance(self, instrument_agent_instance_id=''):
        return self.instrument_device.find_having_agent_instance(instrument_agent_instance_id)

    def find_instrument_agent_instance_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_agent_instance(instrument_device_id)

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


    def find_data_product_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_output_product(instrument_device_id)

    def find_instrument_device_by_data_product(self, data_product_id=''):
        return self.instrument_device.find_having_output_product(data_product_id)


    ############################
    #
    #  SPECIALIZED FIND METHODS
    #
    ############################

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

