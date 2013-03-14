#!/usr/bin/env python

from ion.util.agent_launcher import AgentLauncher
from ion.services.sa.instrument.agent_configuration_builder import InstrumentAgentConfigurationBuilder, \
    PlatformAgentConfigurationBuilder
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.util.resource_lcs_policy import AgentPolicy, ResourceLCSPolicy, ModelPolicy, DevicePolicy

__author__ = 'Maurice Manning, Ian Katz, Michael Meisinger'


import os
import pwd
import json
from datetime import datetime, timedelta
import time

from ooi.logging import log

from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent,BadRequest, NotFound
from pyon.ion.resource import ExtendedResourceContainer
from pyon.util.ion_time import IonTime
from pyon.public import LCE
from pyon.public import RT, PRED, OT
from pyon.util.containers import get_ion_ts
from pyon.agent.agent import ResourceAgentState
from coverage_model.parameter import ParameterDictionary

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from ion.services.sa.instrument.flag import KeywordFlag

from ion.services.sa.observatory.observatory_util import ObservatoryUtil

from ion.util.module_uploader import RegisterModulePreparerEgg
from ion.util.qa_doc_parser import QADocParser

from ion.agents.port.port_agent_process import PortAgentProcess

from interface.objects import AttachmentType, ComputedValueAvailability, ComputedIntValue, StatusType, ProcessDefinition
from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE
from pyon.core.governance import ORG_MANAGER_ROLE, GovernanceHeaderValues, has_org_role, is_system_actor, has_exclusive_resource_commitment
from pyon.core.governance import has_shared_resource_commitment, is_resource_owner


class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access
    """
    def on_init(self):
        #suppress a few "variable declared but not used" annoying pyflakes errors
        IonObject("Resource")

        self.override_clients(self.clients)
        self.outil = ObservatoryUtil(self)

        self.extended_resource_handler = ExtendedResourceContainer(self)

        self.init_module_uploader()


        # set up all of the policy interceptions
        if self.container and self.container.governance_controller:
            reg_precondition = self.container.governance_controller.register_process_operation_precondition

            no_policy     = ResourceLCSPolicy(self.clients)
            agent_policy  = AgentPolicy(self.clients)
            model_policy  = ModelPolicy(self.clients)
            device_policy = DevicePolicy(self.clients)

            #LCS
            reg_precondition(self, 'execute_instrument_agent_lifecycle',
                             agent_policy.policy_fn_lcs_precondition("instrument_agent_id"))
            reg_precondition(self, 'execute_instrument_agent_instance_lifecycle',
                             no_policy.policy_fn_lcs_precondition("instrument_agent_instance_id"))
            reg_precondition(self, 'execute_instrument_model_lifecycle',
                             model_policy.policy_fn_lcs_precondition("instrument_model_id"))
            reg_precondition(self, 'execute_instrument_device_lifecycle',
                             device_policy.policy_fn_lcs_precondition("instrument_device_id"))
            reg_precondition(self, 'execute_platform_agent_lifecycle',
                             agent_policy.policy_fn_lcs_precondition("platform_agent_id"))
            reg_precondition(self, 'execute_platform_agent_instance_lifecycle',
                             no_policy.policy_fn_lcs_precondition("platform_agent_instance_id"))
            reg_precondition(self, 'execute_platform_model_lifecycle',
                             model_policy.policy_fn_lcs_precondition("platform_model_id"))
            reg_precondition(self, 'execute_platform_device_lifecycle',
                             device_policy.policy_fn_lcs_precondition("platform_device_id"))
            reg_precondition(self, 'execute_sensor_model_lifecycle',
                             model_policy.policy_fn_lcs_precondition("sensor_model_id"))
            reg_precondition(self, 'execute_sensor_device_lifecycle',
                             device_policy.policy_fn_lcs_precondition("sensor_device_id"))

            #Delete
            reg_precondition(self, 'force_delete_instrument_agent',
                             agent_policy.policy_fn_delete_precondition("instrument_agent_id"))
            reg_precondition(self, 'force_delete_instrument_agent_instance',
                             no_policy.policy_fn_delete_precondition("instrument_agent_instance_id"))
            reg_precondition(self, 'force_delete_instrument_model',
                             model_policy.policy_fn_delete_precondition("instrument_model_id"))
            reg_precondition(self, 'force_delete_instrument_device',
                             device_policy.policy_fn_delete_precondition("instrument_device_id"))
            reg_precondition(self, 'force_delete_platform_agent',
                             agent_policy.policy_fn_delete_precondition("platform_agent_id"))
            reg_precondition(self, 'force_delete_platform_agent_instance',
                             no_policy.policy_fn_delete_precondition("platform_agent_instance_id"))
            reg_precondition(self, 'force_delete_platform_model',
                             model_policy.policy_fn_delete_precondition("platform_model_id"))
            reg_precondition(self, 'force_delete_platform_device',
                             device_policy.policy_fn_delete_precondition("platform_device_id"))
            reg_precondition(self, 'force_delete_sensor_model',
                             model_policy.policy_fn_delete_precondition("sensor_model_id"))
            reg_precondition(self, 'force_delete_sensor_device',
                             device_policy.policy_fn_delete_precondition("sensor_device_id"))

    def init_module_uploader(self):
        if self.CFG:
            # looking for forms like host=amoeba.ucsd.edu, remotepath=/var/www/release, user=steve
            cfg_host        = self.CFG.get_safe("service.instrument_management.driver_release_host", None)
            cfg_remotepath  = self.CFG.get_safe("service.instrument_management.driver_release_directory", None)
            cfg_user        = self.CFG.get_safe("service.instrument_management.driver_release_user",
                                                pwd.getpwuid(os.getuid())[0])
            cfg_wwwprefix   = self.CFG.get_safe("service.instrument_management.driver_release_wwwprefix", None)

            if cfg_host is None or cfg_remotepath is None or cfg_wwwprefix is None:
                raise BadRequest("Missing configuration items; host='%s', directory='%s', wwwprefix='%s'" %
                                 (cfg_host, cfg_remotepath, cfg_wwwprefix))


            self.module_uploader = RegisterModulePreparerEgg(dest_user=cfg_user,
                                                             dest_host=cfg_host,
                                                             dest_path=cfg_remotepath,
                                                             dest_wwwprefix=cfg_wwwprefix)

    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        self.RR2   = EnhancedResourceRegistryClient(new_clients.resource_registry)

        #shortcut names for the import sub-services
        # we hide these behind checks even though we expect them so that
        # the resource_impl_metatests will work
        if hasattr(new_clients, "resource_registry"):
            self.RR    = new_clients.resource_registry

        if hasattr(new_clients, "data_acquisition_management"):
            self.DAMS  = new_clients.data_acquisition_management

        if hasattr(new_clients, "data_product_management"):
            self.DPMS  = new_clients.data_product_management

        if hasattr(new_clients, "pubsub_management"):
            self.PSMS = new_clients.pubsub_management

        if hasattr(new_clients, "data_retriever"):
            self.DRS = new_clients.data_retriever


    def restore_resource_state(self, instrument_device_id='', attachment_id=''):
        """
        restore a snapshot of an instrument agent instance config
        """

        instrument_device_obj = self.RR.read(instrument_device_id)
        resource_type = type(instrument_device_obj).__name__
        if not RT.InstrumentDevice == resource_type:
            raise BadRequest("Can only restore resource states for %s resources, got %s" %
                             (RT.InstrumentDevice, resource_type))


        instrument_agent_instance_obj = self.RR2.find_instrument_agent_instance_of_instrument_device(instrument_device_id)

        attachment = self.RR2.read_attachment(attachment_id, include_content=True)

        if not KeywordFlag.CONFIG_SNAPSHOT in attachment.keywords:
            raise BadRequest("Attachment '%s' does not seem to be a config snapshot" % attachment_id)

        if not 'application/json' == attachment.content_type:
            raise BadRequest("Attachment '%s' is not labeled as json")

        snapshot = json.loads(attachment.content)
        driver_config = snapshot["driver_config"]

        instrument_agent_instance_obj.driver_config["comms_config"] = driver_config["comms_config"]
        instrument_agent_instance_obj.driver_config["pagent_pid"]   = driver_config["pagent_pid"]

        self.RR2.update(instrument_agent_instance_obj)

        #todo
        #agent.set_config(snapshot["running_config"])

        #todo
        # re-launch agent?



    def save_resource_state(self, instrument_device_id='', name=''):
        """
        take a snapshot of the current instrument agent instance config for this instrument,
          and save it as an attachment
        """
        config_builder = InstrumentAgentConfigurationBuilder(self.clients)

        instrument_device_obj = self.RR.read(instrument_device_id)

        resource_type = type(instrument_device_obj).__name__
        if not RT.InstrumentDevice == resource_type:
            raise BadRequest("Can only save resource states for %s resources, got %s" %
                             (RT.InstrumentDevice, resource_type))

        inst_agent_instance_obj = self.RR2.find_instrument_agent_instance_of_instrument_device(instrument_device_id)
        config_builder.set_agent_instance_object(inst_agent_instance_obj)
        agent_config = config_builder.prepare(will_launch=False)

        epoch = time.mktime(datetime.now().timetuple())
        snapshot_name = name or "Running Config Snapshot %s.js" % epoch


        snapshot = {}
        snapshot["driver_config"] = agent_config['driver_config']
        snapshot["agent_config"]  = agent_config

        #todo
        # Start a resource agent client to talk with the instrument agent.
#        self._ia_client = ResourceAgentClient(instrument_device_id,
#                                              to_name=inst_agent_instance_obj.agent_process_id,
#                                              process=FakeProcess())
        snapshot["running_config"] = {} #agent.get_config()


        #make an attachment for the snapshot
        attachment = IonObject(RT.Attachment,
                               name=snapshot_name,
                               description="Config snapshot at time %s" % epoch,
                               content=json.dumps(snapshot),
                               content_type="application/json", # RFC 4627
                               keywords=[KeywordFlag.CONFIG_SNAPSHOT],
                               attachment_type=AttachmentType.ASCII)

        # return the attachment id
        return self.RR2.create_attachment(instrument_device_id, attachment)



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

        instrument_agent_instance_id = self.RR2.create(instrument_agent_instance, RT.InstrumentAgentInstance)

        if instrument_agent_id:
            self.assign_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)

        if instrument_device_id:
            self.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)
        log.debug("device %s now connected to instrument agent instance %s (L4-CI-SA-RQ-363)",
                  str(instrument_device_id),  str(instrument_agent_instance_id))

        return instrument_agent_instance_id


    def update_instrument_agent_instance(self, instrument_agent_instance=None):
        """
        update an existing instance
        @param instrument_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.RR2.update(instrument_agent_instance, RT.InstrumentAgentInstance)

    def read_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_instance_id the id of the object to be fetched
        @retval InstrumentAgentInstance resource
        """
        return self.RR2.read(instrument_agent_instance_id, RT.InstrumentAgentInstance)

    def delete_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """

        self.RR2.retire(instrument_agent_instance_id, RT.InstrumentAgentInstance)


    def force_delete_instrument_agent_instance(self, instrument_agent_instance_id=''):

        self.RR2.pluck_delete(instrument_agent_instance_id, RT.InstrumentAgentInstance)




    def record_instrument_producer_activation(self, instrument_device_id, instrument_agent_instance_id):

        log.debug("update the producer context for provenance")
        #todo: should get the time from process dispatcher
        producer_obj = self._get_instrument_producer(instrument_device_id)
        if OT.InstrumentProducerContext == producer_obj.producer_context.type_:

            # reload resource as it has been updated by the launch function
            instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)

            producer_obj.producer_context.activation_time =  IonTime().to_string()
            producer_obj.producer_context.configuration = instrument_agent_instance_obj.agent_config

            # get the site where this device is currently deploy instrument_device_id
            try:
                site_id = self.RR2.find_instrument_site_id_by_instrument_device(instrument_device_id)
                producer_obj.producer_context.deployed_site_id = site_id
            except NotFound:
                pass
            except:
                raise
            self.RR2.update(producer_obj)


    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instument agent instance and return the id
        """

        instrument_agent_instance_obj = self.read_instrument_agent_instance(instrument_agent_instance_id)

        # launch the port agent before verifying anything.
        # if agent instance doesn't validate, port agent won't care and will be available for when it does validate

        # if no comms_config specified in the driver config then we need to start a port agent
        if not 'comms_config' in instrument_agent_instance_obj.driver_config:
            log.info("IMS:start_instrument_agent_instance no comms_config specified in the driver_config so call _start_port_agent")
            instrument_agent_instance_obj = self._start_port_agent(instrument_agent_instance_obj) # <-- this updates agent instance obj!
        # if the comms_config host addr in the driver config is localhost
        elif 'addr' in instrument_agent_instance_obj.driver_config.get('comms_config') and\
             instrument_agent_instance_obj.driver_config['comms_config']['addr'] == 'localhost':
            log.info("IMS:start_instrument_agent_instance  comms_config host addr in the driver_config is localhost so call _start_port_agent")
            instrument_agent_instance_obj = self._start_port_agent(instrument_agent_instance_obj) # <-- this updates agent instance obj!

        config_builder = InstrumentAgentConfigurationBuilder(self.clients)
        launcher = AgentLauncher(self.clients.process_dispatcher)
        try:
            config_builder.set_agent_instance_object(instrument_agent_instance_obj)
            config = config_builder.prepare()
        except:
            self._stop_port_agent(instrument_agent_instance_obj.port_agent_config)
            raise

        process_id = launcher.launch(config, config_builder._get_process_definition()._id)
        config_builder.record_launch_parameters(config, process_id)

        self.record_instrument_producer_activation(config_builder._get_device()._id, instrument_agent_instance_id)

        launcher.await_launch(20)

        return process_id




    def _start_port_agent(self, instrument_agent_instance_obj=None):
        """
        Construct and start the port agent, ONLY NEEDED FOR INSTRUMENT AGENTS.
        """

        _port_agent_config = instrument_agent_instance_obj.port_agent_config

        #todo: ask bill if this blocks
        # It blocks until the port agent starts up or a timeout
        log.info("IMS:_start_pagent calling PortAgentProcess.launch_process ")
        _pagent = PortAgentProcess.launch_process(_port_agent_config,  test_mode = True)
        pid = _pagent.get_pid()
        port = _pagent.get_data_port()
        cmd_port = _pagent.get_command_port()
        log.info("IMS:_start_pagent returned from PortAgentProcess.launch_process pid: %s ", pid)

        # Hack to get ready for DEMO.  Further though needs to be put int
        # how we pass this config info around.
        host = 'localhost'

        driver_config = instrument_agent_instance_obj.driver_config
        comms_config = driver_config.get('comms_config')
        if comms_config:
            host = comms_config.get('addr')
        else:
            log.warn("No comms_config specified, using '%s'" % host)

        # Configure driver to use port agent port number.
        instrument_agent_instance_obj.driver_config['comms_config'] = {
            'addr' : host,
            'cmd_port' : cmd_port,
            'port' : port
        }
        instrument_agent_instance_obj.driver_config['pagent_pid'] = pid
        self.update_instrument_agent_instance(instrument_agent_instance_obj)
        return self.read_instrument_agent_instance(instrument_agent_instance_obj._id)


    def _stop_port_agent(self, port_agent_config):
        log.debug("Stopping port agent")
        try:
            _port_agent_config = port_agent_config

            process = PortAgentProcess.get_process(_port_agent_config, test_mode=True)
            process.stop()
        except NotFound:
            log.debug("No port agent process found")
            pass
        except Exception as e:
            raise e
        else:
            log.debug("Success stopping port agent")


    def stop_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Deactivate the instrument agent instance
        """
        instance_obj, device_id = self.stop_agent_instance(instrument_agent_instance_id, RT.InstrumentDevice)

        self._stop_port_agent(instance_obj.port_agent_config)

        #update the producer context for provenance
        producer_obj = self._get_instrument_producer(device_id)
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.deactivation_time =  IonTime().to_string()
            self.RR2.update(producer_obj)



    def stop_agent_instance(self, agent_instance_id, device_type):
        """
        Deactivate an agent instance, return device ID
        """
        agent_instance_obj = self.RR2.read(agent_instance_id)

        device_id = self.RR2.find_subject(subject_type=device_type,
                                          predicate=PRED.hasAgentInstance,
                                          object=agent_instance_id,
                                          id_only=True)


        log.debug("Canceling the execution of agent's process ID")
        if None is agent_instance_obj.agent_process_id:
            raise BadRequest("Agent Instance '%s' does not have an agent_process_id.  Stopped already?"
            % agent_instance_id)
        try:
            self.clients.process_dispatcher.cancel_process(process_id=agent_instance_obj.agent_process_id)
        except NotFound:
            log.debug("No agent process found")
            pass
        except Exception as e:
            raise e
        else:
            log.debug("Success cancelling agent process")




        #reset the process ids.
        agent_instance_obj.agent_process_id = None
        if "pagent_pid" in agent_instance_obj.driver_config:
            agent_instance_obj.driver_config['pagent_pid'] = None
        self.RR2.update(agent_instance_obj)

        return agent_instance_obj, device_id



    def _get_instrument_producer(self, instrument_device_id=""):
        producer_objs, _ = self.clients.resource_registry.find_objects(subject=instrument_device_id,
                                                                       predicate=PRED.hasDataProducer,
                                                                       object_type=RT.DataProducer,
                                                                       id_only=False)
        if not producer_objs:
            raise NotFound("No Producers created for this Instrument Device " + str(instrument_device_id))
        return producer_objs[0]



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
        instrument_agent_id = self.RR2.create(instrument_agent, RT.InstrumentAgent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.agents.instrument.instrument_agent'
        process_definition.executable['class'] = 'InstrumentAgent'
        pd = self.clients.process_dispatcher
        process_definition_id = pd.create_process_definition(process_definition=process_definition)

        #associate the agent and the process def
        self.RR2.assign_process_definition_to_instrument_agent(process_definition_id, instrument_agent_id)

        return instrument_agent_id

    def update_instrument_agent(self, instrument_agent=None):
        """
        update an existing instance
        @param instrument_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.RR2.update(instrument_agent, RT.InstrumentAgent)

    def read_instrument_agent(self, instrument_agent_id=''):
        """
        fetch a resource by ID
        @param instrument_agent_id the id of the object to be fetched
        @retval InstrumentAgent resource
        """
        return self.RR2.read(instrument_agent_id, RT.InstrumentAgent)

    def delete_instrument_agent(self, instrument_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        #retrieve the associated process definition

        self.RR2.retire(instrument_agent_id, RT.InstrumentAgent)


    def force_delete_instrument_agent(self, instrument_agent_id=''):

        process_def_objs = self.RR2.find_process_definitions_of_instrument_agent(instrument_agent_id)

        for pd_obj in process_def_objs:
            self.RR2.unassign_process_definition_from_instrument_agent(pd_obj._id, instrument_agent_id)
            self.clients.process_dispatcher.delete_process_definition(pd_obj._id)

        self.RR2.pluck_delete(instrument_agent_id, RT.InstrumentAgent)


    def register_instrument_agent(self, instrument_agent_id='', agent_egg='', qa_documents=''):
        """
        register an instrument driver by putting it in a web-accessible location
        @instrument_agent_id the agent receiving the driver
        @agent_egg a base64-encoded egg file
        @qa_documents a base64-encoded zip file containing a MANIFEST.csv file 

        MANIFEST.csv fields:
         - filename
         - name
         - description
         - content_type
         - keywords
        """

        # retrieve the resource
        self.read_instrument_agent(instrument_agent_id)

        qa_doc_parser = QADocParser()

        #process the input files (base64-encoded qa documents)
        qa_parse_result, err  = qa_doc_parser.prepare(qa_documents)
        if not qa_parse_result:
            raise BadRequest("Processing qa_documents file failed: %s" % err)


        #process the input files (base64-encoded egg)
        uploader_obj, err = self.module_uploader.prepare(agent_egg)
        if None is uploader_obj:
            raise BadRequest("Egg failed validation: %s" % err)

        attachments, err = qa_doc_parser.convert_to_attachments()

        if None is attachments:
            raise BadRequest("QA Docs processing failed: %s" % err)

        # actually upload
        up_success, err = uploader_obj.upload()
        if not up_success:
            raise BadRequest("Upload failed: %s" % err)


        #now we can do the ION side of things

        #make an attachment for the url
        attachments.append(IonObject(RT.Attachment,
                                     name=uploader_obj.get_egg_urlfile_name(),
                                     description="url to egg",
                                     content="[InternetShortcut]\nURL=%s" % uploader_obj.get_destination_url(),
                                     content_type="text/url",
                                     keywords=[KeywordFlag.EGG_URL],
                                     attachment_type=AttachmentType.ASCII))

        #insert all attachments
        for att in attachments:
            self.RR2.create_attachment(instrument_agent_id, att)

        #updates the state of this InstAgent to integrated
        self.RR2.advance_lcs(instrument_agent_id, LCE.INTEGRATE)

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
        return self.RR2.create(instrument_model, RT.InstrumentModel)

    def update_instrument_model(self, instrument_model=None):
        """
        update an existing instance
        @param instrument_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.RR2.update(instrument_model, RT.InstrumentModel)

    def read_instrument_model(self, instrument_model_id=''):
        """
        fetch a resource by ID
        @param instrument_model_id the id of the object to be fetched
        @retval InstrumentModel resource
        """
        return self.RR2.read(instrument_model_id, RT.InstrumentModel)

    def delete_instrument_model(self, instrument_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(instrument_model_id, RT.InstrumentModel)

    def force_delete_instrument_model(self, instrument_model_id=''):
        self.RR2.pluck_delete(instrument_model_id, RT.InstrumentModel)



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
        instrument_device_id = self.RR2.create(instrument_device, RT.InstrumentDevice)

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
        return self.RR2.update(instrument_device, RT.InstrumentDevice)

    def read_instrument_device(self, instrument_device_id=''):
        """
        fetch a resource by ID
        @param instrument_device_id the id of the object to be fetched
        @retval InstrumentDevice resource

        """
        return self.RR2.read(instrument_device_id, RT.InstrumentDevice)

    def delete_instrument_device(self, instrument_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param instrument_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(instrument_device_id, RT.InstrumentDevice)


    def force_delete_instrument_device(self, instrument_device_id=''):
        self.RR2.pluck_delete(instrument_device_id, RT.InstrumentDevice)

    ##
    ##
    ##  PRECONDITION FUNCTIONS
    ##
    ##

    def check_direct_access_policy(self, msg, headers):

        try:
            gov_values = GovernanceHeaderValues(headers)
        except Inconsistent, ex:
            return False, ex.message

        #The system actor can to anything
        if is_system_actor(gov_values.actor_id):
            return True, ''

        #TODO - this shared commitment might not be with the right Org - may have to relook at how this is working.
        if not has_exclusive_resource_commitment(gov_values.actor_id, gov_values.resource_id):
            return False, '%s(%s) has been denied since the user %s has not acquired the resource exclusively' % (self.name, gov_values.op, gov_values.actor_id)

        return True, ''

    def check_device_lifecycle_policy(self, msg, headers):

        try:
            gov_values = GovernanceHeaderValues(headers)
        except Inconsistent, ex:
            return False, ex.message

        #The system actor can to anything
        if is_system_actor(gov_values.actor_id):
            return True, ''

        if msg.has_key('lifecycle_event'):
            lifecycle_event = msg['lifecycle_event']
        else:
            raise Inconsistent('%s(%s) has been denied since the lifecycle_event can not be found in the message'% (self.name, gov_values.op))

        orgs,_ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasResource, gov_values.resource_id)
        if not orgs:
            return False, '%s(%s) has been denied since the resource id %s has not been shared with any Orgs' % (self.name, gov_values.op, gov_values.resource_id)

        #Handle these lifecycle transitions first
        if lifecycle_event == LCE.INTEGRATE or lifecycle_event == LCE.DEPLOY or lifecycle_event == LCE.RETIRE:

            #Check across Orgs which have shared this device for role which as proper level to allow lifecycle transition
            for org in orgs:
                if has_org_role(gov_values.actor_roles, org.name, [OBSERVATORY_OPERATOR_ROLE,ORG_MANAGER_ROLE]):
                    return True, ''

        else:

            #The owner can do any of these other lifecycle transitions
            is_owner = is_resource_owner(gov_values.actor_id, gov_values.resource_id)
            if is_owner:
                return True, ''

            #TODO - this shared commitment might not be with the right Org - may have to relook at how this is working.
            is_shared = has_shared_resource_commitment(gov_values.actor_id, gov_values.resource_id)

            #Check across Orgs which have shared this device for role which as proper level to allow lifecycle transition
            for org in orgs:
                if has_org_role(gov_values.actor_roles, org.name, [INSTRUMENT_OPERATOR_ROLE, OBSERVATORY_OPERATOR_ROLE,ORG_MANAGER_ROLE] ) and is_shared:
                    return True, ''

        return False, '%s(%s) has been denied since the user %s has not acquired the resource or is not the proper role for this transition: %s' % (self.name, gov_values.op, gov_values.actor_id, lifecycle_event)


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

    def create_platform_agent_instance(self, platform_agent_instance=None, platform_agent_id="", platform_device_id=""):
        """
        create a new instance
        @param platform_agent_instance the object to be created as a resource
        @retval platform_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """
        platform_agent_instance_id = self.RR2.create(platform_agent_instance, RT.PlatformAgentInstance)

        if platform_agent_id:
            self.assign_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)
        if platform_device_id:
            self.assign_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)

        return platform_agent_instance_id

    def update_platform_agent_instance(self, platform_agent_instance=None):
        """
        update an existing instance
        @param platform_agent_instance the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.RR2.update(platform_agent_instance, RT.PlatformAgentInstance)

    def read_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        fetch a resource by ID
        @param platform_agent_instance_id the id of the object to be fetched
        @retval PlatformAgentInstance resource
        """
        return self.RR2.read(platform_agent_instance_id, RT.PlatformAgentInstance)

    def delete_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_instance_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(platform_agent_instance_id, RT.PlatformAgentInstance)

    def force_delete_platform_agent_instance(self, platform_agent_instance_id=''):
        self.RR2.pluck_delete(platform_agent_instance_id, RT.PlatformAgentInstance)

#    def _get_child_platforms(self, platform_device_id):
#        """ recursively trace hasDevice relationships, return list of all PlatformDevice objects
#            TODO: how to get platform ID from platform device?
#        """
#        children = [] # find by hasDevice relationship
#        out = children[:]
#        for obj in children:
#            descendents = self._get_child_platforms(obj._id)
#            out[0:] = descendents
#        return out


    def start_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a platform device
        Launch the platform agent instance and return the id
        """
        configuration_builder = PlatformAgentConfigurationBuilder(self.clients)
        launcher = AgentLauncher(self.clients.process_dispatcher)

        platform_agent_instance_obj = self.read_platform_agent_instance(platform_agent_instance_id)

        configuration_builder.set_agent_instance_object(platform_agent_instance_obj)
        config = configuration_builder.prepare()

        platform_device_obj = configuration_builder._get_device()
        log.debug("start_platform_agent_instance: device is %s connected to platform agent instance %s (L4-CI-SA-RQ-363)",
                  str(platform_device_obj._id),  str(platform_agent_instance_id))

        #retrive the stream info for this model
        #todo: add stream info to the platform model create
        #        streams_dict = platform_model_obj.custom_attributes['streams']
        #        if not streams_dict:
        #            raise BadRequest("Device model does not contain stream configuation used in launching the agent. Model: '%s", str(platform_models_objs[0]) )


        process_id = launcher.launch(config, configuration_builder._get_process_definition()._id)
        configuration_builder.record_launch_parameters(config, process_id)
        launcher.await_launch(20)

        return process_id



    def stop_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        Deactivate the platform agent instance
        """
        self.stop_agent_instance(platform_agent_instance_id, RT.PlatformDevice)




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

        platform_agent_id = self.RR2.create(platform_agent, RT.PlatformAgent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.agents.platform.platform_agent'
        process_definition.executable['class'] = 'PlatformAgent'
        pd = self.clients.process_dispatcher
        process_definition_id = pd.create_process_definition(process_definition=process_definition)

        #associate the agent and the process def
        self.RR2.assign_process_definition_to_platform_agent(process_definition_id, platform_agent_id)
        return platform_agent_id

    def update_platform_agent(self, platform_agent=None):
        """
        update an existing instance
        @param platform_agent the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.RR2.update(platform_agent, RT.PlatformAgent)

    def read_platform_agent(self, platform_agent_id=''):
        """
        fetch a resource by ID
        @param platform_agent_id the id of the object to be fetched
        @retval PlatformAgent resource

        """
        return self.RR2.read(platform_agent_id, RT.PlatformAgent)

    def delete_platform_agent(self, platform_agent_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_agent_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(platform_agent_id, RT.PlatformAgent)

    def force_delete_platform_agent(self, platform_agent_id=''):
        self.RR2.pluck_delete(platform_agent_id, RT.PlatformAgent)


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
        return self.RR2.create(platform_model, RT.PlatformModel)

    def update_platform_model(self, platform_model=None):
        """
        update an existing instance
        @param platform_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists
        """
        return self.RR2.update(platform_model, RT.PlatformModel)

    def read_platform_model(self, platform_model_id=''):
        """
        fetch a resource by ID
        @param platform_model_id the id of the object to be fetched
        @retval PlatformModel resource

        """
        return self.RR2.read(platform_model_id, RT.PlatformModel)

    def delete_platform_model(self, platform_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(platform_model_id, RT.PlatformModel)

    def force_delete_platform_model(self, platform_model_id=''):
        self.RR2.pluck_delete(platform_model_id, RT.PlatformModel)


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

        platform_device_id = self.RR2.create(platform_device, RT.PlatformDevice)
        #register the platform as a data producer
        self.DAMS.register_instrument(platform_device_id)

        return platform_device_id


    def update_platform_device(self, platform_device=None):
        """
        update an existing instance
        @param platform_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.RR2.update(platform_device, RT.PlatformDevice)

    def read_platform_device(self, platform_device_id=''):
        """
        fetch a resource by ID
        @param platform_device_id the id of the object to be fetched
        @retval PlatformDevice resource

        """
        return self.RR2.read(platform_device_id, RT.PlatformDevice)

    def delete_platform_device(self, platform_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param platform_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(platform_device_id, RT.PlatformDevice)

    def force_delete_platform_device(self, platform_device_id=''):
        self.RR2.pluck_delete(platform_device_id, RT.PlatformDevice)




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
        return self.RR2.create(sensor_model, RT.SensorModel)

    def update_sensor_model(self, sensor_model=None):
        """
        update an existing instance
        @param sensor_model the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.RR2.update(sensor_model, RT.SensorModel)

    def read_sensor_model(self, sensor_model_id=''):
        """
        fetch a resource by ID
        @param sensor_model_id the id of the object to be fetched
        @retval SensorModel resource

        """
        return self.RR2.read(sensor_model_id, RT.SensorModel)

    def delete_sensor_model(self, sensor_model_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_model_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(sensor_model_id, RT.SensorModel)

    def force_delete_sensor_model(self, sensor_model_id=''):
        self.RR2.pluck_delete(sensor_model_id, RT.SensorModel)


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
        return self.RR2.create(sensor_device, RT.SensorDevice)

    def update_sensor_device(self, sensor_device=None):
        """
        update an existing instance
        @param sensor_device the object to be created as a resource
        @retval success whether we succeeded
        @throws BadRequest if the incoming _id field is not set
        @throws BadReqeust if the incoming name already exists

        """
        return self.RR2.update(sensor_device, RT.SensorDevice)

    def read_sensor_device(self, sensor_device_id=''):
        """
        fetch a resource by ID
        @param sensor_device_id the id of the object to be fetched
        @retval SensorDevice resource

        """
        return self.RR2.read(sensor_device_id, RT.SensorDevice)

    def delete_sensor_device(self, sensor_device_id=''):
        """
        delete a resource, including its history (for less ominous deletion, use retire)
        @param sensor_device_id the id of the object to be deleted
        @retval success whether it succeeded

        """
        self.RR2.retire(sensor_device_id, RT.SensorDevice)

    def force_delete_sensor_device(self, sensor_device_id=''):
        self.RR2.pluck_delete(sensor_device_id, RT.SensorDevice)



    ##########################################################################
    #
    # ASSOCIATIONS
    #
    ##########################################################################


    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        instrument_model_obj  = self.RR2.read(instrument_model_id)
        instrument_device_obj = self.RR2.read(instrument_device_id)

        for k, v in instrument_device_obj.custom_attributes.iteritems():
            if not k in instrument_model_obj.custom_attributes:
                err_msg = ("InstrumentDevice '%s' contains custom attribute '%s' (value '%s'), but this attribute"
                        + " is not defined by associated InstrumentModel '%s'") % (instrument_device_id,
                                                                                   k, v,
                                                                                   instrument_model_id)
                #raise BadRequest(err_msg)
                log.warn(err_msg)

        self.RR2.assign_one_instrument_model_to_instrument_device(instrument_model_id, instrument_device_id)


    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.RR2.unassign_instrument_model_from_instrument_device(instrument_model_id, instrument_device_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.RR2.assign_instrument_model_to_instrument_agent(instrument_model_id, instrument_agent_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.RR2.unassign_instrument_model_from_instrument_agent(instrument_agent_id, instrument_model_id)

    def assign_platform_model_to_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.RR2.assign_platform_model_to_platform_agent(platform_model_id, platform_agent_id)

    def unassign_platform_model_from_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.RR2.unassign_platform_model_from_platform_agent(platform_model_id, platform_agent_id)

    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.RR2.assign_one_sensor_model_to_sensor_device(sensor_model_id, sensor_device_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.RR2.unassign_sensor_model_from_sensor_device(self, sensor_model_id, sensor_device_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.RR2.assign_one_platform_model_to_platform_device(platform_model_id, platform_device_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.RR2.unassign_platform_model_from_platform_device(platform_model_id, platform_device_id)

    def assign_instrument_device_to_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.RR2.assign_instrument_device_to_one_platform_device(instrument_device_id, platform_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.RR2.unassign_instrument_device_from_platform_device(instrument_device_id, platform_device_id)

    def assign_platform_device_to_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.RR2.assign_platform_device_to_one_platform_device(child_platform_device_id, platform_device_id)

    def unassign_platform_device_from_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.RR2.unassign_platform_device_from_platform_device(child_platform_device_id, platform_device_id)

    def assign_platform_agent_to_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.RR2.assign_one_platform_agent_to_platform_agent_instance(platform_agent_id, platform_agent_instance_id)

    def unassign_platform_agent_from_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.RR2.unassign_platform_agent_from_platform_agent_instance(platform_agent_id, platform_agent_instance_id)

    def assign_instrument_agent_to_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.RR2.assign_one_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)

    def unassign_instrument_agent_from_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.RR2.unassign_instrument_agent_from_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)

    def assign_instrument_agent_instance_to_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.RR2.assign_one_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)

    def unassign_instrument_agent_instance_from_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.RR2.unassign_instrument_agent_instance_from_instrument_device(instrument_agent_instance_id, instrument_device_id)

    def assign_platform_agent_instance_to_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.RR2.assign_one_platform_agent_instance_to_platform_device(platform_agent_instance_id, platform_device_id)

    def unassign_platform_agent_instance_from_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.RR2.unassign_platform_agent_instance_from_platform_device(platform_agent_instance_id, platform_device_id)

    def assign_sensor_device_to_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.RR2.assign_sensor_device_to_one_instrument_device(sensor_device_id, instrument_device_id)

    def unassign_sensor_device_from_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.RR2.unassign_sensor_device_from_instrument_device(sensor_device_id, instrument_device_id)


    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################



    def deploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        #def link_deployment(self, instrument_device_id='', deployment_id=''):
        #    # make sure that only 1 site-device-deployment triangle exists at one time
        #    sites, _ = self.RR.find_subjects(RT.InstrumentSite, PRED.hasDevice, instrument_device_id, False)
        #    if 1 < len(sites):
        #        raise Inconsistent("Device is assigned to more than one site")
        #    if 1 == len(sites):
        #        site_deployments = self._find_stemming(sites[0]._id, PRED.hasDeployment, RT.Deployment)
        #        if 1 < len(site_deployments):
        #            raise Inconsistent("Site has more than one deployment")
        #        if 1 == len(site_deployments):
        #            if site_deployments[0]._id != deployment_id:
        #                raise BadRequest("Site to which this device is assigned has a different deployment")
        #
        #        for dev in self._find_stemming(sites[0]._id, PRED.hasDevice, RT.InstrumentDevice):
        #            if 0 < len(self._find_stemming(dev, PRED.hasDeployment, RT.Deployment)):
        #                raise BadRequest("Site already has a device with a deployment")
        #
        #    return self._link_resources_single_object(instrument_device_id, PRED.hasDeployment, deployment_id)
        self.RR2.assign_deployment_to_instrument_device(deployment_id, instrument_device_id)


    def undeploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        self.RR2.unassign_deployment_from_instrument_device(deployment_id, instrument_device_id)


    def deploy_platform_device(self, platform_device_id='', deployment_id=''):
        #def link_deployment(self, platform_device_id='', deployment_id=''):
        #    # make sure that only 1 site-device-deployment triangle exists at one time
        #    sites, _ = self.RR.find_subjects(RT.PlatformSite, PRED.hasDevice, platform_device_id, False)
        #    if 1 < len(sites):
        #        raise Inconsistent("Device is assigned to more than one site")
        #    if 1 == len(sites):
        #        site_deployments = self._find_stemming(sites[0]._id, PRED.hasDeployment, RT.Deployment)
        #        if 1 < len(site_deployments):
        #            raise Inconsistent("Site has more than one deployment")
        #        if 1 == len(site_deployments):
        #            if site_deployments[0]._id != deployment_id:
        #                raise BadRequest("Site to which this device is assigned has a different deployment")
        #
        #        for dev in self._find_stemming(sites[0]._id, PRED.hasDevice, RT.PlatformDevice):
        #            if 0 < len(self._find_stemming(dev, PRED.hasDeployment, RT.Deployment)):
        #                raise BadRequest("Site already has a device with a deployment")
        #
        #    return self._link_resources_single_object(platform_device_id, PRED.hasDeployment, deployment_id)
        self.RR2.assign_deployment_to_platform_device(deployment_id, platform_device_id)


    def undeploy_platform_device(self, platform_device_id='', deployment_id=''):
        self.RR2.unassign_deployent_from_platform_device(deployment_id, platform_device_id)




    ############################
    #
    #  ASSOCIATION FIND METHODS
    #
    ############################


    def find_instrument_model_by_instrument_device(self, instrument_device_id=''):
        return self.RR2.find_instrument_models_of_instrument_device(instrument_device_id)

    def find_instrument_device_by_instrument_model(self, instrument_model_id=''):
        return self.RR2.find_instrument_devices_by_instrument_model(instrument_model_id)

    def find_platform_model_by_platform_device(self, platform_device_id=''):
        return self.RR2.find_platform_models_of_platform_device(platform_device_id)

    def find_platform_device_by_platform_model(self, platform_model_id=''):
        return self.RR2.find_platform_devices_by_platform_model(platform_model_id)

    def find_instrument_model_by_instrument_agent(self, instrument_agent_id=''):
        return self.RR2.find_instrument_models_of_instrument_agent(instrument_agent_id)

    def find_instrument_agent_by_instrument_model(self, instrument_model_id=''):
        return self.RR2.find_instrument_agents_by_instrument_model(instrument_model_id)

    def find_instrument_device_by_instrument_agent_instance(self, instrument_agent_instance_id=''):
        return self.RR2.find_instrument_devices_by_instrument_agent_instance(instrument_agent_instance_id)

    def find_instrument_agent_instance_by_instrument_device(self, instrument_device_id=''):
        instrument_agent_instance_objs = self.RR2.find_instrument_agent_instances_of_instrument_device(instrument_device_id)
        if 0 < len(instrument_agent_instance_objs):
            log.debug("L4-CI-SA-RQ-363: device %s is connected to instrument agent instance %s",
                      str(instrument_device_id),
                      str(instrument_agent_instance_objs[0]._id))
        return instrument_agent_instance_objs

    def find_instrument_device_by_platform_device(self, platform_device_id=''):
        return self.RR2.find_instrument_devices_of_platform_device(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.RR2.find_platform_devices_by_instrument_device(instrument_device_id)


    def find_instrument_device_by_logical_instrument(self, logical_instrument_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_logical_instrument_by_instrument_device(self, instrument_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_platform_device_by_logical_platform(self, logical_platform_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_logical_platform_by_platform_device(self, platform_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_data_product_by_instrument_device(self, instrument_device_id=''):
        raise NotImplementedError("TODO: this function will be removed")

    def find_instrument_device_by_data_product(self, data_product_id=''):
        raise NotImplementedError("TODO: this function will be removed")


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


    def execute_instrument_agent_lifecycle(self, instrument_agent_id="", lifecycle_event=""):
       """
       declare a instrument_agent to be in a given state
       @param instrument_agent_id the resource id
       """
       return self.RR2.advance_lcs(instrument_agent_id, lifecycle_event)

    def execute_instrument_agent_instance_lifecycle(self, instrument_agent_instance_id="", lifecycle_event=""):
       """
       declare a instrument_agent_instance to be in a given state
       @param instrument_agent_instance_id the resource id
       """
       return self.RR2.advance_lcs(instrument_agent_instance_id, lifecycle_event)

    def execute_instrument_model_lifecycle(self, instrument_model_id="", lifecycle_event=""):
       """
       declare a instrument_model to be in a given state
       @param instrument_model_id the resource id
       """
       return self.RR2.advance_lcs(instrument_model_id, lifecycle_event)

    def execute_instrument_device_lifecycle(self, instrument_device_id="", lifecycle_event=""):
       """
       declare an instrument_device to be in a given state
       @param instrument_device_id the resource id
       """
       return self.RR2.advance_lcs(instrument_device_id, lifecycle_event)

    def execute_platform_agent_lifecycle(self, platform_agent_id="", lifecycle_event=""):
       """
       declare a platform_agent to be in a given state
       @param platform_agent_id the resource id
       """
       return self.RR2.advance_lcs(platform_agent_id, lifecycle_event)

    def execute_platform_agent_instance_lifecycle(self, platform_agent_instance_id="", lifecycle_event=""):
       """
       declare a platform_agent_instance to be in a given state
       @param platform_agent_instance_id the resource id
       """
       return self.RR2.advance_lcs(platform_agent_instance_id, lifecycle_event)

    def execute_platform_model_lifecycle(self, platform_model_id="", lifecycle_event=""):
       """
       declare a platform_model to be in a given state
       @param platform_model_id the resource id
       """
       return self.RR2.advance_lcs(platform_model_id, lifecycle_event)

    def execute_platform_device_lifecycle(self, platform_device_id="", lifecycle_event=""):
       """
       declare a platform_device to be in a given state
       @param platform_device_id the resource id
       """
       return self.RR2.advance_lcs(platform_device_id, lifecycle_event)

    def execute_sensor_model_lifecycle(self, sensor_model_id="", lifecycle_event=""):
       """
       declare a sensor_model to be in a given state
       @param sensor_model_id the resource id
       """
       return self.RR2.advance_lcs(sensor_model_id, lifecycle_event)

    def execute_sensor_device_lifecycle(self, sensor_device_id="", lifecycle_event=""):
       """
       declare a sensor_device to be in a given state
       @param sensor_device_id the resource id
       """
       return self.RR2.advance_lcs(sensor_device_id, lifecycle_event)




    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################


    def get_instrument_device_extension(self, instrument_device_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an InstrumentDeviceExtension object containing additional related information
        @param instrument_device_id    str
        @param ext_associations    dict
        @param ext_exclude    list
        @retval instrument_device    InstrumentDeviceExtension
        @throws BadRequest    A parameter is missing
        @throws NotFound    An object with the specified instrument_device_id does not exist
        """

        if not instrument_device_id:
            raise BadRequest("The instrument_device_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_instrument = extended_resource_handler.create_extended_resource_container(
            OT.InstrumentDeviceExtension,
            instrument_device_id,
            OT.InstrumentDeviceComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)

        # clean up InstAgent list as it sometimes includes the device
        ia = []
        for agent in extended_instrument.instrument_agent:
            if agent.type_ == 'InstrumentAgent':
                ia.append(agent)
        extended_instrument.instrument_agent = ia

        # Status computation
        status_rollups = self.outil.get_status_roll_ups(instrument_device_id, RT.InstrumentDevice)

        def short_status_rollup(key):
            return ComputedIntValue(status=ComputedValueAvailability.PROVIDED,
                                    value=status_rollups[instrument_device_id].get(key, StatusType.STATUS_UNKNOWN))

        extended_instrument.computed.communications_status_roll_up = short_status_rollup("comms")
        extended_instrument.computed.power_status_roll_up          = short_status_rollup("power")
        extended_instrument.computed.data_status_roll_up           = short_status_rollup("data")
        extended_instrument.computed.location_status_roll_up       = short_status_rollup("loc")
        extended_instrument.computed.aggregated_status             = short_status_rollup("agg")

        return extended_instrument


    # TODO: this causes a problem because an instrument agent must be running in order to look up extended attributes.
    def obtain_agent_handle(self, device_id):
        ia_client = ResourceAgentClient(device_id,  process=self)
        log.debug("got the instrument agent client here: %s for the device id: %s and process: %s", ia_client, device_id, self)

#       #todo: any validation?
#        cmd = AgentCommand(command='get_current_state')
#        retval = self._ia_client.execute_agent(cmd)
#        state = retval.result
#        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)
#

        return ia_client

    def obtain_agent_calculation(self, device_id, result_container):
        ret = IonObject(result_container)
        a_client = None
        try:
            a_client = self.obtain_agent_handle(device_id)
            ret.status = ComputedValueAvailability.PROVIDED
        except NotFound:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
            ret.reason = "Could not connect to instrument agent instance -- may not be running"
        except Exception as e:
            raise e

        return a_client, ret

    #functions for INSTRUMENT computed attributes -- currently bogus values returned

    def get_firmware_version(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 0.0 #todo: use ia_client
        return ret


    def get_last_data_received_datetime(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 0.0 #todo: use ia_client
        return ret


    def get_operational_state(self, taskable_resource_id):   # from Device
        ia_client, ret = self.obtain_agent_calculation(taskable_resource_id, OT.ComputedStringValue)
        if ia_client:
            ret.value = "" #todo: use ia_client
        return ret

    def get_last_calibration_datetime(self, instrument_device_id):
        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
        if ia_client:
            ret.value = 0 #todo: use ia_client
        return ret

    def get_uptime(self, device_id):
        ia_client, ret = self.obtain_agent_calculation(device_id, OT.ComputedStringValue)

        if ia_client:
            # Find events in the event repo that were published when changes of state occurred for the instrument or the platform
            # The Instrument Agent publishes events of a particular type, ResourceAgentStateEvent, and origin_type. So we query the events db for those.

            #----------------------------------------------------------------------------------------------
            # Check whether it is a platform or an instrument
            #----------------------------------------------------------------------------------------------
            device = self.RR.read(device_id)

            #----------------------------------------------------------------------------------------------
            # These below are the possible new event states while taking the instrument off streaming mode or the platform off monitoring mode
            # This is info got from possible actions to wind down the instrument or platform that one can take in the UI when the device is already streaming/monitoring
            #----------------------------------------------------------------------------------------------
            event_state = ''
            not_streaming_states = [ResourceAgentState.COMMAND, ResourceAgentState.INACTIVE, ResourceAgentState.UNINITIALIZED]

            if device.type_ == 'InstrumentDevice':
                event_state = ResourceAgentState.STREAMING
            elif device.type_ == 'PlatformDevice':
                event_state = 'PLATFORM_AGENT_STATE_MONITORING'

            #----------------------------------------------------------------------------------------------
            # Get events associated with device from the events db
            #----------------------------------------------------------------------------------------------
            log.debug("For uptime, we are checking the device with id: %s, type_: %s, and searching recent events for the following event_state: %s",device_id, device.type_, event_state)
            event_tuples = self.container.event_repository.find_events(origin=device_id, event_type='ResourceAgentStateEvent', descending=True)

            recent_events = [tuple[2] for tuple in event_tuples]

            #----------------------------------------------------------------------------------------------
            # We assume below that the events have been sorted in time, with most recent events first in the list
            #----------------------------------------------------------------------------------------------
            for evt in recent_events:
                log.debug("Got a recent event with event_state: %s", evt.state)

                if evt.state == event_state: # "RESOURCE_AGENT_STATE_STREAMING"
                    current_time = get_ion_ts() # this is in milliseconds
                    log.debug("Got most recent streaming event with ts_created:  %s. Got the current time: %s", evt.ts_created, current_time)
                    return self._convert_to_string(ret, int(current_time)/1000 - int(evt.ts_created)/1000 )
                elif evt.state in not_streaming_states:
                    log.debug("Got a most recent event state that means instrument is not streaming anymore: %s", evt.state)
                    # The instrument has been recently shut down. This has happened recently and no need to look further whether it was streaming earlier
                    return self._convert_to_string(ret, 0)

        return self._convert_to_string(ret, 0)

    def _convert_to_string(self, ret, value):
        """
        A helper method to put it in a string value into a ComputedStringValue object that will be returned

        @param ret ComputedStringValue object
        @param value int
        @retval ret The ComputedStringValue with a value that is of type String
        """
        sec = timedelta(seconds = value)
        d = datetime(1,1,1) + sec

        ret.value = "%s days, %s hours, %s minutes" %(d.day-1, d.hour, d.minute)
        log.debug("Returning the computed attribute for uptime with value: %s", ret.value)
        return ret

    #functions for INSTRUMENT computed attributes -- currently bogus values returned

    def get_platform_device_extension(self, platform_device_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an PlatformDeviceExtension object containing additional related information
        """

        if not platform_device_id:
            raise BadRequest("The platform_device_id parameter is empty")

        extended_resource_handler = ExtendedResourceContainer(self)

        extended_platform = extended_resource_handler.create_extended_resource_container(
            OT.PlatformDeviceExtension,
            platform_device_id,
            OT.PlatformDeviceComputedAttributes,
            ext_associations=ext_associations,
            ext_exclude=ext_exclude,
            user_id=user_id)


        # lookup all hasModel predicates
        # lookup is a 2d associative array of [subject type][subject id] -> object id
        lookup = dict([(rt, {}) for rt in [RT.PlatformDevice, RT.InstrumentDevice]])
        for a in self.RR.find_associations(predicate=PRED.hasModel, id_only=False):
            if a.st in lookup:
                lookup[a.st][a.s] = a.o

        def retrieve_model_objs(rsrc_list, object_type):
        # rsrc_list is devices that need models looked up.  object_type is the resource type (a device)
        # not all devices have models (represented as None), which kills read_mult.  so, extract the models ids,
        #  look up all the model ids, then create the proper output
            model_list = [lookup[object_type].get(r._id) for r in rsrc_list]
            model_uniq = list(set([m for m in model_list if m is not None]))
            model_objs = self.clients.resource_registry.read_mult(model_uniq)
            model_dict = dict(zip(model_uniq, model_objs))
            return [model_dict.get(m) for m in model_list]

        extended_platform.instrument_models = retrieve_model_objs(extended_platform.instrument_devices,
                                                                  RT.InstrumentDevice)
        extended_platform.platform_models   = retrieve_model_objs(extended_platform.platforms,
                                                                  RT.PlatformDevice)

        s_unknown = StatusType.STATUS_UNKNOWN

        # Status computation
        extended_platform.computed.instrument_status = [s_unknown] * len(extended_platform.instrument_devices)
        extended_platform.computed.platform_status   = [s_unknown] * len(extended_platform.platforms)

        def status_unknown():
            return ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=StatusType.STATUS_UNKNOWN)

        extended_platform.computed.communications_status_roll_up = status_unknown()
        extended_platform.computed.power_status_roll_up          = status_unknown()
        extended_platform.computed.data_status_roll_up           = status_unknown()
        extended_platform.computed.location_status_roll_up       = status_unknown()
        extended_platform.computed.aggregated_status             = status_unknown()

        try:
            status_rollups = self.outil.get_status_roll_ups(platform_device_id, RT.PlatformDevice)

            extended_platform.computed.instrument_status = [status_rollups.get(idev._id,{}).get("agg", s_unknown)
                                                            for idev in extended_platform.instrument_devices]
            extended_platform.computed.platform_status = [status_rollups(pdev._id,{}).get("agg", s_unknown)
                                                          for pdev in extended_platform.platforms]

            def short_status_rollup(key):
                return ComputedIntValue(status=ComputedValueAvailability.PROVIDED,
                                        value=status_rollups[platform_device_id].get(key, StatusType.STATUS_UNKNOWN))

            extended_platform.computed.communications_status_roll_up = short_status_rollup("comms")
            extended_platform.computed.power_status_roll_up          = short_status_rollup("power")
            extended_platform.computed.data_status_roll_up           = short_status_rollup("data")
            extended_platform.computed.location_status_roll_up       = short_status_rollup("loc")
            extended_platform.computed.aggregated_status             = short_status_rollup("agg")

        except Exception as ex:
            log.exception("Computed attribute failed for %s" % platform_device_id)

        return extended_platform


    def get_data_product_parameters_set(self, resource_id=''):
        # return the set of data product with the processing_level_code as the key to identify
        ret = IonObject(OT.ComputedDictValue)
        log.debug("get_data_product_parameters_set: resource_id is %s ", str(resource_id))
        if not resource_id:
            raise BadRequest("The resource_id parameter is empty")

        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(resource_id,
                                                                          PRED.hasOutputProduct,
                                                                          RT.DataProduct,
                                                                          True)
        log.debug("get_data_product_parameters_set: data_product_ids is %s ", str(data_product_ids))
        if not data_product_ids:
            ret.status = ComputedValueAvailability.NOTAVAILABLE
        else:
            for data_product_id in data_product_ids:
                data_product_obj = self.clients.resource_registry.read(data_product_id)

                #retrieve the stream for this data product
                data_product_stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id,
                                                                                  PRED.hasStream,
                                                                                  RT.Stream,
                                                                                  True)
                if not data_product_stream_ids:
                    raise BadRequest("The data product has no stream associated")
                #retrieve the stream definitions for this stream
                stream_def_ids, _ = self.clients.resource_registry.find_objects(data_product_stream_ids[0],
                                                                                  PRED.hasStreamDefinition,
                                                                                  RT.StreamDefinition,
                                                                                  True)
                if not stream_def_ids:
                    raise BadRequest("The data product stream has no stream definition associated")

                context_dict = {}
                pdict = self.clients.pubsub_management.read_stream_definition(stream_def_ids[0]).parameter_dictionary
                log.debug("get_data_product_parameters_set: pdict %s ", str(pdict) )

                pdict_full = ParameterDictionary.load(pdict)

                for key in pdict_full.keys():
                    log.debug("get_data_product_parameters_set: key %s ", str(key))
                    context = DatasetManagementService.get_parameter_context_by_name(key)
                    log.debug("get_data_product_parameters_set: context %s ", str(context))
                    context_dict[key] = context.dump()

                ret.value[data_product_obj.processing_level_code] = context_dict
            ret.status = ComputedValueAvailability.PROVIDED
        return ret
