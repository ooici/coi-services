#!/usr/bin/env python

__author__ = 'Maurice Manning, Ian Katz, Michael Meisinger'

import os
import pwd
import json
from datetime import datetime, timedelta
import time
from collections import defaultdict

from ooi.logging import log
from ooi.timer import Timer, Accumulator

from pyon.agent.agent import ResourceAgentState, ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent, BadRequest, NotFound, ServerError, Unauthorized
from pyon.core.governance import GovernanceHeaderValues, has_org_role
from pyon.core.governance import has_valid_shared_resource_commitment, is_resource_owner
from pyon.ion.resource import ExtendedResourceContainer
from pyon.public import LCE, RT, PRED, OT
from pyon.util.ion_time import IonTime
from pyon.util.containers import get_ion_ts
from coverage_model.parameter import ParameterDictionary

from ion.agents.port.port_agent_process import PortAgentProcess
from ion.agents.platform.platform_agent import PlatformAgentState
from ion.processes.event.device_state import DeviceStateManager
from ion.services.sa.instrument.rollx_builder import RollXBuilder
from ion.services.sa.instrument.status_builder import AgentStatusBuilder
from ion.services.sa.instrument.agent_configuration_builder import InstrumentAgentConfigurationBuilder, \
    PlatformAgentConfigurationBuilder
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.sa.instrument.flag import KeywordFlag
from ion.services.sa.observatory.observatory_management_service import INSTRUMENT_OPERATOR_ROLE
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from ion.services.sa.observatory.deployment_util import describe_deployments
from ion.util.agent_launcher import AgentLauncher
from ion.util.module_uploader import RegisterModulePreparerEgg
from ion.util.qa_doc_parser import QADocParser
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.util.resource_lcs_policy import AgentPolicy, ResourceLCSPolicy, ModelPolicy, DevicePolicy

from interface.objects import AttachmentType, ComputedValueAvailability, ProcessDefinition, ComputedDictValue
from interface.objects import DeviceStatusType, AggregateStatusType
from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService


stats = Accumulator(persist=True)


class InstrumentManagementService(BaseInstrumentManagementService):
    """
    @brief Service to manage instrument, platform, and sensor resources, their relationships, and direct access
    """
    def on_init(self):
        #suppress a few "variable declared but not used" annoying pyflakes errors
        IonObject("Resource")

        self.override_clients(self.clients)

        self.extended_resource_handler = ExtendedResourceContainer(self)

        self.init_module_uploader()
        self.agent_status_builder = AgentStatusBuilder(process=self)

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


        instrument_agent_instance_obj = \
            self.RR2.find_instrument_agent_instance_of_instrument_device_using_has_agent_instance(instrument_device_id)

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

        inst_agent_instance_obj = \
            self.RR2.find_instrument_agent_instance_of_instrument_device_using_has_agent_instance(instrument_device_id)
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
#                                              to_name=ResourceAgentClient._get_agent_process_id(instrument_device_id),
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

        instrument_agent_instance_id = self.create_instrument_agent_instance_(instrument_agent_instance)

        if instrument_agent_id:
            self.assign_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)

        if instrument_device_id:
            self.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)
        log.debug("device %s now connected to instrument agent instance %s (L4-CI-SA-RQ-363)",
                  str(instrument_device_id),  str(instrument_agent_instance_id))

        return instrument_agent_instance_id

    def create_instrument_agent_instance_(self, instrument_agent_instance=None):
        """
        create a new instance
        @param instrument_agent_instance the object to be created as a resource
        @retval instrument_agent_instance_id the id of the new object
        @throws BadRequest if the incoming _id field is set
        @throws BadReqeust if the incoming name already exists
        """

        return self.RR2.create(instrument_agent_instance, RT.InstrumentAgentInstance)

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




    def record_instrument_producer_activation(self, instrument_device_id, instrument_agent_config):

        log.debug("update the producer context for provenance")
        #todo: should get the time from process dispatcher
        producer_obj = self._get_instrument_producer(instrument_device_id)
        if OT.InstrumentProducerContext == producer_obj.producer_context.type_:

            producer_obj.producer_context.activation_time =  IonTime().to_string()
            producer_obj.producer_context.configuration = instrument_agent_config

            # get the site where this device is currently deploy instrument_device_id
            try:
                site_id = self.RR2.find_instrument_site_id_by_instrument_device_using_has_device(instrument_device_id)
                producer_obj.producer_context.deployed_site_id = site_id
            except NotFound:
                pass
            except:
                raise
            self.RR2.update(producer_obj)

    def _assert_persistence_on(self, config_builder):
        if not config_builder or RT.DataProduct not in config_builder.associated_objects:
           return
        data_products = config_builder.associated_objects[RT.DataProduct]
        if config_builder._get_device().type_ == RT.PlatformDevice:
            for dp in data_products:
                if self.DPMS.is_persisted(dp._id):
                    return
            raise BadRequest("Cannot start agent - data product persistence is not activated!")
        else:
            parsed_dp_id = None
            for dp in data_products:
                if dp.processing_level_code == "Parsed":
                    parsed_dp_id = dp._id
                    break
            if parsed_dp_id:
                if not self.DPMS.is_persisted(parsed_dp_id):
                    raise BadRequest("Cannot start agent - data product persistence is not activated!")
            else:
                log.warn("Cannot determine if persistence is activated for agent instance=%s", config_builder.agent_instance_obj._id)

    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instrument agent instance and return the id
        """
        instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)
        if instrument_agent_instance_obj.type_ == RT.ExternalDatasetAgentInstance:
            log.info("IMS.start_instrument_agent_instance() is=%s is ExternalDatasetAgentInstance - forwarding to DAMS", instrument_agent_instance_id)
            return self.DAMS.start_external_dataset_agent_instance(instrument_agent_instance_id)
        elif instrument_agent_instance_obj.type_ != RT.InstrumentAgentInstance:
            raise BadRequest("Expected a InstrumentAgentInstance for the resource %s, but received type %s" %
                            (instrument_agent_instance_id, instrument_agent_instance_obj.type_))

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
            log.error('failed to launch', exc_info=True)
            raise ServerError('failed to launch')

        # Check that persistence is on
        self._assert_persistence_on(config_builder)

        # Save the config into an object in the object store which will be passed to the agent by the container.
        config_builder.record_launch_parameters(config)

        process_id = launcher.launch(config, config_builder._get_process_definition()._id)
        if not process_id:
            raise ServerError("Launched instrument agent instance but no process_id")

        # reload resource as it has been updated by the launch function
        #instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)

        self.record_instrument_producer_activation(config_builder._get_device()._id, config)

        launcher.await_launch(self._agent_launch_timeout("start_instrument_agent_instance"))

        return process_id


    def _agent_launch_timeout(self, fn_name):
        # some hopefully intelligent buffers on timeout.
        #
        # we expect to have at least 20 seconds to launch the agent.
        # the agent needs around 6 seconds to launch, currently.  pad that out to 16 seconds.
        #
        # use a 1-second buffer to guarantee that we time out the launch and not the function call
        # let the buffer be longer (up to 5 seconds) if we have time.
        # if the buffer is calculated to be short, warn.
        remaining_time_s = self._remaining_reply_time_s(fn_name)

        minbuffer  = 1
        maxbuffer  = 5
        launchtime = 16

        buffer = max(minbuffer, min(maxbuffer, remaining_time_s - launchtime))
        log.debug("Agent launch buffer time is %s", buffer)

        if buffer == minbuffer:
            log_fn = log.warn
        else:
            log_fn = log.info

        log_fn("Allowing (%s - %s) seconds for agent launch in %s", remaining_time_s, buffer, fn_name)

        return remaining_time_s - buffer


    def _remaining_reply_time_s(self, fn_name):
        ret = int(self._remaining_reply_time_ms(fn_name) / 1000)
        return ret

    def _remaining_reply_time_ms(self, fn_name):
        """
        look into the request headers to find out how many milliseconds are left before the call will time out

        @param fn_name the name of the RPC function that will be
        """

        ctx = self.get_context()
        # make sure the op matches our function name
        if "op" not in ctx or fn_name != ctx["op"]:
            raise BadRequest("Could not find reply-by for %s in get_context: %s" % (fn_name, ctx))

        # look for the reply-by field
        if "reply-by" not in ctx:
            raise BadRequest("Could not find reply-by field in context %s" % ctx)

        # convert to int and only allow it if it's nonzero
        reply_by_val = int(ctx["reply-by"])
        if 0 == reply_by_val:
            raise BadRequest("Got a zero value when parsing 'reply-by' field of '%s'" % ctx["reply_by"])

        # get latest time
        now = int(get_ion_ts())

        return reply_by_val - now


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
        instrument_agent_instance_obj = self.RR2.read(instrument_agent_instance_id)
        if instrument_agent_instance_obj.type_ == RT.ExternalDatasetAgentInstance:
            log.info("IMS.stop_instrument_agent_instance() id=%s is ExternalDatasetAgentInstance - forwarding to DAMS", instrument_agent_instance_id)
            return self.DAMS.stop_external_dataset_agent_instance(instrument_agent_instance_id)
        elif instrument_agent_instance_obj.type_ != RT.InstrumentAgentInstance:
            raise BadRequest("Expected a InstrumentAgentInstance for the resource %s, but received type %s" %
                            (instrument_agent_instance_id, instrument_agent_instance_obj.type_))

        try:
            instance_obj, device_id = self._stop_agent_instance(instrument_agent_instance_id,
                                                                RT.InstrumentDevice, instrument_agent_instance_obj)

        except BadRequest as e:
            #
            # stopping the instrument agent instance failed, but try at least
            # to stop the port agent:
            #
            log.error("Exception in _stop_agent_instance: %s", e)
            log.debug("Trying to stop the port agent anyway ...")
            instance_obj = self.RR2.read(instrument_agent_instance_id)
            self._stop_port_agent(instance_obj.port_agent_config)

            # raise the exception anyway:
            raise e

        self._stop_port_agent(instance_obj.port_agent_config)

        #update the producer context for provenance
        producer_obj = self._get_instrument_producer(device_id)
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.deactivation_time =  IonTime().to_string()
            self.RR2.update(producer_obj)


    def _stop_agent_instance(self, agent_instance_id, device_type, agent_instance_obj=None):
        """
        Deactivate an agent instance, return device ID
        """
        if agent_instance_obj is None:
            agent_instance_obj = self.RR2.read(agent_instance_id)

        device_id = self.RR2.find_subject(subject_type=device_type,
                                          predicate=PRED.hasAgentInstance,
                                          object=agent_instance_id,
                                          id_only=True)

        agent_process_id = ResourceAgentClient._get_agent_process_id(device_id)

        log.debug("Canceling the execution of agent's process ID")
        if None is agent_process_id:
            raise BadRequest("Agent Instance '%s' does not have an agent_process_id.  Stopped already?"
            % agent_instance_id)
        try:
            self.clients.process_dispatcher.cancel_process(process_id=agent_process_id)
        except NotFound:
            log.debug("No agent process found")
            pass
        except Exception as e:
            raise e
        else:
            log.debug("Success cancelling agent process")

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
        process_definition.name = "ProcessDefinition for InstrumentAgent %s" % instrument_agent.name
        process_definition.executable['url'] = instrument_agent.agent_uri
        process_definition.executable['module'] = instrument_agent.agent_module or 'ion.agents.instrument.instrument_agent'
        process_definition.executable['class'] = instrument_agent.agent_class or 'InstrumentAgent'
        pd = self.clients.process_dispatcher
        process_definition_id = pd.create_process_definition(process_definition=process_definition)

        # Associate the agent and the process def
        self.RR2.assign_process_definition_to_instrument_agent_with_has_process_definition(process_definition_id, instrument_agent_id)

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

        process_def_objs = \
            self.RR2.find_process_definitions_of_instrument_agent_using_has_process_definition(instrument_agent_id)

        for pd_obj in process_def_objs:
            self.RR2.unassign_process_definition_from_instrument_agent_with_has_process_definition(pd_obj._id, instrument_agent_id)
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
    ##  GOVERNANCE FUNCTIONS
    ##
    ##


    def check_lifecycle_policy(self, process, message, headers):

        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process)
        except Inconsistent, ex:
            return False, ex.message

        #Device policy
        if gov_values.op == 'execute_instrument_device_lifecycle' or gov_values.op == 'execute_platform_device_lifecycle':

            log.debug("check_device_lifecycle_policy: actor info: %s %s %s", gov_values.actor_id, gov_values.actor_roles, gov_values.resource_id)

            if message.has_key('lifecycle_event'):
                lifecycle_event = message['lifecycle_event']
            else:
                return False, '%s(%s) has been denied since the lifecycle_event can not be found in the message'% (process.name, gov_values.op)

            orgs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=gov_values.resource_id, id_only=False)

            if not orgs:
                return False, '%s(%s) has been denied since the resource id %s has not been shared with any Org' % (process.name, gov_values.op, gov_values.resource_id)

            #Handle these lifecycle transitions first

            if not (lifecycle_event == LCE.INTEGRATE or lifecycle_event == LCE.DEPLOY or lifecycle_event == LCE.RETIRE):

                #The owner can do any of these other lifecycle transitions
                is_owner = is_resource_owner(gov_values.actor_id, gov_values.resource_id)
                if is_owner:
                    return True, ''

                #TODO - this shared commitment might not be with the right Org - may have to relook at how this is working.
                is_shared = has_valid_shared_resource_commitment(gov_values.actor_id, gov_values.resource_id)

                #Check across Orgs which have shared this device for role which as proper level to allow lifecycle transition
                for org in orgs:
                    if has_org_role(gov_values.actor_roles, org.org_governance_name, [INSTRUMENT_OPERATOR_ROLE] ) and is_shared:
                        return True, ''

            return False, '%s(%s) has been denied since the user %s has not acquired the resource or is not the proper role for this transition: %s' % (process.name, gov_values.op, gov_values.actor_id, lifecycle_event)

        else:

            return True, ''



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
        platform_agent_instance_obj = self.RR2.read(platform_agent_instance_id)
        if platform_agent_instance_obj.type_ == RT.ExternalDatasetAgentInstance:
            log.info("IMS.start_platform_agent_instance() id=%s with ExternalDatasetAgentInstance - forwarding to DAMS", platform_agent_instance_id)
            return self.DAMS.start_external_dataset_agent_instance(platform_agent_instance_id)
        elif platform_agent_instance_obj.type_ != RT.PlatformAgentInstance:
            raise BadRequest("Expected a InstrumentAgentInstance for the resource %s, but received type %s" %
                            (platform_agent_instance_id, platform_agent_instance_obj.type_))

        configuration_builder = PlatformAgentConfigurationBuilder(self.clients)
        launcher = AgentLauncher(self.clients.process_dispatcher)


        configuration_builder.set_agent_instance_object(platform_agent_instance_obj)
        config = configuration_builder.prepare()

        platform_device_obj = configuration_builder._get_device()
        log.debug("start_platform_agent_instance: device is %s connected to platform agent instance %s (L4-CI-SA-RQ-363)",
                  str(platform_device_obj._id),  str(platform_agent_instance_id))

        # Check that persistence is on
        self._assert_persistence_on(configuration_builder)

        # Save the config into an object in the object store which will be passed to the agent by the container.
        configuration_builder.record_launch_parameters(config)

        process_id = launcher.launch(config, configuration_builder._get_process_definition()._id)

        launcher.await_launch(self._agent_launch_timeout("start_platform_agent_instance"))

        return process_id

    def stop_platform_agent_instance(self, platform_agent_instance_id=''):
        """
        Deactivate the platform agent instance
        """
        platform_agent_instance_obj = self.RR2.read(platform_agent_instance_id)
        if platform_agent_instance_obj.type_ == RT.ExternalDatasetAgentInstance:
            log.info("IMS.stop_platform_agent_instance() id=%s is ExternalDatasetAgentInstance - forwarding to DAMS", platform_agent_instance_id)
            return self.DAMS.stop_external_dataset_agent_instance(platform_agent_instance_id)
        elif platform_agent_instance_obj.type_ != RT.PlatformAgentInstance:
            raise BadRequest("Expected a InstrumentAgentInstance for the resource %s, but received type %s" %
                            (platform_agent_instance_id, platform_agent_instance_obj.type_))

        self._stop_agent_instance(platform_agent_instance_id, RT.PlatformDevice, platform_agent_instance_obj)




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
        process_definition.name = "ProcessDefinition for PlatformAgent %s" % platform_agent.name
        process_definition.executable['url'] = platform_agent.agent_uri
        process_definition.executable['module'] = platform_agent.agent_module or 'ion.agents.platform.platform_agent'
        process_definition.executable['class'] = platform_agent.agent_class or 'PlatformAgent'
        pd = self.clients.process_dispatcher
        process_definition_id = pd.create_process_definition(process_definition=process_definition)

        # Associate the agent and the process def
        self.RR2.assign_process_definition_to_platform_agent_with_has_process_definition(process_definition_id,
                                                                                         platform_agent_id)
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

        self.RR2.assign_one_instrument_model_to_instrument_device_with_has_model(instrument_model_id,
                                                                                 instrument_device_id)


    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.RR2.unassign_instrument_model_from_instrument_device_with_has_model(instrument_model_id,
                                                                                 instrument_device_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.RR2.assign_instrument_model_to_instrument_agent_with_has_model(instrument_model_id,
                                                                            instrument_agent_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.RR2.unassign_instrument_model_from_instrument_agent_with_has_model(instrument_agent_id,
                                                                                instrument_model_id)

    def assign_platform_model_to_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.RR2.assign_platform_model_to_platform_agent_with_has_model(platform_model_id, platform_agent_id)

    def unassign_platform_model_from_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.RR2.unassign_platform_model_from_platform_agent_with_has_model(platform_model_id, platform_agent_id)

    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.RR2.assign_one_sensor_model_to_sensor_device_with_has_model(sensor_model_id, sensor_device_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.RR2.unassign_sensor_model_from_sensor_device_with_has_model(self, sensor_model_id, sensor_device_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.RR2.assign_one_platform_model_to_platform_device_with_has_model(platform_model_id, platform_device_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.RR2.unassign_platform_model_from_platform_device_with_has_model(platform_model_id, platform_device_id)

    def assign_instrument_device_to_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.RR2.assign_instrument_device_to_one_platform_device_with_has_device(instrument_device_id,
                                                                                 platform_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.RR2.unassign_instrument_device_from_platform_device_with_has_device(instrument_device_id,
                                                                                 platform_device_id)

    def assign_platform_device_to_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.RR2.assign_platform_device_to_one_platform_device_with_has_device(child_platform_device_id,
                                                                               platform_device_id)

    def unassign_platform_device_from_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.RR2.unassign_platform_device_from_platform_device_with_has_device(child_platform_device_id,
                                                                               platform_device_id)

    def assign_platform_agent_to_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.RR2.assign_one_platform_agent_to_platform_agent_instance_with_has_agent_definition(platform_agent_id,
                                                                                                platform_agent_instance_id)

    def unassign_platform_agent_from_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.RR2.unassign_platform_agent_from_platform_agent_instance_with_has_agent_definition(platform_agent_id,
                                                                                                platform_agent_instance_id)

    def assign_instrument_agent_to_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.RR2.assign_one_instrument_agent_to_instrument_agent_instance_with_has_agent_definition(instrument_agent_id,
                                                                                                    instrument_agent_instance_id)

    def unassign_instrument_agent_from_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.RR2.unassign_instrument_agent_from_instrument_agent_instance_with_has_agent_definition(instrument_agent_id, instrument_agent_instance_id)

    def assign_instrument_agent_instance_to_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.RR2.assign_one_instrument_agent_instance_to_instrument_device_with_has_agent_instance(instrument_agent_instance_id, instrument_device_id)

    def unassign_instrument_agent_instance_from_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.RR2.unassign_instrument_agent_instance_from_instrument_device_with_has_agent_instance(instrument_agent_instance_id, instrument_device_id)

    def assign_platform_agent_instance_to_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.RR2.assign_one_platform_agent_instance_to_platform_device_with_has_agent_instance(platform_agent_instance_id, platform_device_id)

    def unassign_platform_agent_instance_from_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.RR2.unassign_platform_agent_instance_from_platform_device_with_has_agent_instance(platform_agent_instance_id, platform_device_id)

    def assign_sensor_device_to_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.RR2.assign_sensor_device_to_one_instrument_device_with_has_device(sensor_device_id, instrument_device_id)

    def unassign_sensor_device_from_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.RR2.unassign_sensor_device_from_instrument_device_with_has_device(sensor_device_id, instrument_device_id)


    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################



    def deploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        # OBSOLETE - Move calls to assign/unassign in observatory_management
        self.RR2.assign_deployment_to_instrument_device_with_has_deployment(deployment_id, instrument_device_id)


    def undeploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        # OBSOLETE - Move calls to assign/unassign in observatory_management
        self.RR2.unassign_deployment_from_instrument_device_with_has_deployment(deployment_id, instrument_device_id)


    def deploy_platform_device(self, platform_device_id='', deployment_id=''):
        # OBSOLETE - Move calls to assign/unassign in observatory_management
        self.RR2.assign_deployment_to_platform_device_with_has_deployment(deployment_id, platform_device_id)


    def undeploy_platform_device(self, platform_device_id='', deployment_id=''):
        # OBSOLETE - Move calls to assign/unassign in observatory_management
        self.RR2.unassign_deployment_from_platform_device_with_has_deployment(deployment_id, platform_device_id)



    ############################
    #
    #  ASSOCIATION FIND METHODS
    #
    ############################


    def find_instrument_model_by_instrument_device(self, instrument_device_id=''):
        return self.RR2.find_instrument_models_of_instrument_device_using_has_model(instrument_device_id)

    def find_instrument_device_by_instrument_model(self, instrument_model_id=''):
        return self.RR2.find_instrument_devices_by_instrument_model_using_has_model(instrument_model_id)

    def find_platform_model_by_platform_device(self, platform_device_id=''):
        return self.RR2.find_platform_models_of_platform_device_using_has_model(platform_device_id)

    def find_platform_device_by_platform_model(self, platform_model_id=''):
        return self.RR2.find_platform_devices_by_platform_model_using_has_model(platform_model_id)

    def find_instrument_model_by_instrument_agent(self, instrument_agent_id=''):
        return self.RR2.find_instrument_models_of_instrument_agent_using_has_model(instrument_agent_id)

    def find_instrument_agent_by_instrument_model(self, instrument_model_id=''):
        return self.RR2.find_instrument_agents_by_instrument_model_using_has_model(instrument_model_id)

    def find_instrument_device_by_instrument_agent_instance(self, instrument_agent_instance_id=''):
        return self.RR2.find_instrument_devices_by_instrument_agent_instance_using_has_agent_instance(instrument_agent_instance_id)

    def find_instrument_agent_instance_by_instrument_device(self, instrument_device_id=''):
        instrument_agent_instance_objs = \
            self.RR2.find_instrument_agent_instances_of_instrument_device_using_has_agent_instance(instrument_device_id)
        if instrument_agent_instance_objs:
            log.debug("L4-CI-SA-RQ-363: device %s is connected to instrument agent instance %s",
                      str(instrument_device_id),
                      str(instrument_agent_instance_objs[0]._id))
        else:
            instrument_agent_instance_objs = \
                self.RR2.find_external_dataset_agent_instances_of_instrument_device_using_has_agent_instance(instrument_device_id)
            log.debug("Found ExternalDatasetAgentInstance %s for InstrumentDevice %s" % (instrument_agent_instance_objs, instrument_device_id))

        return instrument_agent_instance_objs

    def find_instrument_device_by_platform_device(self, platform_device_id=''):
        return self.RR2.find_instrument_devices_of_platform_device_using_has_device(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.RR2.find_platform_devices_by_instrument_device_using_has_device(instrument_device_id)




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
        t = Timer() if stats.is_log_enabled() else None
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
        if t:
            t.complete_step('ims.instrument_device_extension.container')

        try:
            # Prefetch persisted status from object store for all devices of interest (one here)
            dsm = DeviceStateManager()
            device_ids_of_interest = [instrument_device_id]
            state_list = dsm.read_states(device_ids_of_interest)
            status_dict = {}
            for dev_id, dev_state in zip(device_ids_of_interest, state_list):
                status = {AggregateStatusType.AGGREGATE_DATA: DeviceStatusType.STATUS_UNKNOWN,
                          AggregateStatusType.AGGREGATE_COMMS: DeviceStatusType.STATUS_UNKNOWN,
                          AggregateStatusType.AGGREGATE_POWER: DeviceStatusType.STATUS_UNKNOWN,
                          AggregateStatusType.AGGREGATE_LOCATION: DeviceStatusType.STATUS_UNKNOWN}
                if dev_state is not None:
                    for k, v in dev_state['agg_status'].iteritems():
                        status[int(k)] = v['status']
                status_dict[dev_id] = status

            # retrieve the statuses for the instrument
            self.agent_status_builder.add_device_rollup_statuses_to_computed_attributes(instrument_device_id,
                                                                                        extended_instrument.computed,
                                                                                        status_dict=status_dict)
            # retrieve the aggregate status for the instrument
            status_values = [ extended_instrument.computed.communications_status_roll_up,
                              extended_instrument.computed.data_status_roll_up,
                              extended_instrument.computed.location_status_roll_up,
                              extended_instrument.computed.power_status_roll_up  ]
            status = self.agent_status_builder._crush_status_list(status_values)

            log.debug('get_instrument_device_extension  extended_instrument.computed: %s', extended_instrument.computed)
            if t:
                t.complete_step('ims.instrument_device_extension.rollup')

            # add UI details for deployments in same order as deployments
            extended_instrument.deployment_info = describe_deployments(extended_instrument.deployments, self.clients,
                                                                       instruments=[extended_instrument.resource],
                                                                       instrument_status=[status])
            if t:
                t.complete_step('ims.instrument_device_extension.deploy')
                stats.add(t)

        except Exception as ex:
            log.exception("Cannot build instrument %s status", instrument_device_id)

        return extended_instrument


    # functions for INSTRUMENT computed attributes -- currently bogus values returned

    def get_last_data_received_datetime(self, instrument_device_id):
        # not currently available from device or agent
        ret = IonObject(OT.ComputedFloatValue)
        ret.value = 0
        ret.status = ComputedValueAvailability.NOTAVAILABLE
        return ret


    def get_operational_state(self, taskable_resource_id):   # from Device

        resource_agent_state_labels = {
            'RESOURCE_AGENT_STATE_POWERED_DOWN': 'POWERED DOWN',
            'RESOURCE_AGENT_STATE_UNINITIALIZED':'UNINITIALIZED',
            'RESOURCE_AGENT_STATE_INACTIVE': 'INACTIVE',
            'RESOURCE_AGENT_STATE_IDLE': 'IDLE',
            'RESOURCE_AGENT_STATE_STOPPED': 'STOPPED',
            'RESOURCE_AGENT_STATE_COMMAND': 'COMMAND',
            'RESOURCE_AGENT_STATE_STREAMING': 'STREAMING',
            'RESOURCE_AGENT_STATE_TEST': 'TEST',
            'RESOURCE_AGENT_STATE_CALIBRATE': 'CALIBRATE',
            'RESOUCE_AGENT_STATE_DIRECT_ACCESS': 'DIRECT ACCESS',
            'RESOURCE_AGENT_STATE_BUSY': 'BUSY',
            'RESOURCE_AGENT_STATE_LOST_CONNECTION': 'LOST CONNECTION',
            'PLATFORM_AGENT_STATE_AUTOSAMPLE' : 'STREAMING',
            'PLATFORM_AGENT_STATE_LAUNCHING' : 'LAUNCHING'
        }

        retval = IonObject(OT.ComputedStringValue)
        ia_client, reason = self.agent_status_builder.get_device_agent(taskable_resource_id)

        # early exit for no client
        if ia_client is None:
            retval.value = 'UNKNOWN'
            retval.status = ComputedValueAvailability.NOTAVAILABLE
            retval.reason = reason
            return retval

        try:
            state = ia_client.get_agent_state()
            if resource_agent_state_labels.has_key(state):
                retval.value = resource_agent_state_labels[ state ]
                retval.status = ComputedValueAvailability.PROVIDED
            else:
                log.warning('IMS:get_operational_state label map has no value for this state:  %s', state)
                retval.value = 'UNKNOWN'
                retval.status = ComputedValueAvailability.NOTAVAILABLE
                retval.reason = "State not returned in agent response"

        except Unauthorized:
            retval.value = 'UNKNOWN'
            retval.status = ComputedValueAvailability.NOTAVAILABLE
            retval.reason = "The requester does not have the proper role to access the status of this agent"


        return retval


    def get_uptime(self, device_id):
        ia_client, ret = self.agent_status_builder.obtain_agent_calculation(device_id, OT.ComputedStringValue)

        if not ia_client:
            return self._convert_to_string(ret, 0)

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
            event_state = PlatformAgentState.MONITORING

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
        log.trace("Returning the computed attribute for uptime with value: %s", ret.value)
        return ret

    #functions for INSTRUMENT computed attributes -- currently bogus values returned

    def get_platform_device_extension(self, platform_device_id='', ext_associations=None, ext_exclude=None, user_id=''):
        """Returns an PlatformDeviceExtension object containing additional related information
        """
        t = Timer() if stats.is_log_enabled() else None

        RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)
        outil = ObservatoryUtil(self, enhanced_rr=RR2)

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
        if t:
            t.complete_step('ims.platform_device_extension.create')

        # Build a lookup for device models via hasModel predicates.
        # lookup is a 2d associative array of [subject type][subject id] -> object id
        RR2.cache_predicate(PRED.hasModel)
        if t:
            t.complete_step('ims.platform_device_extension.assoc')
        lookup = {rt : {} for rt in [RT.InstrumentDevice, RT.PlatformDevice]}
        for a in RR2.filter_cached_associations(PRED.hasModel, lambda assn: assn.st in lookup):
            lookup[a.st][a.s] = a.o

        def retrieve_model_objs(rsrc_list, object_type):
        # rsrc_list is devices that need models looked up.  object_type is the resource type (a device)
        # not all devices have models (represented as None), which kills read_mult.  so, extract the models ids,
        #  look up all the model ids, then create the proper output
            model_list = [lookup[object_type].get(r._id) for r in rsrc_list]
            model_uniq = list(set([m for m in model_list if m is not None]))
            model_objs = self.RR2.read_mult(model_uniq)
            model_dict = dict(zip(model_uniq, model_objs))
            return [model_dict.get(m) for m in model_list]

        if extended_platform.deployed_site and not "_id" in extended_platform.deployed_site:
            extended_platform.deployed_site = None

        # Set platform parents
        device_relations = outil.get_child_devices(platform_device_id)

        extended_platform.parent_platform_device = None
        extended_platform.parent_platform_model = None
        res_objs = RR2.find_subjects(subject_type=RT.PlatformDevice, predicate=PRED.hasDevice, object=platform_device_id, id_only=False)
        if res_objs:
            extended_platform.parent_platform_device = res_objs[0]
            res_objs1 = RR2.find_objects(subject=res_objs[0]._id, predicate=PRED.hasModel, object_type=RT.PlatformModel, id_only=False)
            if res_objs1:
                extended_platform.parent_platform_model = res_objs1[0]

        # Set the platform device children
        child_platform_ids = [did for pt,did,dt in device_relations[platform_device_id] if dt == RT.PlatformDevice]
        child_instrument_ids = [did for pt,did,dt in device_relations[platform_device_id] if dt == RT.InstrumentDevice]
        child_ids = list(set(child_platform_ids + child_instrument_ids))
        child_objs = RR2.read_mult(child_ids)
        child_by_id = dict(zip(child_ids, child_objs))

        extended_platform.platforms = [child_by_id[did] for did in child_platform_ids]
        extended_platform.platform_models = retrieve_model_objs(extended_platform.platforms, RT.PlatformDevice)

        extended_platform.instrument_devices = [child_by_id[did] for did in child_instrument_ids]
        extended_platform.instrument_models = retrieve_model_objs(extended_platform.instrument_devices, RT.InstrumentDevice)
        if t:
            t.complete_step('ims.platform_device_extension.index')

        # Set network connected devices
        up_devices = self.RR.find_objects(subject=platform_device_id, predicate=PRED.hasNetworkParent, id_only=False)
        down_devices = self.RR.find_subjects(predicate=PRED.hasNetworkParent, object=platform_device_id, id_only=False)
        extended_platform.connected_devices = up_devices + down_devices
        extended_platform.connected_device_info = []
        for dev in extended_platform.connected_devices:
            info_dict = dict(direction="up" if dev in up_devices else "down",
                             port="TBD",
                             status="TBD")
            extended_platform.connected_device_info.append(info_dict)

        # JIRA OOIION948: REMOVE THIS BLOCK WHEN FIXED
        # Note: probably fixed by now, add back special decorator
        # add portals, sites related to platforms (SHOULD HAPPEN AUTOMATICALLY USING THE COMPOUND ASSOCIATION)
        if extended_platform.deployed_site and not extended_platform.portals:
            extended_platform.portals = RR2.find_objects(subject=extended_platform.deployed_site._id, predicate=PRED.hasSite, id_only=False)
        # END JIRA BLOCK

        # Set primary device (as children of self) at immediate child sites
        child_site_ids = [p._id for p in extended_platform.portals]
        portal_device_relations = outil.get_device_relations(child_site_ids)
        extended_platform.portal_instruments = []
        portal_instrument_ids = []
        for ch_id in child_site_ids:
            device_id = self._get_site_device(ch_id, portal_device_relations)
            device_obj = child_by_id.get(device_id, None)
            extended_platform.portal_instruments.append(device_obj)
            if device_obj:
                #these are the same set of devices that constitute the rollup status for this platform device, create the list
                portal_instrument_ids.append(device_obj._id)

        log.debug('have portal instruments %s', [i._id if i else "None" for i in extended_platform.portal_instruments])

        # Building status - for PlatformAgents only (not ExternalDatasetAgent)
        # @TODO: clean this UP!!!
        child_device_ids = device_relations.keys()
        ### agent_status_builder
        self.agent_status_builder.add_device_rollup_statuses_to_computed_attributes(platform_device_id,
                                                                                    extended_platform.computed,
                                                                                    portal_instrument_ids)
        ### agent_status_builder
        statuses, reason = self.agent_status_builder.get_cumulative_status_dict(platform_device_id)
        def csl(device_id_list):
            ### agent_status_builder
            return self.agent_status_builder.compute_status_list(statuses, device_id_list)

        extended_platform.computed.instrument_status = csl([dev._id for dev in extended_platform.instrument_devices])
        extended_platform.computed.platform_status   = csl([platform._id for platform in extended_platform.platforms])
        log.debug('instrument_status: %s %r instruments %d\nplatform_status: %s %r platforms %d',
            extended_platform.computed.instrument_status.reason, extended_platform.computed.instrument_status.value, len(extended_platform.instrument_devices),
            extended_platform.computed.platform_status.reason, extended_platform.computed.platform_status.value, len(extended_platform.platforms))
        if t:
            t.complete_step('ims.platform_device_extension.status')

        # TODO: why don't we just use the immediate device children? child_objs
        ids =[i._id if i else None for i in extended_platform.portal_instruments]
        extended_platform.computed.portal_status = csl(ids)
        log.debug('%d portals, %d instruments, %d status: %r', len(extended_platform.portals),
                  len(extended_platform.portal_instruments), len(extended_platform.computed.portal_status.value), ids)

        rollx_builder = RollXBuilder(self)
        top_platformnode_id = rollx_builder.get_toplevel_network_node(platform_device_id)
        if t:
            t.complete_step('ims.platform_device_extension.top')

        ### agent_status_builder
        net_stats, ancestors = rollx_builder.get_network_hierarchy(top_platformnode_id,
                                                                   lambda x: self.agent_status_builder.get_aggregate_status_of_device(x))
        if t:
            t.complete_step('ims.platform_device_extension.hierarchy')
        extended_platform.computed.rsn_network_child_device_status = ComputedDictValue(value=net_stats,
                                                                                       status=ComputedValueAvailability.PROVIDED)
        if t:
            t.complete_step('ims.platform_device_extension.nodes')
        parent_node_device_ids = rollx_builder.get_parent_network_nodes(platform_device_id)

        if not parent_node_device_ids:
            # todo, just the current network status?
            extended_platform.computed.rsn_network_rollup = ComputedDictValue(status=ComputedValueAvailability.NOTAVAILABLE,
                                                                              reason="Could not find parent network node")
        else:
            ### agent_status_builder
            parent_node_statuses = [self.agent_status_builder.get_status_of_device(x) for x in parent_node_device_ids]
            rollup_values = {}
            for astkey, astname in AggregateStatusType._str_map.iteritems():
                log.debug("collecting all %s values to crush", astname)
                single_type_list = [nodestat.get(astkey, DeviceStatusType.STATUS_UNKNOWN) for nodestat in parent_node_statuses]
                 ### agent_status_builder
                rollup_values[astkey] = self.agent_status_builder._crush_status_list(single_type_list)

            extended_platform.computed.rsn_network_rollup = ComputedDictValue(status=ComputedValueAvailability.PROVIDED,
                                                                             value=rollup_values)
        if t:
            t.complete_step('ims.platform_device_extension.crush')

        # add UI details for deployments
        extended_platform.deployment_info = describe_deployments(extended_platform.deployments, self.clients,
                instruments=extended_platform.instrument_devices, instrument_status=extended_platform.computed.instrument_status.value)
        if t:
            t.complete_step('ims.platform_device_extension.deploy')
            stats.add(t)

        return extended_platform

    def _get_site_device(self, site_id, device_relations):
        site_devices = [tup[1] for tup in device_relations.get(site_id, []) if tup[2] in (RT.InstrumentDevice, RT.PlatformDevice)]
        if len(site_devices) > 1:
            log.error("Inconsistent: Site %s has multiple devices: %s", site_id, site_devices)
        if not site_devices:
            return None
        return site_devices[0]

    def get_data_product_parameters_set(self, resource_id=''):
        # return the set of data product with the processing_level_code as the key to identify
        ret = IonObject(OT.ComputedDictValue)
        log.debug("get_data_product_parameters_set: resource_id is %s ", resource_id)
        if not resource_id:
            raise BadRequest("The resource_id parameter is empty")

        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(resource_id,
                                                                          PRED.hasOutputProduct,
                                                                          RT.DataProduct,
                                                                          True)
        log.debug("get_data_product_parameters_set: data_product_ids is %s ", data_product_ids)
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
                log.trace("get_data_product_parameters_set: pdict %s ", str(pdict) )

                pdict_full = ParameterDictionary.load(pdict)

                for key in pdict_full.keys():
                    log.debug("get_data_product_parameters_set: key %s ", str(key))
                    context = DatasetManagementService.get_parameter_context_by_name(key)
                    log.debug("get_data_product_parameters_set: context %s ", str(context))
                    context_dict[key] = context.dump()

                ret.value[data_product_obj.processing_level_code] = context_dict
            ret.status = ComputedValueAvailability.PROVIDED
        return ret


    ############################
    #
    #  PREPARE RESOURCES
    #
    ############################


    def prepare_instrument_device_support(self, instrument_device_id=''):
        """
        Returns the object containing the data to create/update an instrument device resource
        """

        #TODO - does this have to be filtered by Org ( is an Org parameter needed )
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(instrument_device_id, OT.InstrumentDevicePrepareSupport)

        #Fill out service request information for creating a instrument device
        extended_resource_handler.set_service_requests(resource_data.create_request, 'instrument_management',
            'create_instrument_device', { "instrument_device":  "$(instrument_device)" })

        #Fill out service request information for creating a instrument device
        extended_resource_handler.set_service_requests(resource_data.update_request, 'instrument_management',
            'update_instrument_device', { "instrument_device":  "$(instrument_device)" })

        #Fill out service request information for assigning a model
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentModel'].assign_request, 'instrument_management',
            'assign_instrument_model_to_instrument_device', { "instrument_model_id":  "$(instrument_model_id)",
                                                              "instrument_device_id":  instrument_device_id })


        #Fill out service request information for unassigning a model
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentModel'].unassign_request, 'instrument_management',
            'unassign_instrument_model_from_instrument_device', { "instrument_model_id":  "$(instrument_model_id)",
                                                                "instrument_device_id":  instrument_device_id })

        #There can be multiple instruments to a platform
        resource_data.associations['SensorDevice'].multiple_associations = True

        #Fill out service request information for assigning a sensor
        extended_resource_handler.set_service_requests(resource_data.associations['SensorDevice'].assign_request, 'instrument_management',
            'assign_sensor_device_to_instrument_device', { "sensor_device_id":  "$(sensor_device_id)",
                                                              "instrument_device_id":  instrument_device_id })


        #Fill out service request information for unassigning a sensor
        extended_resource_handler.set_service_requests(resource_data.associations['SensorDevice'].unassign_request, 'instrument_management',
            'unassign_sensor_device_from_instrument_device', { "sensor_device_id":  "$(sensor_device_id)",
                                                                "instrument_device_id":  instrument_device_id })

        ####
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgentInstance'].assign_request,
                                                       'instrument_management',
                                                       'assign_instrument_agent_instance_to_instrument_device',
                                                       {'instrument_device_id':  instrument_device_id,
                                                        'instrument_agent_instance_id':  '$(instrument_agent_instance_id)' })

        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgentInstance'].unassign_request,
                                                       'instrument_management',
                                                       'unassign_instrument_agent_instance_from_instrument_device',
                                                       {'instrument_device_id':  instrument_device_id,
                                                        'instrument_agent_instance_id':  '$(instrument_agent_instance_id)' })

        # prepare grouping for IAI
        ia_to_im   = [a for a in self.RR2.find_associations(predicate='hasModel') if a.st=='InstrumentAgent']
        iai_to_ia  = self.RR2.find_associations(predicate='hasAgentDefinition')
        all_iai, _ = self.RR2.find_resources('InstrumentAgentInstance', id_only=True)

        # discussions indicate we want to only show unassociated IAIs or IAIs associated with this ID
        # this is a list of all IAIs resids currently associated to an ID, not including this current ID we're preparing for
        cur_id_to_iai_without_this = [a.o for a in self.RR2.find_associations(predicate='hasAgentInstance') if a.st=='InstrumentDevice' and a.s != instrument_device_id]
        allowed_list = list(set(all_iai).difference(set(cur_id_to_iai_without_this)))
        def allowed(iai):
            return iai in allowed_list

        ia_to_iai = defaultdict(list)
        for a in iai_to_ia:
            if allowed(a.s):
                ia_to_iai[a.o].append(a.s)

        resource_data.associations['InstrumentAgentInstance'].group = {'group_by': 'InstrumentModel',
                                                                       'resources': {iaimassoc.o:ia_to_iai[iaimassoc.s] for iaimassoc in ia_to_im}}

        return resource_data

    def prepare_instrument_agent_instance_support(self, instrument_agent_instance_id=''):
        """
        Returns the object containing the data to create/update an instrument agent instance resource
        """

        #TODO - does this have to be filtered by Org ( is an Org parameter needed )
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(instrument_agent_instance_id, OT.InstrumentAgentInstancePrepareSupport)

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.create_request, 'instrument_management',
            'create_instrument_agent_instance', { "instrument_agent_instance":  "$(instrument_agent_instance)" })

        #Fill out service request information for creating a instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.update_request, 'instrument_management',
            'update_instrument_agent_instance', { "instrument_agent_instance":  "$(instrument_agent_instance)" })

        #Fill out service request information for starting an instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.start_request, 'instrument_management',
            'start_instrument_agent_instance', { "instrument_agent_instance_id":  "$(instrument_agent_instance_id)" })

        #Fill out service request information for starting an instrument agent instance
        extended_resource_handler.set_service_requests(resource_data.stop_request, 'instrument_management',
            'stop_instrument_agent_instance', { "instrument_agent_instance_id":  "$(instrument_agent_instance_id)" })

        #Fill out service request information for assigning a InstrumentDevice
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDevice'].assign_request, 'instrument_management',
            'assign_instrument_agent_instance_to_instrument_device', {"instrument_device_id":  "$(instrument_device_id)",
                                                                      "instrument_agent_instance_id":  instrument_agent_instance_id })

        #Fill out service request information for unassigning a InstrumentDevice
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDevice'].unassign_request, 'instrument_management',
            'unassign_instrument_agent_instance_from_instrument_device', {"instrument_device_id":  "$(instrument_device_id)",
                                                                          "instrument_agent_instance_id":  instrument_agent_instance_id })

        #Fill out service request information for assigning a InstrumentAgent
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgent'].assign_request, 'instrument_management',
            'assign_instrument_agent_to_instrument_agent_instance', {"instrument_agent_id":  "$(instrument_agent_id)",
                                                                     "instrument_agent_instance_id":  instrument_agent_instance_id })

        #Fill out service request information for unassigning a InstrumentAgent
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgent'].unassign_request, 'instrument_management',
            'unassign_instrument_agent_from_instrument_agent_instance', {"instrument_agent_id":  "$(instrument_agent_id)",
                                                                         "instrument_agent_instance_id":  instrument_agent_instance_id })


        return resource_data

    def prepare_platform_device_support(self, platform_device_id=''):
        """
        Returns the object containing the data to create/update an instrument device resource
        """

        #TODO - does this have to be filtered by Org ( is an Org parameter needed )
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(platform_device_id, OT.PlatformDevicePrepareSupport)

        #Fill out service request information for creating a platform device
        extended_resource_handler.set_service_requests(resource_data.create_request, 'instrument_management',
            'create_platform_device', { "platform_device":  "$(platform_device)" })

        #Fill out service request information for creating a platform device
        extended_resource_handler.set_service_requests(resource_data.update_request, 'instrument_management',
            'update_platform_device', { "platform_device":  "$(platform_device)" })

        #Fill out service request information for assigning a model
        extended_resource_handler.set_service_requests(resource_data.associations['PlatformModel'].assign_request, 'instrument_management',
            'assign_platform_model_to_platform_device', { "platform_model_id":  "$(platform_model_id)",
                                                          "platform_device_id":  platform_device_id })


        #Fill out service request information for unassigning a model
        extended_resource_handler.set_service_requests(resource_data.associations['PlatformModel'].unassign_request, 'instrument_management',
            'unassign_platform_model_from_platform_device', {  "platform_model_id":  "$(platform_model_id)",
                                                             "platform_device_id":  platform_device_id})

        #There can be multiple instruments to a platform
        resource_data.associations['InstrumentDevice'].multiple_associations = True

        #Fill out service request information for assigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDevice'].assign_request, 'instrument_management',
            'assign_instrument_device_to_platform_device', {"instrument_device_id":  "$(instrument_device_id)",
                                                            "platform_device_id":  platform_device_id })


        #Fill out service request information for unassigning an instrument
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentDevice'].unassign_request, 'instrument_management',
            'unassign_instrument_device_from_platform_device', { "instrument_device_id":  "$(instrument_device_id)",
                                                               "platform_device_id":  platform_device_id })

        ####
        extended_resource_handler.set_service_requests(resource_data.associations['PlatformAgentInstance'].assign_request,
                                                       'instrument_management',
                                                       'assign_platform_agent_instance_to_platform_device',
                                                       {'platform_device_id':  platform_device_id,
                                                        'platform_agent_instance_id':  '$(platform_agent_instance_id)' })

        extended_resource_handler.set_service_requests(resource_data.associations['PlatformAgentInstance'].unassign_request,
                                                       'instrument_management',
                                                       'unassign_platform_agent_instance_to_platform_device',
                                                       {'platform_device_id':  platform_device_id,
                                                        'platform_agent_instance_id':  '$(platform_agent_instance_id)' })

        # prepare grouping for PAI
        pa_to_pm   = [a for a in self.RR2.find_associations(predicate='hasModel') if a.st=='PlatformAgent']
        pai_to_pa  = self.RR2.find_associations(predicate='hasAgentDefinition')
        all_pai, _ = self.RR2.find_resources('PlatformAgentInstance', id_only=True)

        # discussions indicate we want to only show unassociated PAIs or PAIs associated with this PD
        # this is a list of all PAIs resids currently associated to an PD, not including this current PD we're preparing for
        cur_pd_to_pai_without_this = [a.o for a in self.RR2.find_associations(predicate='hasAgentInstance') if a.st=='PlatformDevice' and a.s != platform_device_id]
        allowed_list = list(set(all_pai).difference(set(cur_pd_to_pai_without_this)))
        def allowed(pai):
            return pai in allowed_list

        pa_to_pai = defaultdict(list)
        for a in pai_to_pa:
            if allowed(a.s):
                pa_to_pai[a.o].append(a.s)

        resource_data.associations['PlatformAgentInstance'].group = {'group_by': 'PlatformModel',
                                                                     'resources': {papmassoc.o:pa_to_pai[papmassoc.s] for papmassoc in pa_to_pm}}

        # prepare grouping for EDAI
        eda_to_pm   = [a for a in self.RR2.find_associations(predicate='hasModel') if a.st=='ExternalDatasetAgent']
        edai_to_eda  = self.RR2.find_associations(predicate='hasAgentDefinition')
        all_edai, _ = self.RR2.find_resources('ExternalDatasetAgentInstance', id_only=True)

        # discussions indicate we want to only show unassociated PAIs or PAIs associated with this PD
        # this is a list of all PAIs resids currently associated to an PD, not including this current PD we're preparing for
        cur_pd_to_edai_without_this = [a.o for a in self.RR2.find_associations(predicate='hasAgentInstance') if a.st=='PlatformDevice' and a.s != platform_device_id]
        allowed_list = list(set(all_edai).difference(set(cur_pd_to_edai_without_this)))
        def allowed(edai):
            return edai in allowed_list

        eda_to_edai = defaultdict(list)
        for a in edai_to_eda:
            if allowed(a.s):
                eda_to_edai[a.o].append(a.s)

        resource_data.associations['ExternalDatasetAgentInstance'].group = {'group_by': 'PlatformModel',
                                                                     'resources': {edapmassoc.o:eda_to_edai[edapmassoc.s] for edapmassoc in eda_to_pm}}

        return resource_data

    def prepare_instrument_agent_support(self, instrument_agent_id=''):
        """
        Returns the object containing the data to create/update an instrument agent resource
        """

        #TODO - does this have to be filtered by Org ( is an Org parameter needed )
        extended_resource_handler = ExtendedResourceContainer(self)

        resource_data = extended_resource_handler.create_prepare_resource_support(instrument_agent_id, OT.InstrumentAgentPrepareSupport)

        #Fill out service request information for creating a instrument agent
        extended_resource_handler.set_service_requests(resource_data.create_request, 'instrument_management',
            'create_instrument_agent', { "instrument_agent":  "$(instrument_agent)" })

        #Fill out service request information for creating a instrument agent
        extended_resource_handler.set_service_requests(resource_data.update_request, 'instrument_management',
            'update_instrument_agent', { "instrument_agent":  "$(instrument_agent)" })

        #Fill out service request information for assigning a InstrumentModel
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentModel'].assign_request,
                                                       'instrument_management',
                                                       'assign_instrument_model_to_instrument_agent',
                                                       {"instrument_model_id":  "$(instrument_model_id)",
                                                        "instrument_agent_id":  instrument_agent_id })

        #Fill out service request information for unassigning a InstrumentModel
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentModel'].unassign_request,
                                                       'instrument_management',
                                                       'unassign_instrument_model_from_instrument_agent',
                                                       {"instrument_model_id":  "$(instrument_model_id)",
                                                        "instrument_agent_id":  instrument_agent_id })

        #There can be multiple instruments to a InstrumentModel
        resource_data.associations['InstrumentModel'].multiple_associations = True

        #Fill out service request information for assigning a InstrumentAgentInstance
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgentInstance'].assign_request, 'instrument_management',
            'assign_instrument_agent_to_instrument_agent_instance', {"instrument_agent_id":  instrument_agent_id,
                                                                     "instrument_agent_instance_id": "$(instrument_agent_instance_id)" })

        #Fill out service request information for unassigning a InstrumentAgentInstance
        extended_resource_handler.set_service_requests(resource_data.associations['InstrumentAgentInstance'].unassign_request, 'instrument_management',
            'unassign_instrument_agent_from_instrument_agent_instance', {"instrument_agent_id": instrument_agent_id,
                                                                         "instrument_agent_instance_id":  "$(instrument_agent_instance_id)" })


        return resource_data
