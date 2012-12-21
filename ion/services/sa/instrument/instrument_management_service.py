#!/usr/bin/env python

__author__ = 'Maurice Manning, Ian Katz, Michael Meisinger'


import os
import pwd
import json
from datetime import date, datetime, timedelta
import time
import tempfile

from ooi.logging import log

from pyon.agent.agent import ResourceAgentClient
from pyon.core.bootstrap import IonObject
from pyon.core.exception import Inconsistent,BadRequest, NotFound
from pyon.ion.resource import ExtendedResourceContainer
from pyon.util.ion_time import IonTime
from pyon.public import LCE
from pyon.public import RT, PRED, OT, CFG

from coverage_model.parameter import ParameterDictionary

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from ion.services.sa.instrument.flag import KeywordFlag

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

from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.observatory_util import ObservatoryUtil

from ion.util.module_uploader import RegisterModulePreparerEgg
from ion.util.qa_doc_parser import QADocParser

# TODO: these are for methods which may belong in DAMS/DPMS/MFMS
from ion.services.sa.product.data_product_impl import DataProductImpl
from ion.services.sa.instrument.data_producer_impl import DataProducerImpl

from ion.agents.port.port_agent_process import PortAgentProcess, PortAgentProcessType

from interface.objects import AttachmentType, ComputedValueAvailability, ProcessDefinition, ComputedIntValue
from interface.services.sa.iinstrument_management_service import BaseInstrumentManagementService


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

            #LCS
            reg_precondition(self, 'execute_instrument_agent_lifecycle',
                             self.instrument_agent.policy_fn_lcs_precondition("instrument_agent_id"))
            reg_precondition(self, 'execute_instrument_agent_instance_lifecycle',
                             self.instrument_agent_instance.policy_fn_lcs_precondition("instrument_agent_instance_id"))
            reg_precondition(self, 'execute_instrument_model_lifecycle',
                             self.instrument_model.policy_fn_lcs_precondition("instrument_model_id"))
            reg_precondition(self, 'execute_instrument_device_lifecycle',
                             self.instrument_device.policy_fn_lcs_precondition("instrument_device_id"))
            reg_precondition(self, 'execute_platform_agent_lifecycle',
                             self.platform_agent.policy_fn_lcs_precondition("platform_agent_id"))
            reg_precondition(self, 'execute_platform_agent_instance_lifecycle',
                             self.platform_agent_instance.policy_fn_lcs_precondition("platform_agent_instance_id"))
            reg_precondition(self, 'execute_platform_model_lifecycle',
                             self.platform_model.policy_fn_lcs_precondition("platform_model_id"))
            reg_precondition(self, 'execute_platform_device_lifecycle',
                             self.platform_device.policy_fn_lcs_precondition("platform_device_id"))
            reg_precondition(self, 'execute_sensor_model_lifecycle',
                             self.sensor_model.policy_fn_lcs_precondition("sensor_model_id"))
            reg_precondition(self, 'execute_sensor_device_lifecycle',
                             self.sensor_device.policy_fn_lcs_precondition("sensor_device_id"))

            #Delete
            reg_precondition(self, 'force_delete_instrument_agent',
                             self.instrument_agent.policy_fn_delete_precondition("instrument_agent_id"))
            reg_precondition(self, 'force_delete_instrument_agent_instance',
                             self.instrument_agent_instance.policy_fn_delete_precondition("instrument_agent_instance_id"))
            reg_precondition(self, 'force_delete_instrument_model',
                             self.instrument_model.policy_fn_delete_precondition("instrument_model_id"))
            reg_precondition(self, 'force_delete_instrument_device',
                             self.instrument_device.policy_fn_delete_precondition("instrument_device_id"))
            reg_precondition(self, 'force_delete_platform_agent',
                             self.platform_agent.policy_fn_delete_precondition("platform_agent_id"))
            reg_precondition(self, 'force_delete_platform_agent_instance',
                             self.platform_agent_instance.policy_fn_delete_precondition("platform_agent_instance_id"))
            reg_precondition(self, 'force_delete_platform_model',
                             self.platform_model.policy_fn_delete_precondition("platform_model_id"))
            reg_precondition(self, 'force_delete_platform_device',
                             self.platform_device.policy_fn_delete_precondition("platform_device_id"))
            reg_precondition(self, 'force_delete_sensor_model',
                             self.sensor_model.policy_fn_delete_precondition("sensor_model_id"))
            reg_precondition(self, 'force_delete_sensor_device',
                             self.sensor_device.policy_fn_delete_precondition("sensor_device_id"))

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

        #farm everything out to the impls

        self.instrument_agent           = InstrumentAgentImpl(new_clients)
        self.instrument_agent_instance  = InstrumentAgentInstanceImpl(new_clients)
        self.instrument_model           = InstrumentModelImpl(new_clients)
        self.instrument_device          = InstrumentDeviceImpl(new_clients)

        self.platform_agent           = PlatformAgentImpl(new_clients)
        self.platform_agent_instance  = PlatformAgentInstanceImpl(new_clients)
        self.platform_model           = PlatformModelImpl(new_clients)
        self.platform_device          = PlatformDeviceImpl(new_clients)

        self.sensor_model    = SensorModelImpl(new_clients)
        self.sensor_device   = SensorDeviceImpl(new_clients)

        self.instrument_site = InstrumentSiteImpl(new_clients)

        #TODO: may not belong in this service
        self.data_product        = DataProductImpl(new_clients)
        self.data_producer       = DataProducerImpl(new_clients)




    def restore_resource_state(self, instrument_device_id='', attachment_id=''):
        """
        restore a snapshot of an instrument agent instance config
        """

        instrument_device_obj = self.RR.read(instrument_device_id)
        resource_type = type(instrument_device_obj).__name__
        if not RT.InstrumentDevice == resource_type:
            raise BadRequest("Can only restore resource states for %s resources, got %s" %
                             (RT.InstrumentDevice, resource_type))

        # get instrument_agent_instance_id
        inst_agent_inst_objs = self.instrument_device.find_stemming_agent_instance(instrument_device_id)
        n = len(inst_agent_inst_objs)
        if 1 != n:
            raise NotFound("%s instrument agent instances found for instrument %s, not 1" % (n, instrument_device_id))
        instrument_agent_instance_obj = self.instrument_agent_instance.read_one(inst_agent_inst_objs[0]._id)

        attachment = self.clients.resource_registry.read_attachment(attachment_id, include_content=True)

        if not KeywordFlag.CONFIG_SNAPSHOT in attachment.keywords:
            raise BadRequest("Attachment '%s' does not seem to be a config snapshot" % attachment_id)

        if not 'application/json' == attachment.content_type:
            raise BadRequest("Attachment '%s' is not labeled as json")

        snapshot = json.loads(attachment.content)
        driver_config = snapshot["driver_config"]

        instrument_agent_instance_obj.driver_config["comms_config"] = driver_config["comms_config"]
        instrument_agent_instance_obj.driver_config["pagent_pid"]   = driver_config["pagent_pid"]

        self.instrument_agent_instance.update_one(instrument_agent_instance_obj)

        #todo
        #agent.set_config(snapshot["running_config"])

        #todo
        # re-launch agent?



    def save_resource_state(self, instrument_device_id='', name=''):
        """
        take a snapshot of the current instrument agent instance config for this instrument,
          and save it as an attachment
        """
        instrument_device_obj = self.RR.read(instrument_device_id)
        resource_type = type(instrument_device_obj).__name__
        if not RT.InstrumentDevice == resource_type:
            raise BadRequest("Can only save resource states for %s resources, got %s" %
                             (RT.InstrumentDevice, resource_type))

        # get instrument_agent_instance_id
        inst_agent_inst_objs = self.instrument_device.find_stemming_agent_instance(instrument_device_id)

        if 0 == len(inst_agent_inst_objs):
            raise NotFound("No instrument agent instance was found for instrument %s" % instrument_device_id)

        inst_agent_instance_obj = inst_agent_inst_objs[0]

        self._validate_instrument_agent_instance(inst_agent_inst_objs[0])

        epoch = time.mktime(datetime.now().timetuple())
        snapshot_name = name or "Running Config Snapshot %s.js" % epoch

        driver_config, agent_config = self._generate_instrument_agent_config(instrument_device_id)

        snapshot = {}
        snapshot["driver_config"] = driver_config
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
        return self.clients.resource_registry.create_attachment(instrument_device_id, attachment)



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

        instrument_agent_instance_id = self.instrument_agent_instance.create_one(instrument_agent_instance)

        if instrument_agent_id:
            self.assign_instrument_agent_to_instrument_agent_instance(instrument_agent_id, instrument_agent_instance_id)

        if instrument_device_id:
            self.assign_instrument_agent_instance_to_instrument_device(instrument_agent_instance_id, instrument_device_id)
        log.debug("create_instrument_agent_instance: device %s now connected to instrument agent instance %s (L4-CI-SA-RQ-363)", str(instrument_device_id),  str(instrument_agent_instance_id))

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

        self.instrument_agent_instance._unlink_all_subjects_by_association_type(PRED.hasAgentInstance,
                                                                                instrument_agent_instance_id)

        self.instrument_agent_instance.delete_one(instrument_agent_instance_id)


    def force_delete_instrument_agent_instance(self, instrument_agent_instance_id=''):

        self.instrument_agent_instance.force_delete_one(instrument_agent_instance_id)


    def _validate_instrument_device_preagentlaunch(self, instrument_device_id):
        #retrieve the instrument model
        model_objs = self.instrument_device.find_stemming_model(instrument_device_id)
        if 1 != len(model_objs):
            raise BadRequest("Expected 1 InstrumentDevice attached to  InstrumentAgentInstance '%s', got %d" %
                             (str(instrument_device_id), len(model_objs)))

        model_obj = model_objs[0]
        instrument_model_id = model_obj._id

        #retrieve the associated instrument agent
        agent_objs = self.instrument_agent.find_having_model(instrument_model_id)
        if 1 != len(agent_objs):
            raise BadRequest("Expected 1 InstrumentAgent attached to InstrumentModel '%s', got %d" %
                             (str(instrument_model_id), len(agent_objs)))

        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(instrument_device_id,
                                                                          PRED.hasOutputProduct,
                                                                          RT.DataProduct,
                                                                          True)
        if not data_product_ids:
            raise NotFound("No output Data Products attached to this Instrument Device " + str(instrument_device_id))

        #retrieve the streams assoc with each defined output product
        for product_id in data_product_ids:
            stream_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasStream, RT.Stream, True)

            #One stream per product ...for now.
            if not stream_ids:
                raise NotFound("No Stream  attached to this Data Product " + str(product_id))
            if len(stream_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Stream" + str(product_id))

            #get the  parameter dictionary for this stream
            dataset_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasDataset, RT.DataSet, True)
            #One data set per product ...for now.
            if not dataset_ids:
                raise NotFound("No Dataset attached to this Data Product " + str(product_id))
            if len(dataset_ids) > 1:
                raise Inconsistent("Data Product should only have ONE Dataset" + str(product_id))




    def _validate_instrument_agent(self, instrument_agent_obj):
        if not instrument_agent_obj.stream_configurations:
            raise BadRequest("Agent does not contain stream configuration used in launching the agent. Agent Instance object: '%s",
                             str(instrument_agent_obj) )



    def _validate_instrument_agent_instance(self, instrument_agent_instance_obj):
        """
        Verify that an agent instance is valid for launch.

        Returns a dict of params necessary to start this instance
        """

        #retrieve the associated instrument device
        inst_device_objs = self.instrument_device.find_having_agent_instance(instrument_agent_instance_obj._id)
        if 1 != len(inst_device_objs):
            raise BadRequest("Expected 1 InstrumentDevice attached to  InstrumentAgentInstance '%s', got %d" %
                             (str(instrument_agent_instance_obj._id), len(inst_device_objs)))
        instrument_device_id = inst_device_objs[0]._id
        log.debug("L4-CI-SA-RQ-363: device is %s connected to instrument agent instance %s",
                  str(instrument_device_id),
                  str(instrument_agent_instance_obj._id))


        self._validate_instrument_device_preagentlaunch(instrument_device_id)

    def _generate_platform_streamconfig(self, platform_id, device_id):
        # FROM test_oms_launch2 L355

        # handled by preload
#        #create the log data product
#        self.dp_obj.name = '%s platform_eng data' % platform_id
#        data_product_id = self.dpclient.create_data_product(data_product=self.dp_obj, stream_definition_id=self.platform_eng_stream_def_id)
#        self.damsclient.assign_data_product(input_resource_id=device_id, data_product_id=data_product_id)

        data_product_ids, _ = self.clients.resource_registry.find_objects(device_id, PRED.hasOutputProduct, RT.DataProduct, True)
        if not data_product_ids:
            raise BadRequest('platform %s has no associated data product' % platform_id)
        data_product_id = data_product_ids[0]

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.clients.resource_registry.find_objects(data_product_id, PRED.hasStream, None, True)
        stream_id = stream_ids[0]

        platform_eng_dictionary = DatasetManagementService.get_parameter_dictionary_by_name('platform_eng_parsed')

        #get the streamroute object from pubsub by passing the stream_id
        stream_def_ids, _ = self.clients.resource_registry.find_objects(stream_id, PRED.hasStreamDefinition, RT.StreamDefinition, True)

        stream_route = self.clients.pubsub_management.read_stream_route(stream_id=stream_id)
        stream_config = {'routing_key' : stream_route.routing_key,
                         'stream_id' : stream_id,
                         'stream_definition_ref' : stream_def_ids[0],
                         'exchange_point' : stream_route.exchange_point,
                         'parameter_dictionary':platform_eng_dictionary.dump()}

        return stream_config


    def _generate_stream_config(self, instrument_device_id='', instrument_agent_instance_obj=None):

        #_stream_config = self.instrument_device.find_stemming_model(instrument_device_id)[0].stream_configuration

        #retrieve the agent definition
        instrument_agent_objs, _ = self.clients.resource_registry.find_objects(instrument_agent_instance_obj._id, PRED.hasAgentDefinition, RT.InstrumentAgent, False)
        if not instrument_agent_objs:
            raise NotFound("No Agent Definition attached to this Instrument Agent Instance " + str(instrument_agent_instance_obj))

        streams_dict = {}
        for stream_configuration in instrument_agent_objs[0].stream_configurations:
            #create a stream def for each param dict to match against the existing data products
            param_dict_id = self.clients.dataset_management.read_parameter_dictionary_by_name(stream_configuration.parameter_dictionary_name,
                                                                                              id_only=True)
            stream_def_id = self.clients.pubsub_management.create_stream_definition(parameter_dictionary_id=param_dict_id)
            streams_dict[stream_configuration.stream_name] = {'param_dict_name':stream_configuration.parameter_dictionary_name, 'stream_def_id':stream_def_id,
                                                            'records_per_granule': stream_configuration.records_per_granule, 'granule_publish_rate':stream_configuration.granule_publish_rate }

        #retrieve the output products
        data_product_ids, _ = self.clients.resource_registry.find_objects(instrument_device_id,
                                                                          PRED.hasOutputProduct,
                                                                          RT.DataProduct,
                                                                          True)

        out_streams = []
        for product_id in data_product_ids:
            stream_ids, _ = self.clients.resource_registry.find_objects(product_id, PRED.hasStream, RT.Stream, True)
            out_streams.append(stream_ids[0])


        stream_config_too = {}

        # create a stream config got each stream (dataproduct) assoc with this agent/device
        for product_stream_id in out_streams:

            #get the streamroute object from pubsub by passing the stream_id
            stream_def_ids, _ = self.clients.resource_registry.find_objects(product_stream_id,
                                                                            PRED.hasStreamDefinition,
                                                                            RT.StreamDefinition,
                                                                            True)

            #match the streamdefs/apram dict for this model with the data products attached to this device to know which tag to use
            for model_stream_name, stream_info_dict  in streams_dict.items():

                if self.clients.pubsub_management.compare_stream_definition(stream_info_dict.get('stream_def_id'),
                                                                            stream_def_ids[0]):
                    model_param_dict = DatasetManagementService.get_parameter_dictionary_by_name(stream_info_dict.get('param_dict_name'))
                    stream_route = self.clients.pubsub_management.read_stream_route(stream_id=product_stream_id)

                    stream_config_too[model_stream_name] = {'routing_key'           : stream_route.routing_key,
                                                            'stream_id'             : product_stream_id,
                                                            'stream_definition_ref' : stream_def_ids[0],
                                                            'exchange_point'        : stream_route.exchange_point,
                                                            'parameter_dictionary'  : model_param_dict.dump(),
                                                            'records_per_granule'  : stream_info_dict.get('records_per_granule'),
                                                            'granule_publish_rate'  : stream_info_dict.get('granule_publish_rate')
                                                            }

        log.debug("IMS:_generate_stream_config: %s", str(stream_config_too) )
        return stream_config_too


    def _generate_instrument_agent_config(self, instrument_device_id):

        instance_objs = self.instrument_device.find_stemming_agent_instance(instrument_device_id)
        if 1 != len(instance_objs):
            raise BadRequest("InstrumentDevice had %s agent instances, not 1" % len(instance_objs))

        instrument_agent_instance_obj = instance_objs[0]

        #retrieve the instrument agent for this device
        agent_objs = self.instrument_agent_instance.find_stemming_agent_definition(instrument_agent_instance_obj._id)

        if not agent_objs:
            raise BadRequest("InstrumentDevice %s does not have an agent defined" % instrument_device_id)
        instrument_agent_obj = agent_objs[0]


        stream_config = self._generate_stream_config(instrument_device_id, instrument_agent_instance_obj)

        # Create driver config.

        driver_config = {
            'dvr_mod' : instrument_agent_obj.driver_module,
            'dvr_cls' : instrument_agent_obj.driver_class,
            'workdir' : tempfile.gettempdir(),
            'process_type' : ('ZMQPyClassDriverLauncher',),
            'comms_config' : instrument_agent_instance_obj.driver_config.get('comms_config'),
            'pagent_pid' : instrument_agent_instance_obj.driver_config.get('pagent_pid')
        }

        # Create agent config.
        agent_config = {
            'driver_config' : driver_config,
            'stream_config' : stream_config,
            'agent'         : {'resource_id': instrument_device_id}
        }

        #retrieve the Org name to which this agent instance belongs
        org_name = ''
        org_objs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=instrument_agent_instance_obj._id)
        if org_objs:
            org_name = org_objs[0].name
        agent_config['org_name'] = org_name

        return driver_config, agent_config

    def start_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Agent instance must first be created and associated with a instrument device
        Launch the instument agent instance and return the id
        """
        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        #if there is an agent pid then assume that a drive is already started
        if instrument_agent_instance_obj.agent_process_id:
            raise BadRequest("Instrument Agent Instance already running for this device pid: %s" %
                             str(instrument_agent_instance_obj.agent_process_id))

        # validate the associations, then pick things up
        self._validate_instrument_agent_instance(instrument_agent_instance_obj)
        instrument_device_id = self.instrument_device.find_having_agent_instance(instrument_agent_instance_id)[0]._id
        instrument_model_id  = self.instrument_device.find_stemming_model(instrument_device_id)[0]._id
        instrument_agent_id  = self.instrument_agent.find_having_model(instrument_model_id)[0]._id

        instrument_agent_obj = self.clients.resource_registry.read(instrument_agent_id)
        self._validate_instrument_agent(instrument_agent_obj)

        #retrieve the associated process definition
        process_def_ids, _ = self.clients.resource_registry.find_objects(instrument_agent_id,
                                                                         PRED.hasProcessDefinition,
                                                                         RT.ProcessDefinition,
                                                                         True)
        if 1 != len(process_def_ids):
            raise BadRequest("Expected 1 ProcessDefinition attached to InstrumentAgent '%s', got %d" %
                           (str(instrument_agent_id), len(process_def_ids)))


        process_definition_id = process_def_ids[0]

        # retrieve the process definition information
        process_def_obj = self.clients.resource_registry.read(process_definition_id)
        if not process_def_obj:
            raise NotFound("ProcessDefinition %s does not exist" % process_definition_id)

        # if no comms_config specified in the driver config
        if not 'comms_config' in instrument_agent_instance_obj.driver_config:
            self._start_pagent(instrument_agent_instance_id) # <-- this updates agent instance obj!
        # if the comms_config host addr in the driver config is localhost
        elif 'addr' in instrument_agent_instance_obj.driver_config.get('comms_config') and \
             instrument_agent_instance_obj.driver_config['comms_config']['addr'] == 'localhost':
                log.debug("IMS:start_instrument_agent_instance comms_server_address: %s", str(instrument_agent_instance_obj.driver_config['comms_config']['addr']) )
                self._start_pagent(instrument_agent_instance_id) # <-- this updates agent instance obj!

        instrument_agent_instance_obj = self.read_instrument_agent_instance(instrument_agent_instance_id)

        driver_config, agent_config = self._generate_instrument_agent_config(instrument_device_id)

        instrument_agent_instance_obj.driver_config = driver_config

        process_id = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                                      schedule=None,
                                                                      configuration=agent_config)
        #update the producer context for provenance
        #todo: should get the time from process dispatcher
        producer_obj = self._get_instrument_producer(instrument_device_id)
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.activation_time =  IonTime().to_string()
            producer_obj.producer_context.configuration = agent_config
            # get the site where this device is currently deploy instrument_device_id
            site_objs = self.instrument_site.find_having_device(instrument_device_id)

            if len(site_objs) == 1:
                producer_obj.producer_context.deployed_site_id = site_objs[0]._id


            self.clients.resource_registry.update(producer_obj)

        # add the process id and update the resource
        instrument_agent_instance_obj.agent_config = agent_config
        instrument_agent_instance_obj.agent_process_id = process_id

        self.update_instrument_agent_instance(instrument_agent_instance_obj)

    def _start_pagent(self, instrument_agent_instance_id=None):
        """
        Construct and start the port agent.
        """
        instrument_agent_instance_obj = self.read_instrument_agent_instance(instrument_agent_instance_id)

        _port_agent_config = instrument_agent_instance_obj.port_agent_config
       
        #todo: ask bill if this blocks
        # It blocks until the port agent starts up or a timeout
        _pagent = PortAgentProcess.launch_process(_port_agent_config,  test_mode = True)
        pid = _pagent.get_pid()
        port = _pagent.get_data_port()

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
            'port' : port
        }
        instrument_agent_instance_obj.driver_config['pagent_pid'] = pid
        self.update_instrument_agent_instance(instrument_agent_instance_obj)


    def stop_instrument_agent_instance(self, instrument_agent_instance_id=''):
        """
        Deactivate the instrument agent instance
        """
        instrument_agent_instance_obj = self.clients.resource_registry.read(instrument_agent_instance_id)

        instrument_device_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.InstrumentDevice,
                                                                                predicate=PRED.hasAgentInstance,
                                                                                object=instrument_agent_instance_id,
                                                                                id_only=True)
        if not instrument_device_ids:
            raise NotFound("No Instrument Device resource associated with this Instrument Agent Instance: %s",
                           str(instrument_agent_instance_id) )

        # Cancels the execution of the given process id.
        if None is instrument_agent_instance_obj.agent_process_id:
            raise BadRequest("Instrument Agent Instance '%s' does not have an agent_process_id.  Stopped already?"
                                % instrument_agent_instance_id)
        try:
            self.clients.process_dispatcher.cancel_process(process_id=instrument_agent_instance_obj.agent_process_id)
        except NotFound:
            pass
        except Exception as e:
            raise e

        try:
            _port_agent_config = instrument_agent_instance_obj.port_agent_config

            process = PortAgentProcess.get_process(_port_agent_config, test_mode=True)
            process.stop()
        except NotFound:
            pass
        except Exception as e:
            raise e

        #reset the process ids.
        instrument_agent_instance_obj.agent_process_id = None
        instrument_agent_instance_obj.driver_config['pagent_pid'] = None
        self.clients.resource_registry.update(instrument_agent_instance_obj)

        #update the producer context for provenance
        producer_obj = self._get_instrument_producer(instrument_device_ids[0])
        if producer_obj.producer_context.type_ == OT.InstrumentProducerContext :
            producer_obj.producer_context.deactivation_time =  IonTime().to_string()
            self.clients.resource_registry.update(producer_obj)

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
        process_definition.executable['module']='ion.agents.instrument.instrument_agent'
        process_definition.executable['class'] = 'InstrumentAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

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
        process_def_objs = self.instrument_agent.find_stemming_process_definition(instrument_agent_id)

        for pd_obj in process_def_objs:
            self.instrument_agent.unlink_process_definition(instrument_agent_id, pd_obj._id)
            self.clients.process_dispatcher.delete_process_definition(pd_obj._id)

        self.instrument_agent.delete_one(instrument_agent_id)

    def force_delete_instrument_agent(self, instrument_agent_id=''):
        self.instrument_agent.force_delete_one(instrument_agent_id)


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
        self.instrument_agent.read_one(instrument_agent_id)

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
            self.clients.resource_registry.create_attachment(instrument_agent_id, att)

        #updates the state of this InstAgent to integrated
        self.instrument_agent.advance_lcs(instrument_agent_id, LCE.INTEGRATE)

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
        self.instrument_model.delete_one(instrument_model_id)

    def force_delete_instrument_model(self, instrument_model_id=''):
        self.instrument_model.force_delete_one(instrument_model_id)



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
        self.instrument_device.delete_one(instrument_device_id)


    def force_delete_instrument_device(self, instrument_device_id=''):
        self.instrument_device.force_delete_one(instrument_device_id)

    ##
    ##
    ##  DIRECT ACCESS
    ##
    ##

    def check_exclusive_commitment(self, msg,  headers):
        """
        This function is used for governance validation for the request_direct_access and stop_direct_access operation.
        """

        user_id = headers['ion-actor-id']
        resource_id = msg['instrument_device_id']

        commitment =  self.container.governance_controller.get_resource_commitment(user_id, resource_id)

        if commitment is None:
            return False, '(execute_resource) has been denied since the user %s has not acquired the resource %s' % (user_id, resource_id)

        #Look for any active commitments that are exclusive - and only allow for exclusive commitment
        if not commitment.commitment.exclusive:
            return False, 'Direct Access Mode has been denied since the user %s has not acquired the resource %s exclusively' % (user_id, resource_id)

        return True, ''

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



    def _get_instrument_producer(self, instrument_device_id=""):
        producer_objs, _ = self.clients.resource_registry.find_objects(subject=instrument_device_id, predicate=PRED.hasDataProducer, object_type=RT.DataProducer, id_only=False)
        if not producer_objs:
            raise NotFound("No Producers created for this Instrument Device " + str(instrument_device_id))
        return producer_objs[0]


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
        platform_agent_instance_id = self.platform_agent_instance.create_one(platform_agent_instance)

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
        self.platform_agent_instance.delete_one(platform_agent_instance_id)

    def force_delete_platform_agent_instance(self, platform_agent_instance_id=''):
        self.platform_agent_instance.force_delete_one(platform_agent_instance_id)

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
        platform_agent_instance_obj = self.clients.resource_registry.read(platform_agent_instance_id)

        #if there is a agent pid then assume that a drive is already started
        if platform_agent_instance_obj.agent_process_id:
            raise BadRequest("Platform Agent Instance already running for this device pid: %s" %
                             str(platform_agent_instance_obj.agent_process_id))

        #retrieve the associated platform device
        platform_device_objs, _  = self.clients.resource_registry.find_subjects(subject_type=RT.PlatformDevice, predicate=PRED.hasAgentInstance, object=platform_agent_instance_id, id_only=False )
        if 1 != len(platform_device_objs):
            raise BadRequest("Expected 1 PlatformDevice attached to  PlatformAgentInstance '%s', got %d" %
                             (str(platform_agent_instance_id), len(platform_device_objs)))
        platform_device_id = platform_device_objs[0]._id
        log.debug("start_platform_agent_instance: device is %s connected to platform agent instance %s (L4-CI-SA-RQ-363)", str(platform_device_id),  str(platform_agent_instance_id))

        #retrieve the platform model
        platform_models_objs, _  = self.clients.resource_registry.find_objects(subject=platform_device_id, predicate=PRED.hasModel, object_type=RT.PlatformModel, id_only=False )
        if 1 != len(platform_models_objs):
            raise BadRequest("Expected 1 PlatformDevice attached to  PlatformAgentInstance '%s', got %d" %
                             (str(platform_device_id), len(platform_models_objs)))
        platform_model_id = platform_models_objs[0]

        #retrive the stream info for this model
        #todo: add stream info to the platofrom model create
#        streams_dict = platform_models_objs[0].custom_attributes['streams']
#        if not streams_dict:
#            raise BadRequest("Device model does not contain stream configuation used in launching the agent. Model: '%s", str(platform_models_objs[0]) )

        #retrieve the associated platform agent
        platform_agent_objs, _  = self.clients.resource_registry.find_objects(subject=platform_agent_instance_id, predicate=PRED.hasAgentDefinition, object_type=RT.PlatformAgent, id_only=False )
        if 1 != len(platform_agent_objs):
            raise BadRequest("Expected 1 InstrumentAgent attached to InstrumentAgentInstance '%s', got %d" %
                           (str(platform_agent_instance_id), len(platform_agent_objs)))
        platform_agent_id = platform_agent_objs[0]._id

        #retrieve the associated process definition
        #todo: this association is not in the diagram... is it ok?
        process_def_ids, _ = self.clients.resource_registry.find_objects(platform_agent_id,
                                                                         PRED.hasProcessDefinition,
                                                                         RT.ProcessDefinition,
                                                                         True)
        if 1 != len(process_def_ids):
            raise BadRequest("Expected 1 ProcessDefinition attached to PlatformAgent '%s', got %d" %
                           (str(platform_agent_id), len(process_def_ids)))


        process_definition_id = process_def_ids[0]

        # retrieve the process definition information
        process_def_obj = self.clients.resource_registry.read(process_definition_id)
        if not process_def_obj:
            raise NotFound("ProcessDefinition %s does not exist" % process_definition_id)


        # complement agent_config with resource_id
        agent_config = platform_agent_instance_obj.agent_config
        if 'agent' not in agent_config:
            agent_config['agent'] = {'resource_id': platform_device_id}
        elif 'resource_id' not in agent_config.get('agent'):
            agent_config['agent']['resource_id'] = platform_device_id

        # TODO: for platform_id in children in topology:
        platform_id = agent_config['platform_config']['platform_id']
        stream_config = self._generate_platform_streamconfig( platform_id, platform_device_id )
        agent_config['platform_config']['agent_streamconfig_map'] = { platform_id: stream_config }

        #retrieve the Org name to which this agent instance belongs
        org_name = ''
        org_objs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=platform_agent_instance_id)
        if org_objs:
            org_name = org_objs[0].name
        agent_config['org_name'] = org_name


#        import pprint
#        print '============== config within IMS for platform ID: %s ===========' % platform_id
#        pprint.pprint(agent_config)

        process_id = self.clients.process_dispatcher.schedule_process(process_definition_id=process_definition_id,
                                                               schedule=None,
                                                               configuration=agent_config)
        #update the producer context for provenance
        #todo: should get the time from process dispatcher


        # add the process id and update the resource
        platform_agent_instance_obj.agent_config = agent_config
        platform_agent_instance_obj.agent_process_id = process_id
        self.update_instrument_agent_instance(platform_agent_instance_obj)
        log.debug('completed platform agent start, platform id: %s', platform_id)
        return process_id

    def stop_platform_agent_instance(self, platform_agent_instance_id=''):

        """
        Deactivate the platform agent instance
        """
        platform_agent_instance_obj = self.clients.resource_registry.read(platform_agent_instance_id)

        platform_device_ids, _ = self.clients.resource_registry.find_subjects(subject_type=RT.PlatformDevice, predicate=PRED.hasAgentInstance,
                                                                          object=platform_agent_instance_id, id_only=True)
        if not platform_device_ids:
            raise NotFound("No Platform Device resource associated with this Platform Agent Instance: %s", str(platform_agent_instance_id) )

        # Cancels the execution of the given process id.
        self.clients.process_dispatcher.cancel_process(platform_agent_instance_obj.agent_process_id)


        #reset the process ids.
        platform_agent_instance_obj.agent_process_id = None
        self.clients.resource_registry.update(platform_agent_instance_obj)


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

        platform_agent_id = self.platform_agent.create_one(platform_agent)

        # Create the process definition to launch the agent
        process_definition = ProcessDefinition()
        process_definition.executable['module']='ion.agents.platform.platform_agent'
        process_definition.executable['class'] = 'PlatformAgent'
        process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

        #associate the agent and the process def
        self.clients.resource_registry.create_association(platform_agent_id,  PRED.hasProcessDefinition, process_definition_id)

        return platform_agent_id

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
        self.platform_agent.delete_one(platform_agent_id)

    def force_delete_platform_agent(self, platform_agent_id=''):
        self.platform_agent.force_delete_one(platform_agent_id)


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
        self.platform_model.delete_one(platform_model_id)

    def force_delete_platform_model(self, platform_model_id=''):
        self.platform_model.force_delete_one(platform_model_id)


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

        platform_device_id = self.platform_device.create_one(platform_device)
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
        self.platform_device.delete_one(platform_device_id)

    def force_delete_platform_device(self, platform_device_id=''):
        self.platform_device.force_delete_one(platform_device_id)




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
        self.sensor_model.delete_one(sensor_model_id)

    def force_delete_sensor_model(self, sensor_model_id=''):
        self.sensor_model.force_delete_one(sensor_model_id)


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
        self.sensor_device.delete_one(sensor_device_id)

    def force_delete_sensor_device(self, sensor_device_id=''):
        self.sensor_device.force_delete_one(sensor_device_id)



    ##########################################################################
    #
    # ASSOCIATIONS
    #
    ##########################################################################


    def assign_instrument_model_to_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        instrument_model_obj  = self.instrument_model.read_one(instrument_model_id)
        instrument_device_obj = self.instrument_device.read_one(instrument_device_id)

        for k, v in instrument_device_obj.custom_attributes.iteritems():
            if not k in instrument_model_obj.custom_attributes:
                err_msg = ("InstrumentDevice '%s' contains custom attribute '%s' (value '%s'), but this attribute"
                        + " is not defined by associated InstrumentModel '%s'") % (instrument_device_id,
                                                                                   k, v,
                                                                                   instrument_model_id)
                #raise BadRequest(err_msg)
                log.warn(err_msg)

        self.instrument_device.link_model(instrument_device_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_device(self, instrument_model_id='', instrument_device_id=''):
        self.instrument_device.unlink_model(instrument_device_id, instrument_model_id)

    def assign_instrument_model_to_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.link_model(instrument_agent_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_agent(self, instrument_model_id='', instrument_agent_id=''):
        self.instrument_agent.unlink_model(instrument_agent_id, instrument_model_id)

    def assign_platform_model_to_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.platform_agent.link_model(platform_agent_id, platform_model_id)

    def unassign_platform_model_from_platform_agent(self, platform_model_id='', platform_agent_id=''):
        self.platform_agent.unlink_model(platform_agent_id, platform_model_id)

    def assign_sensor_model_to_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.link_model(sensor_device_id, sensor_model_id)

    def unassign_sensor_model_from_sensor_device(self, sensor_model_id='', sensor_device_id=''):
        self.sensor_device.unlink_model(sensor_device_id, sensor_model_id)

    def assign_platform_model_to_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.link_model(platform_device_id, platform_model_id)

    def unassign_platform_model_from_platform_device(self, platform_model_id='', platform_device_id=''):
        self.platform_device.unlink_model(platform_device_id, platform_model_id)

    def assign_instrument_device_to_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.link_instrument_device(platform_device_id, instrument_device_id)

    def unassign_instrument_device_from_platform_device(self, instrument_device_id='', platform_device_id=''):
        self.platform_device.unlink_instrument_device(platform_device_id, instrument_device_id)

    def assign_platform_device_to_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.platform_device.link_platform_device(platform_device_id, child_platform_device_id)

    def unassign_platform_device_from_platform_device(self, child_platform_device_id='', platform_device_id=''):
        self.platform_device.unlink_platform_device(platform_device_id, child_platform_device_id)

    def assign_platform_agent_to_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.platform_agent_instance.link_agent_definition(platform_agent_instance_id, platform_agent_id)

    def unassign_platform_agent_from_platform_agent_instance(self, platform_agent_id='', platform_agent_instance_id=''):
        self.platform_agent_instance.unlink_agent_definition(platform_agent_instance_id, platform_agent_id)

    def assign_instrument_agent_to_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.instrument_agent_instance.link_agent_definition(instrument_agent_instance_id, instrument_agent_id)

    def unassign_instrument_agent_from_instrument_agent_instance(self, instrument_agent_id='', instrument_agent_instance_id=''):
        self.instrument_agent_instance.unlink_agent_definition(instrument_agent_instance_id, instrument_agent_id)

    def assign_instrument_agent_instance_to_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_device.link_agent_instance(instrument_device_id, instrument_agent_instance_id)

    def unassign_instrument_agent_instance_from_instrument_device(self, instrument_agent_instance_id='', instrument_device_id=''):
        self.instrument_device.unlink_agent_instance(instrument_device_id, instrument_agent_instance_id)

    def assign_platform_agent_instance_to_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.platform_device.link_agent_instance(platform_device_id, platform_agent_instance_id)

    def unassign_platform_agent_instance_from_platform_device(self, platform_agent_instance_id='', platform_device_id=''):
        self.platform_device.unlink_agent_instance(platform_device_id, platform_agent_instance_id)

    def assign_sensor_device_to_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.instrument_device.link_device(instrument_device_id, sensor_device_id)

    def unassign_sensor_device_from_instrument_device(self, sensor_device_id='', instrument_device_id=''):
        self.instrument_device.unlink_device(instrument_device_id, sensor_device_id)


    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################



    def deploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        self.instrument_device.link_deployment(instrument_device_id, deployment_id)

    def undeploy_instrument_device(self, instrument_device_id='', deployment_id=''):
        self.instrument_device.unlink_deployment(instrument_device_id, deployment_id)

    def deploy_platform_device(self, platform_device_id='', deployment_id=''):
        self.platform_device.link_deployment(platform_device_id, deployment_id)

    def undeploy_platform_device(self, platform_device_id='', deployment_id=''):
        self.platform_device.unlink_deployment(platform_device_id, deployment_id)




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

    def find_instrument_device_by_instrument_agent_instance(self, instrument_agent_instance_id=''):
        return self.instrument_device.find_having_agent_instance(instrument_agent_instance_id)

    def find_instrument_agent_instance_by_instrument_device(self, instrument_device_id=''):
        return self.instrument_device.find_stemming_agent_instance(instrument_device_id)

    def find_instrument_device_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_instrument_device(platform_device_id)

    def find_platform_device_by_instrument_device(self, instrument_device_id=''):
        return self.platform_device.find_having_instrument_device(instrument_device_id)

    def find_child_platform_device_by_platform_device(self, platform_device_id=''):
        return self.platform_device.find_stemming_platform_device(platform_device_id)

    def find_platform_device_by_child_platform_device(self, instrument_device_id=''):
        return self.platform_device.find_having_platform_device(instrument_device_id)

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
       return self.instrument_agent.advance_lcs(instrument_agent_id, lifecycle_event)

    def execute_instrument_agent_instance_lifecycle(self, instrument_agent_instance_id="", lifecycle_event=""):
       """
       declare a instrument_agent_instance to be in a given state
       @param instrument_agent_instance_id the resource id
       """
       return self.instrument_agent_instance.advance_lcs(instrument_agent_instance_id, lifecycle_event)

    def execute_instrument_model_lifecycle(self, instrument_model_id="", lifecycle_event=""):
       """
       declare a instrument_model to be in a given state
       @param instrument_model_id the resource id
       """
       return self.instrument_model.advance_lcs(instrument_model_id, lifecycle_event)

    def execute_instrument_device_lifecycle(self, instrument_device_id="", lifecycle_event=""):
       """
       declare an instrument_device to be in a given state
       @param instrument_device_id the resource id
       """
       return self.instrument_device.advance_lcs(instrument_device_id, lifecycle_event)

    def execute_platform_agent_lifecycle(self, platform_agent_id="", lifecycle_event=""):
       """
       declare a platform_agent to be in a given state
       @param platform_agent_id the resource id
       """
       return self.platform_agent.advance_lcs(platform_agent_id, lifecycle_event)

    def execute_platform_agent_instance_lifecycle(self, platform_agent_instance_id="", lifecycle_event=""):
       """
       declare a platform_agent_instance to be in a given state
       @param platform_agent_instance_id the resource id
       """
       return self.platform_agent_instance.advance_lcs(platform_agent_instance_id, lifecycle_event)

    def execute_platform_model_lifecycle(self, platform_model_id="", lifecycle_event=""):
       """
       declare a platform_model to be in a given state
       @param platform_model_id the resource id
       """
       return self.platform_model.advance_lcs(platform_model_id, lifecycle_event)

    def execute_platform_device_lifecycle(self, platform_device_id="", lifecycle_event=""):
       """
       declare a platform_device to be in a given state
       @param platform_device_id the resource id
       """
       return self.platform_device.advance_lcs(platform_device_id, lifecycle_event)

    def execute_sensor_model_lifecycle(self, sensor_model_id="", lifecycle_event=""):
       """
       declare a sensor_model to be in a given state
       @param sensor_model_id the resource id
       """
       return self.sensor_model.advance_lcs(sensor_model_id, lifecycle_event)

    def execute_sensor_device_lifecycle(self, sensor_device_id="", lifecycle_event=""):
       """
       declare a sensor_device to be in a given state
       @param sensor_device_id the resource id
       """
       return self.sensor_device.advance_lcs(sensor_device_id, lifecycle_event)




    ############################
    #
    #  EXTENDED RESOURCES
    #
    ############################


    def get_instrument_device_extension(self, instrument_device_id='', ext_associations=None, ext_exclude=None):
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
            ext_exclude=ext_exclude)

        # clean up InstAgent list as it sometimes includes the device
        ia = []
        for agent in extended_instrument.instrument_agent:
            if agent.type_ == 'InstrumentAgent':
                ia.append(agent)
        extended_instrument.instrument_agent = ia

        # Status computation
        status_rollups = self.outil.get_status_roll_ups(instrument_device_id, RT.InstrumentDevice)

        extended_instrument.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[instrument_device_id]["comms"])
        extended_instrument.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[instrument_device_id]["power"])
        extended_instrument.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[instrument_device_id]["data"])
        extended_instrument.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[instrument_device_id]["loc"])
        extended_instrument.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[instrument_device_id]["agg"])

        return extended_instrument


    # TODO: this causes a problem because an instrument agent must be running in order to look up extended attributes.
    def obtain_agent_handle(self, device_id):
        ia_client = ResourceAgentClient(device_id,  process=self)


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


    # def get_uptime(self, device_id): - common to both instrument and platform, see below


    #functions for INSTRUMENT computed attributes -- currently bogus values returned

    def get_platform_device_extension(self, platform_device_id='', ext_associations=None, ext_exclude=None):
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
            ext_exclude=ext_exclude)

        # Status computation
        extended_platform.computed.instrument_status = [4]*len(extended_platform.instrument_devices)
        extended_platform.computed.platform_status = [4]*len(extended_platform.platforms)

        extended_platform.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_platform.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_platform.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_platform.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)
        extended_platform.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=4)

        try:
            status_rollups = self.outil.get_status_roll_ups(platform_device_id, RT.PlatformDevice)

            extended_platform.computed.instrument_status = [status_rollups.get(idev._id,{}).get("agg",4) for idev in extended_platform.instrument_devices]
            extended_platform.computed.platform_status = [status_rollups(pdev._id,{}).get("agg",4) for pdev in extended_platform.platforms]

            extended_platform.computed.communications_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[platform_device_id]["comms"])
            extended_platform.computed.power_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[platform_device_id]["power"])
            extended_platform.computed.data_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[platform_device_id]["data"])
            extended_platform.computed.location_status_roll_up = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[platform_device_id]["loc"])
            extended_platform.computed.aggregated_status = ComputedIntValue(status=ComputedValueAvailability.PROVIDED, value=status_rollups[platform_device_id]["agg"])
        except Exception as ex:
            log.exception("Computed attribute failed for %s" % platform_device_id)

        return extended_platform

    # The actual initiation of the deployment, calculated from when the deployment was activated
    def get_uptime(self, device_id):
        #used by both instrument device, platform device
#        ia_client, ret = self.obtain_agent_calculation(instrument_device_id, OT.ComputedFloatValue)
#        if ia_client:
#            ret.value = 45.5 #todo: use ia_client
#        return ret
        return "0 days, 0 hours, 0 minutes"

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
