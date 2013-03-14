#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.agent_ConfigurationBuilder
@author   Ian Katz
"""

import tempfile
from ion.agents.instrument.driver_process import DriverProcessType
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.core.exception import NotFound, BadRequest
from pyon.ion.resource import PRED, RT

from ooi.logging import log


class AgentConfigurationBuilderFactory(object):

    def __init__(self, clients):
        self.clients = clients

    def create_by_device_type(self, device_type):
        if RT.InstrumentDevice == device_type:
            return InstrumentAgentConfigurationBuilder(self.clients)
        elif RT.PlatformDevice == device_type:
            return PlatformAgentConfigurationBuilder(self.clients)

    def create_by_agent_instance_type(self, instance_type):
        if RT.InstrumentAgentInstance == instance_type:
            return InstrumentAgentConfigurationBuilder(self.clients)
        elif RT.PlatformAgentInstance == instance_type:
            return PlatformAgentConfigurationBuilder(self.clients)


class AgentConfigurationBuilder(object):

    def __init__(self, clients):
        self.clients = clients
        self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)
        self.agent_instance_obj = None
        self.associated_objects = None
        self.last_id            = None
        self.will_launch        = False

    def _lookup_means(self):
        """
        return a dict indicating how various related resources will be looked up

        The dict is keyed on association type:
        PRED.hasAgentInstance -> device type
        PRED.hasModel -> model type
        PRED.hasAgentDefinition -> agent type
        """
        raise NotImplementedError("Extender of class must implement this")


    def _check_associations(self):
        assert self.agent_instance_obj
        assert self.associated_objects

        lookup_means = self._lookup_means()
        assert lookup_means

        # make sure we've picked up the associations we expect
        def check_keys(somekeys):
            for k in somekeys:
                assert k in lookup_means
                assert lookup_means[k] in self.associated_objects

        #check_keys([PRED.hasAgentInstance, PRED.hasModel, PRED.hasAgentDefinition])
        check_keys([PRED.hasAgentInstance, PRED.hasAgentDefinition])
        assert RT.ProcessDefinition in self.associated_objects


    def set_agent_instance_object(self, agent_instance_obj):
        """
        Set the agent instance object that we'll be interacting with

        it may be necessary to set this several times, such as if external operations update the object
        """
        assert agent_instance_obj._id

        if self.last_id != agent_instance_obj._id:
            self.associated_objects = None

        self.agent_instance_obj = agent_instance_obj
        self.last_id = agent_instance_obj._id


    def prepare(self, will_launch=True):
        """
        Prepare (validate) an agent for launch, fetching all associated resources

        @param will_launch - whether the running status should be checked -- set false if just generating config
        """
        assert self.agent_instance_obj

        if will_launch:
            #if there is an agent pid then assume that a drive is already started
            if self.agent_instance_obj.agent_process_id:
                raise BadRequest("Agent Instance already running for this device pid: %s" %
                                 str(self.agent_instance_obj.agent_process_id))

        # validate the associations, then pick things up
        self._collect_agent_instance_associations()
        self.will_launch = will_launch
        return self.generate_config()


    def _generate_org_name(self):
        log.debug("retrieve the Org name to which this agent instance belongs")
        try:
            org_obj = self.RR2.find_subject(RT.Org, PRED.hasResource, self.agent_instance_obj._id, id_only=False)
            return org_obj.name
        except NotFound:
            return ''
        except:
            raise

    def _generate_device_type(self):
        return type(self._get_device()).__name__

    def _generate_driver_config(self):
        return self.agent_instance_obj.driver_config

    def _generate_stream_config(self):
        dsm = self.clients.dataset_management
        psm = self.clients.pubsub_management

        agent_obj  = self._get_agent()
        device_obj = self._get_device()

        streams_dict = {}
        for stream_cfg in agent_obj.stream_configurations:
            #create a stream def for each param dict to match against the existing data products
            param_dict_id = dsm.read_parameter_dictionary_by_name(stream_cfg.parameter_dictionary_name,
                                                                  id_only=True)
            stream_def_id = psm.create_stream_definition(parameter_dictionary_id=param_dict_id)
            streams_dict[stream_cfg.stream_name] = {'param_dict_name':stream_cfg.parameter_dictionary_name,
                                                    'stream_def_id':stream_def_id,
                                                    'records_per_granule': stream_cfg.records_per_granule,
                                                    'granule_publish_rate':stream_cfg.granule_publish_rate,
                                                    'alarms'              :stream_cfg.alarms  }

        #retrieve the output products
        device_id = device_obj._id
        data_product_ids = self.RR2.find_data_product_ids_of_instrument_device_using_has_output_product(device_id)

        out_streams = []
        for product_id in data_product_ids:
            stream_id = self.RR2.find_stream_id_of_data_product(product_id)
            out_streams.append(stream_id)


        stream_config = {}

        log.debug("Creating a stream config got each stream (dataproduct) assoc with this agent/device")
        for product_stream_id in out_streams:

            #get the streamroute object from pubsub by passing the stream_id
            stream_def_id = self.RR2.find_stream_definition_id_of_stream(product_stream_id)

            #match the streamdefs/apram dict for this model with the data products attached to this device to know which tag to use
            for model_stream_name, stream_info_dict  in streams_dict.items():

                if self.clients.pubsub_management.compare_stream_definition(stream_info_dict.get('stream_def_id'),
                                                                            stream_def_id):
                    model_param_dict = DatasetManagementService.get_parameter_dictionary_by_name(stream_info_dict.get('param_dict_name'))
                    stream_route = self.clients.pubsub_management.read_stream_route(stream_id=product_stream_id)

                    stream_config[model_stream_name] = {'routing_key'           : stream_route.routing_key,
                                                            'stream_id'             : product_stream_id,
                                                            'stream_definition_ref' : stream_def_id,
                                                            'exchange_point'        : stream_route.exchange_point,
                                                            'parameter_dictionary'  : model_param_dict.dump(),
                                                            'records_per_granule'  : stream_info_dict.get('records_per_granule'),
                                                            'granule_publish_rate'  : stream_info_dict.get('granule_publish_rate'),
                                                            'alarms'                : stream_info_dict.get('alarms')
                    }

        log.debug("Stream config generated")
        log.trace("generate_stream_config: %s", str(stream_config) )
        return stream_config

    def _generate_agent_config(self):
        # should override this
        return {}

    def _generate_alarms_config(self):
        # should override this
        return {}

    def _generate_startup_config(self):
        # should override this
        return {}

    def _generate_children(self):
        # should override this
        return {}

    def _generate_skeleton_config_block(self):
        # should override this
        agent_config = self.agent_instance_obj.agent_config

        # Create agent_ config.
        agent_config['org_name']       = self._generate_org_name()
        agent_config['device_type']    = self._generate_device_type()
        agent_config['driver_config']  = self._generate_driver_config()
        agent_config['stream_config']  = self._generate_stream_config()
        agent_config['agent']          = self._generate_agent_config()
        agent_config['alarm_defs']     = self._generate_alarms_config()
        agent_config['startup_config'] = self._generate_startup_config()
        agent_config['children']       = self._generate_children()

        return agent_config


    def generate_config(self):
        """
        create the generic parts of the configuration including resource_id, egg_uri, and org
        """
        self._check_associations()

        agent_config = self._generate_skeleton_config_block()

        device_obj = self._get_device()
        agent_obj  = self._get_agent()

        log.debug("complement agent_config with resource_id")
        if 'agent' not in agent_config:
            agent_config['agent'] = {'resource_id': device_obj._id}
        elif 'resource_id' not in agent_config.get('agent'):
            agent_config['agent']['resource_id'] = device_obj._id


        log.debug("add egg URI if available")
        if agent_obj.driver_uri:
            agent_config['driver_config']['process_type'] = (DriverProcessType.EGG,)
            agent_config['driver_config']['dvr_egg'] = agent_obj.driver_uri
        else:
            agent_config['driver_config']['process_type'] = (DriverProcessType.PYTHON_MODULE,)

        return agent_config



    def record_launch_parameters(self, agent_config, process_id):
        """
        record process id of the launch
        """

        log.debug("add the process id and update the resource")
        self.agent_instance_obj.agent_config = agent_config
        self.agent_instance_obj.agent_process_id = process_id
        self.RR2.update(self.agent_instance_obj)

        log.debug('completed agent start')

        return process_id



    def _collect_agent_instance_associations(self):
        """
        Collect related resources to this agent instance

        Returns a dict of objects necessary to start this instance, keyed on the values of self._lookup_means()
            PRED.hasAgentInstance   -> device_obj
            PRED.hasModel           -> model_obj
            PRED.hasAgentDefinition -> agent_obj
            RT.ProcessDefinition    -> process_def_obj

        """
        assert self.agent_instance_obj

        lookup_means = self._lookup_means()

        assert lookup_means
        assert PRED.hasAgentInstance in lookup_means
        assert PRED.hasModel in lookup_means
        assert PRED.hasAgentDefinition in lookup_means
        #assert PRED.hasProcessDefinition in lookup_means

        lu = lookup_means

        ret = {}

        log.debug("retrieve the associated device")
        device_obj = self.RR2.find_subject(subject_type=lu[PRED.hasAgentInstance],
                                           predicate=PRED.hasAgentInstance,
                                           object=self.agent_instance_obj._id)

        ret[lu[PRED.hasAgentInstance]]= device_obj
        device_id = device_obj._id

        log.debug("%s '%s' connected to %s '%s' (L4-CI-SA-RQ-363)",
                  lu[PRED.hasAgentInstance],
                  str(device_id),
                  type(self.agent_instance_obj).__name__,
                  str(self.agent_instance_obj._id))

#        log.debug("retrieve the model associated with the device")
#        model_obj = self.RR2.find_object(subject=device_id,
#                                         predicate=PRED.hasModel,
#                                         object_type=lu[PRED.hasModel])
#
#        ret[lu[PRED.hasModel]] = model_obj
#        model_id = model_obj

        #retrive the stream info for this model
        #todo: add stream info to the platofrom model create
        #        streams_dict = platform_models_objs[0].custom_attributes['streams']
        #        if not streams_dict:
        #            raise BadRequest("Device model does not contain stream configuation used in launching the agent. Model: '%s", str(platform_models_objs[0]) )
        #TODO: get the agent from the instance not from the model!!!!!!!
        log.debug("retrieve the agent associated with the model")
        agent_obj = self.RR2.find_object(subject=self.agent_instance_obj._id,
                                         predicate=PRED.hasAgentDefinition,
                                         object_type=lu[PRED.hasAgentDefinition])

        ret[lu[PRED.hasAgentDefinition]] = agent_obj
        agent_id = agent_obj._id

        if not agent_obj.stream_configurations:
            raise BadRequest("Agent '%s' does not contain stream configuration used in launching" %
                             str(agent_obj) )

        log.debug("retrieve the process definition associated with this agent")
        process_def_obj = self.RR2.find_object(subject=agent_id,
                                               predicate=PRED.hasProcessDefinition,
                                               object_type=RT.ProcessDefinition)


        ret[RT.ProcessDefinition] = process_def_obj

        #retrieve the output products
        data_product_ids, _ = self.RR2.find_objects(device_id, PRED.hasOutputProduct, RT.DataProduct, id_only=True)

        if not data_product_ids:
            raise NotFound("No output Data Products attached to this Device " + str(device_id))

        #retrieve the streams assoc with each defined output product
        for product_id in data_product_ids:
            self.RR2.find_stream_id_of_data_product(product_id)  # check one stream per product
            self.RR2.find_dataset_id_of_data_product(product_id) # check one dataset per product

        self.associated_objects = ret


    def _get_device(self):
        self._check_associations()
        return self.associated_objects[self._lookup_means()[PRED.hasAgentInstance]]

#    def _get_model(self):
#        self._check_associations()
#        return self.associated_objects[self._lookup_means()[PRED.hasModel]]

    def _get_agent(self):
        self._check_associations()
        return self.associated_objects[self._lookup_means()[PRED.hasAgentDefinition]]

    def _get_process_definition(self):
        self._check_associations()
        return self.associated_objects[RT.ProcessDefinition]




class InstrumentAgentConfigurationBuilder(AgentConfigurationBuilder):

    def _lookup_means(self):
        instrument_agent_lookup_means = {}
        instrument_agent_lookup_means[PRED.hasAgentInstance]   = RT.InstrumentDevice
        instrument_agent_lookup_means[PRED.hasModel]           = RT.InstrumentModel
        instrument_agent_lookup_means[PRED.hasAgentDefinition] = RT.InstrumentAgent

        return instrument_agent_lookup_means


    def _generate_startup_config(self):
        return self.agent_instance_obj.startup_config

    def _generate_driver_config(self):
        # get default config
        driver_config = super(InstrumentAgentConfigurationBuilder, self)._generate_driver_config()

        instrument_agent_instance_obj = self.agent_instance_obj
        agent_obj = self._get_agent()

        # Create driver config.
        add_driver_config = {
            'workdir'      : tempfile.gettempdir(),
            'comms_config' : instrument_agent_instance_obj.driver_config.get('comms_config'),
            'pagent_pid'   : instrument_agent_instance_obj.driver_config.get('pagent_pid'),
            'dvr_mod'      : agent_obj.driver_module,
            'dvr_cls'      : agent_obj.driver_class
        }

        for k, v in add_driver_config.iteritems():
            if k in driver_config:
                log.warn("Overwriting Agent driver_config[%s] of '%s' with '%s'", k, driver_config[k], v)
            driver_config[k] = v

        return driver_config





class PlatformAgentConfigurationBuilder(AgentConfigurationBuilder):

    def _lookup_means(self):
        platform_agent_lookup_means = {}
        platform_agent_lookup_means[PRED.hasAgentInstance]   = RT.PlatformDevice
        platform_agent_lookup_means[PRED.hasModel]           = RT.PlatformModel
        platform_agent_lookup_means[PRED.hasAgentDefinition] = RT.PlatformAgent

        return platform_agent_lookup_means


    def _generate_children(self):
        """
        Generate the configuration for child devices
        """
        log.debug("Getting child platform device ids")
        child_pdevice_ids = self.RR2.find_platform_device_ids_of_device(self._get_device()._id)
        log.debug("found platform device ids: %s", child_pdevice_ids)

        log.debug("Getting child instrument device ids")
        child_cdevice_ids = self.RR2.find_instrument_device_ids_of_device(self._get_device()._id)
        log.debug("found instrument device ids: %s", child_cdevice_ids)

        child_device_ids = child_cdevice_ids + child_pdevice_ids

        log.debug("combined device ids: %s", child_device_ids)

        ConfigurationBuilder_factory = AgentConfigurationBuilderFactory(self.clients)

        agent_lookup_method = {
            RT.PlatformAgentInstance: self.RR2.find_platform_agent_instance_of_platform_device,
            RT.InstrumentAgentInstance: self.RR2.find_instrument_agent_instance_of_instrument_device,
            }

        # get all agent instances first. if there's no agent instance, just skip
        child_agent_instance = {}
        for ot, lookup_fn in agent_lookup_method.iteritems():
            for d in child_device_ids:
                log.debug("Getting %s of device %s", ot, d)
                try:
                    child_agent_instance[d] = lookup_fn(d)
                except NotFound:
                    log.debug("No agent instance exists; skipping")
                    pass
                except:
                    raise

        ret = {}
        for d, a in child_agent_instance.iteritems():
            agent_instance_type = type(a).__name__
            log.debug("generating %s config for device '%s'", agent_instance_type, d)
            ConfigurationBuilder = ConfigurationBuilder_factory.create_by_agent_instance_type(agent_instance_type)
            ConfigurationBuilder.set_agent_instance_object(a)
            ConfigurationBuilder.prepare(will_launch=False)
            ret[d] = ConfigurationBuilder.generate_config()

        return ret