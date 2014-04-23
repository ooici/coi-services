#!/usr/bin/env python

"""Builds spawn configurations for agent processes"""
from pyon.agent.agent import ResourceAgentClient

__author__ = 'Ian Katz, Michael Meisinger'

import copy
import tempfile
import calendar
import time

from ooi import logging
from ooi.logging import log

from pyon.core import bootstrap
from pyon.core.exception import NotFound, BadRequest
from pyon.core.object import IonObjectSerializer
from pyon.ion.resource import PRED, RT, OT
from pyon.util.containers import get_ion_ts, dict_merge

from ion.agents.instrument.driver_process import DriverProcessType
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from ion.core.ooiref import OOIReferenceDesignator


class AgentConfigurationBuilderFactory(object):

    def __init__(self, clients, RR2=None):
        self.clients = clients
        self.RR2 = RR2

    def create_by_device_type(self, device_type):
        # WARNING: This is ambiguous for ExternalDatasetAgents
        if device_type == RT.InstrumentDevice:
            return InstrumentAgentConfigurationBuilder(self.clients, self.RR2)
        elif device_type == RT.PlatformDevice:
            return PlatformAgentConfigurationBuilder(self.clients, self.RR2)

    def create_by_agent_instance_type(self, instance_type):
        if instance_type == RT.InstrumentAgentInstance:
            return InstrumentAgentConfigurationBuilder(self.clients, self.RR2)
        elif instance_type == RT.PlatformAgentInstance:
            return PlatformAgentConfigurationBuilder(self.clients, self.RR2)
        elif instance_type == RT.ExternalDatasetAgentInstance:
            return ExternalDatasetAgentConfigurationBuilder(self.clients, self.RR2)


class AgentConfigurationBuilder(object):

    def __init__(self, clients, RR2=None):
        self.clients = clients
        self.RR2 = RR2

        if self.RR2 is None:
            log.warn("Creating new RR2")
            self.RR2 = EnhancedResourceRegistryClient(self.clients.resource_registry)

        if not isinstance(self.RR2, EnhancedResourceRegistryClient):
            raise AssertionError("Type of self.RR2 is %s not %s" %
                                 (type(self.RR2), type(EnhancedResourceRegistryClient)))

        self.agent_instance_obj = None
        self.associated_objects = None
        self.last_id            = None
        self.will_launch        = False
        self.generated_config   = False

    def _predicates_to_cache(self):
        return [PRED.hasOutputProduct,
                #PRED.hasStream,
                #PRED.hasStreamDefinition,
                PRED.hasAgentInstance,
                PRED.hasAgentDefinition,
                PRED.hasDataset,
                PRED.hasDevice,
                PRED.hasNetworkParent,
                #PRED.hasParameterContext,
                PRED.hasDeployment,
                ]

    def _resources_to_cache(self):
        return [#RT.StreamDefinition,
                RT.ParameterDictionary,
                #RT.ParameterContext,
                RT.Deployment,
                ]

    def _update_cached_predicates(self):
        # cache some predicates for in-memory lookups
        preds = self._predicates_to_cache()
        log.debug("updating cached predicates: %s" % preds)
        time_caching_start = get_ion_ts()
        for pred in preds:
            log.debug(" - %s", pred)
            self.RR2.cache_predicate(pred)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.info("Cached %s predicates in %s seconds", len(preds), total_time / 1000.0)

    def _update_cached_resources(self):
        # cache some resources for in-memory lookups
        rsrcs = self._resources_to_cache()
        log.debug("updating cached resources: %s" % rsrcs)
        time_caching_start = get_ion_ts()
        for r in rsrcs:
            log.debug(" - %s", r)
            self.RR2.cache_resources(r)
        time_caching_stop = get_ion_ts()

        total_time = int(time_caching_stop) - int(time_caching_start)

        log.info("Cached %s resource types in %s seconds", len(rsrcs), total_time / 1000.0)

    def _clear_caches(self):
        log.warn("Clearing caches")
        for r in self._resources_to_cache():
            self.RR2.clear_cached_resource(r)

        for p in self._predicates_to_cache():
            self.RR2.clear_cached_predicate(p)
            

    def _lookup_means(self):
        """
        return a dict indicating how various related resources will be looked up

        The dict is keyed on association type:
        PRED.hasAgentInstance -> device type
        PRED.hasModel -> model type
        PRED.hasAgentDefinition -> agent type
        """
        raise NotImplementedError("Extender of class must implement this")

    def _augment_dict(self, title, basedict, newitems):
        # TODO: pyon.util.containers has dict_merge for this purpose (without logs)
        for k, v in newitems.iteritems():
            if k in basedict:
                prev_v = basedict[k]
                # just warn if the new value is different
                if v != prev_v:
                    log.warn("Overwriting %s[%s] of '%s' with '%s'", title, k, prev_v, v)
                else:
                    log.debug("Overwriting %s[%s] with same value already assigned '%s'",
                              title, k, v)
            basedict[k] = v

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
        self.generated_config = False

    def prepare(self, will_launch=True):
        """
        Prepare (validate) an agent for launch, fetching all associated resources

        @param will_launch - whether the running status should be checked -- set false if just generating config
        """
        assert self.agent_instance_obj

        # fetch caches just in time
        if any([not self.RR2.has_cached_predicate(x) for x in self._predicates_to_cache()]):
            self._update_cached_predicates()

        if any([not self.RR2.has_cached_resource(x) for x in self._resources_to_cache()]):
            self._update_cached_resources()

        # validate the associations, then pick things up
        self._collect_agent_instance_associations()

        if will_launch:
            # if there is an agent pid then assume that a drive is already started
            agent_process_id = ResourceAgentClient._get_agent_process_id(self._get_device()._id)
            if agent_process_id:
                raise BadRequest("Agent Instance already running for this device pid: %s" %
                                 str(agent_process_id))

        self.will_launch = will_launch
        config = self.generate_config()
        return config


    def _generate_org_governance_name(self):
        log.debug("_generate_org_governance_name for %s", self.agent_instance_obj.name)
        log.debug("retrieve the Org governance name to which this agent instance belongs")
        try:
            org_obj = self.RR2.find_subject(RT.Org, PRED.hasResource, self.agent_instance_obj._id, id_only=False)
            return org_obj.org_governance_name
        except NotFound:
            return ''
        except:
            raise

    def _generate_device_type(self):
        log.debug("_generate_device_type for %s", self.agent_instance_obj.name)
        return type(self._get_device()).__name__

    def _generate_driver_config(self):
        log.debug("_generate_driver_config for %s", self.agent_instance_obj.name)
        # get default config
        driver_config = self.agent_instance_obj.driver_config

        agent_obj = self._get_agent()

        # Create driver config.
        add_driver_config = {
            'workdir'      : tempfile.gettempdir(),
            'dvr_mod'      : agent_obj.driver_module,
            'dvr_cls'      : agent_obj.driver_class
        }

        self._augment_dict("Agent driver_config", driver_config, add_driver_config)

        return driver_config

    def _get_param_dict_by_name(self, name):
        dict_obj = self.RR2.find_resources_by_name(RT.ParameterDictionary, name)[0]
        parameter_contexts = \
            self.RR2.find_parameter_contexts_of_parameter_dictionary_using_has_parameter_context(dict_obj._id)
        return DatasetManagementService.build_parameter_dictionary(dict_obj, parameter_contexts)

    def _find_streamdef_for_dp_and_pdict(self, dp_id, pdict_id):
        # Given a pdict_id and a data_product_id find the stream def in the middle
        pdict_stream_defs = self.RR2.find_stream_definition_ids_by_parameter_dictionary_using_has_parameter_dictionary(pdict_id)
        stream_def_id = self.RR2.find_stream_definition_id_of_data_product_using_has_stream_definition(dp_id)
        result = stream_def_id if stream_def_id in pdict_stream_defs else None

        return result


    def _generate_stream_config(self):
        log.debug("_generate_stream_config for %s", self.agent_instance_obj.name)
        dsm = self.clients.dataset_management
        psm = self.clients.pubsub_management

        agent_obj  = self._get_agent()
        device_obj = self._get_device()

        streams_dict = {}
        for stream_cfg in agent_obj.stream_configurations:
            #create a stream def for each param dict to match against the existing data products
            streams_dict[stream_cfg.stream_name] = {'param_dict_name':stream_cfg.parameter_dictionary_name}

        #retrieve the output products
        # TODO: What about platforms? other things?
        device_id = device_obj._id
        data_product_objs = self.RR2.find_data_products_of_instrument_device_using_has_output_product(device_id)

        stream_config = {}
        for dp in data_product_objs:
            stream_def_id = self.RR2.find_stream_definition_id_of_data_product_using_has_stream_definition(dp._id)
            for stream in self.RR2.find_streams_of_data_product_using_has_stream(dp._id):
                if stream.stream_name and stream.stream_name in streams_dict:
                    stream_def = psm.read_stream_definition(stream_def_id)
                    #from pyon.core.object import IonObjectSerializer
                    stream_def_dict = IonObjectSerializer().serialize(stream_def)
                    stream_def_dict.pop('type_')

                    stream_config[stream.stream_name] = { 
                        'routing_key'           : stream.stream_route.routing_key,
                        'stream_id'             : stream._id,
                        'stream_definition_ref' : stream_def_id,
                        'stream_def_dict'       : stream_def_dict,
                        'exchange_point'        : stream.stream_route.exchange_point
                   }

#            stream_def_id = self.RR2.find_stream_definition_id_of_data_product_using_has_stream_definition(dp._id)
#            for stream_name, stream_info_dict in streams_dict.items():
#                # read objects from cache to be compared
#                pdict = self.RR2.find_resource_by_name(RT.ParameterDictionary, stream_info_dict.get('param_dict_name'))
#                stream_def_id = self._find_streamdef_for_dp_and_pdict(dp._id, pdict._id)
#
#                if stream_def_id:
#                    #model_param_dict = self.RR2.find_resources_by_name(RT.ParameterDictionary,
#                    #                                         stream_info_dict.get('param_dict_name'))[0]
#                    #model_param_dict = self._get_param_dict_by_name(stream_info_dict.get('param_dict_name'))
#                    #stream_route = self.RR2.read(product_stream_id).stream_route
#                    product_stream_id = self.RR2.find_stream_id_of_data_product_using_has_stream(dp._id)
#                    stream_def = psm.read_stream_definition(stream_def_id)
#                    stream_route = psm.read_stream_route(stream_id=product_stream_id)
#
#                    from pyon.core.object import IonObjectSerializer
#                    stream_def_dict = IonObjectSerializer().serialize(stream_def)
#                    stream_def_dict.pop('type_')
#
#                    if stream_name in stream_config:
#                        log.warn("Overwriting stream_config[%s]", stream_name)
#
#                    stream_config[stream_name] = {  'routing_key'           : stream_route.routing_key,  # TODO: Serialize stream_route together
#                                                    'stream_id'             : product_stream_id,
#                                                    'stream_definition_ref' : stream_def_id,
#                                                    'stream_def_dict'       : stream_def_dict,  # This is very large
#                                                    'exchange_point'        : stream_route.exchange_point,
#                                                    # This is redundant and very large - the param dict is in the stream_def_dict
#                                                    #'parameter_dictionary'  : stream_def.parameter_dictionary,
#
#                    }
        if len(stream_config) < len(streams_dict):
            log.warn("Found only %s matching streams by stream definition (%s) than %s defined in the agent (%s).",
                     len(stream_config), stream_config.keys(), len(streams_dict), streams_dict.keys())

        log.debug("Stream config generated")
        log.trace("generate_stream_config: %s", stream_config)
        return stream_config

    def _generate_agent_config(self):
        log.debug("_generate_agent_config for %s", self.agent_instance_obj.name)
        agent_config = {}

        # Set the agent state vector from the prior agent run
        if self.agent_instance_obj.saved_agent_state:
            agent_config["prior_state"] = self.agent_instance_obj.saved_agent_state

        return agent_config

    def _generate_alerts_config(self):
        log.debug("_generate_alerts_config for %s", self.agent_instance_obj.name)
        # should override this
        return self.agent_instance_obj.alerts

    def _generate_startup_config(self):
        log.debug("_generate_startup_config for %s", self.agent_instance_obj.name)
        # should override this
        return {}

    def _generate_children(self):
        log.debug("_generate_children for %s", self.agent_instance_obj.name)
        # should override this
        return {}

    def _generate_skeleton_config_block(self):
        log.info("Generating skeleton config block for %s", self.agent_instance_obj.name)

        # merge the agent config into the default config
        agent_config = dict_merge(self._get_agent().agent_default_config, self.agent_instance_obj.agent_config, True)

        # Create agent_config.
        agent_config['instance_id']        = self.agent_instance_obj._id
        agent_config['instance_name']        = self.agent_instance_obj.name
        agent_config['org_governance_name']  = self._generate_org_governance_name()
        agent_config['device_type']          = self._generate_device_type()
        agent_config['driver_config']        = self._generate_driver_config()
        agent_config['stream_config']        = self._generate_stream_config()
        agent_config['agent']                = self._generate_agent_config()
        agent_config['aparam_alerts_config'] = self._generate_alerts_config()
        agent_config['startup_config']       = self._generate_startup_config()
        agent_config['children']             = self._generate_children()

        log.info("DONE generating skeleton config block for %s", self.agent_instance_obj.name)

        return agent_config


    def _summarize_children(self, config_dict):
        ret = dict([(v['instance_name'], self._summarize_children(v))
                                for k, v in config_dict["children"].iteritems()])
        #agent_config['agent']['resource_id']
        return ret

    def generate_config(self):
        """
        create the generic parts of the configuration including resource_id, egg_uri, and org
        """
        if self.generated_config:
            log.warn("Generating config again for the same Instance object (%s)", self.agent_instance_obj.name)

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


        if log.isEnabledFor(logging.INFO):
            tree = self._summarize_children(agent_config)
            log.info("Children of %s are %s", self.agent_instance_obj.name, tree)

        self.generated_config = True

        return agent_config


    def record_launch_parameters(self, agent_config):
        """
        record process id of the launch
        """
        #self.RR2.update(self.agent_instance_obj)

        log.debug('completed agent start')

    def _collect_deployment(self, device_id=None):

        deployment_objs = self.RR2.find_objects(device_id, PRED.hasDeployment, RT.Deployment)

        # find current deployment using time constraints
        current_time =  int( calendar.timegm(time.gmtime()) )

        for d in deployment_objs:
            # find deployment start and end time
            time_constraint = None
            for constraint in d.constraint_list:
                if constraint.type_ == OT.TemporalBounds:
                    if time_constraint:
                        log.warn('deployment %s has more than one time constraint (using first)', d.name)
                    else:
                        time_constraint = constraint
            if time_constraint:
                # a time constraint was provided, check if the current time is in this window
                if int(time_constraint.start_datetime) < current_time < int(time_constraint.end_datetime) :
                    log.debug('_collect_deployment found current deployment start time: %s, end time: %s   current time:  %s    deployment: %s ',
                              time_constraint.start_datetime, time_constraint.end_datetime, current_time, d)
                    return d

        return None

    def _validate_reference_designator(self, port_assignments):
        #validate that each reference designator is valid / parseable
        # otherwise the platform cannot pull out the port number for power mgmt
        if not port_assignments:
            return

        if not isinstance(port_assignments, dict):
            log.error('Deployment for device has invalid port assignments.  device id: %s ', self._get_device()._id)
            return

        for device_id, platform_port in port_assignments.iteritems():
            if platform_port.type_ != OT.PlatformPort:
                log.error('Deployment for device has invalid port assignments for device.  device id: %s', device_id)
            ooi_rd = OOIReferenceDesignator(platform_port.reference_designator)
            if ooi_rd.error:
                log.error('Agent configuration includes a invalid reference designator for a device in this deployment.  device id: %s  reference designator: %s', device_id, platform_port.reference_designator)

        return

    def _serialize_port_assigments(self, port_assignments=None):
        serializer = IonObjectSerializer()
        serialized_port_assignments = {}
        if isinstance(port_assignments, dict):
            for device_id, platform_port in port_assignments.iteritems():
                flatpp = serializer.serialize(platform_port)
                serialized_port_assignments[device_id] = flatpp

        return serialized_port_assignments

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
        res_types = lu[PRED.hasAgentInstance]
        if not hasattr(res_types, "__iter__"):
            res_types = [res_types]

        device_obj = None
        for res_type in res_types:
            try:
                device_obj = self.RR2.find_subject(subject_type=res_type,
                                                   predicate=PRED.hasAgentInstance,
                                                   object=self.agent_instance_obj._id)
                break
            except NotFound:
                pass
        if not device_obj:
            raise NotFound("Could not find a Device for AgentInstance %s" % self.agent_instance_obj._id)

        ret[lu[PRED.hasAgentInstance]] = device_obj   # Note: can be a tuple key
        device_id = device_obj._id

        log.debug("%s '%s' connected to %s '%s' (L4-CI-SA-RQ-363)",
                  lu[PRED.hasAgentInstance],
                  str(device_id),
                  self.agent_instance_obj.type_,
                  self.agent_instance_obj._id)

#        log.debug("retrieve the model associated with the device")
#        model_obj = self.RR2.find_object(subject=device_id,
#                                         predicate=PRED.hasModel,
#                                         object_type=lu[PRED.hasModel])
#
#        ret[lu[PRED.hasModel]] = model_obj
#        model_id = model_obj

        #retrive the stream info for this model
        #todo: add stream info to the platform model create
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
        data_product_objs = self.RR2.find_objects(device_id, PRED.hasOutputProduct, RT.DataProduct, id_only=False)
        ret[RT.DataProduct] = data_product_objs

        if not data_product_objs:
            raise NotFound("No output Data Products attached to this Device " + str(device_id))

        #retrieve the streams assoc with each defined output product
        for data_product_obj in data_product_objs:
            product_id = data_product_obj._id
            try:
                self.RR2.find_stream_id_of_data_product_using_has_stream(product_id)  # check one stream per product
            except NotFound:
                errmsg = "Device '%s' (%s) has data products %s.  Data product '%s' (%s) has no stream ID." % \
                    (device_obj.name,
                     device_obj._id,
                     [dp._id for dp in data_product_objs],
                     data_product_obj.name,
                     product_id)
                raise NotFound(errmsg)

            # some products may not be persisted
            try:
                # check one dataset per product
                self.RR2.find_dataset_id_of_data_product_using_has_dataset(product_id)
            except NotFound:
                log.warn("Data product '%s' of device %s ('%s') does not appear to be persisted -- no dataset",
                         product_id, device_obj.name, device_obj._id)

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


class ExternalDatasetAgentConfigurationBuilder(AgentConfigurationBuilder):

    def _lookup_means(self):
        agent_lookup_means = {}
        agent_lookup_means[PRED.hasAgentInstance]   = (RT.InstrumentDevice, RT.PlatformDevice)
        agent_lookup_means[PRED.hasModel]           = (RT.InstrumentModel, RT.PlatformModel)
        agent_lookup_means[PRED.hasAgentDefinition] = RT.ExternalDatasetAgent

        return agent_lookup_means

    def _generate_driver_config(self):
        log.debug("_generate_driver_config for %s", self.agent_instance_obj.name)
        # get default config
        driver_config = super(ExternalDatasetAgentConfigurationBuilder, self)._generate_driver_config()

        agent_instance_obj = self.agent_instance_obj
        agent_obj = self._get_agent()

        parser_cfg = copy.deepcopy(agent_obj.parser_default_config)
        poller_cfg = copy.deepcopy(agent_obj.poller_default_config)

        # Create driver config.
        base_driver_config = {
            'parser': {
                'uri': agent_obj.parser_uri,
                'module': agent_obj.parser_module,
                'class': agent_obj.parser_class,
                'config': parser_cfg,
            },
            'poller': {
                'uri': agent_obj.poller_uri,
                'module': agent_obj.poller_module,
                'class': agent_obj.poller_class,
                'config': poller_cfg,
            },
        }

        res_driver_config = dict_merge(base_driver_config, driver_config)

        return res_driver_config

class InstrumentAgentConfigurationBuilder(AgentConfigurationBuilder):

    def _lookup_means(self):
        instrument_agent_lookup_means = {}
        instrument_agent_lookup_means[PRED.hasAgentInstance]   = RT.InstrumentDevice
        instrument_agent_lookup_means[PRED.hasModel]           = RT.InstrumentModel
        instrument_agent_lookup_means[PRED.hasAgentDefinition] = RT.InstrumentAgent

        return instrument_agent_lookup_means

    def _generate_startup_config(self):
        log.debug("_generate_startup_config for %s", self.agent_instance_obj.name)
        return self.agent_instance_obj.startup_config

    def _generate_driver_config(self):
        log.debug("_generate_driver_config for %s", self.agent_instance_obj.name)
        # get default config
        driver_config = super(InstrumentAgentConfigurationBuilder, self)._generate_driver_config()

        #add port assignments
        port_assignments = {}

        #find the associated Deployment resource for this device
        deployment_obj = self._collect_deployment(self._get_device()._id)
        if deployment_obj:
            self._validate_reference_designator(deployment_obj.port_assignments)
            port_assignments = self._serialize_port_assigments(deployment_obj.port_assignments )

        instrument_agent_instance_obj = self.agent_instance_obj

        # Create driver config.
        add_driver_config = {
            'comms_config' : instrument_agent_instance_obj.driver_config.get('comms_config'),
            'pagent_pid'   : instrument_agent_instance_obj.driver_config.get('pagent_pid'),
            'ports' : port_assignments,
        }

        self._augment_dict("Instrument Agent driver_config", driver_config, add_driver_config)

        return driver_config


class PlatformAgentConfigurationBuilder(AgentConfigurationBuilder):

    def _lookup_means(self):
        platform_agent_lookup_means = {}
        platform_agent_lookup_means[PRED.hasAgentInstance]   = RT.PlatformDevice
        platform_agent_lookup_means[PRED.hasModel]           = RT.PlatformModel
        platform_agent_lookup_means[PRED.hasAgentDefinition] = RT.PlatformAgent

        return platform_agent_lookup_means


    def _use_network_parent(self):
        """
        return True if there are any hasNewtorkParent links involved
        """
        dev_id = self._get_device()._id

        network_parents = self.RR2.find_objects(dev_id, PRED.hasNetworkParent, RT.PlatformDevice)
        if 0 < len(network_parents):
            return True

        network_children = self.RR2.find_subjects(RT.PlatformDevice, PRED.hasNetworkParent, dev_id)
        if 0 < len(network_children):
            return True

        return False

    # TODO(OOIION-1495) review the following, which was overwriting the 'ports'
    # entry with and empty dict {}, then causing failures downstream,
    # for example, with test:
    #   test_platform_launch.py:TestPlatformLaunch.test_single_deployed_platform
    #
    #   File "/Users/carueda/workspace/coi-services-carueda/ion/agents/platform/platform_agent.py", line 421, in _validate_configuration
    #     if not self._platform_id in self._network_definition.pnodes:
    # AttributeError: 'NoneType' object has no attribute 'pnodes'
    """
    def _generate_driver_config(self):
        # get default config
        driver_config = super(PlatformAgentConfigurationBuilder, self)._generate_driver_config()

        #add port assignments
        port_assignments_raw = {}

        #find the associated Deployment resource for this device
        deployment_obj = self._collect_deployment(self._get_device()._id)
        if deployment_obj:
            self._validate_reference_designator(deployment_obj.port_assignments)
            port_assignments_raw.update( deployment_obj.port_assignments)

        child_device_ids = self._build_child_list()

        #Deployment info for all children must be added to the driver_config of the platform
        for dev_id in child_device_ids:
            deployment_obj = self._collect_deployment(dev_id)
            if deployment_obj:
                self._validate_reference_designator(deployment_obj.port_assignments)
                port_assignments_raw.update(deployment_obj.port_assignments)

        port_assignments = self._serialize_port_assigments(port_assignments_raw)
        log.debug(' port assignments for platform  %s', port_assignments)

        # Create driver config.
        if 'attributes' in driver_config and driver_config['attributes']:
            add_driver_config = { 'ports' : port_assignments, }
        else:
            #attributes should be an empty dict if none are provided
            add_driver_config = { 'ports' : port_assignments, 'attributes' : {}, }

        self._augment_dict("Platform Agent driver_config", driver_config, add_driver_config)

        return driver_config
    """

    def _generate_children(self):
        """
        Generate the configuration for child devices
        """
        log.debug("_generate_children for %s", self.agent_instance_obj.name)

        child_device_ids = self._build_child_list()

        ConfigurationBuilder_factory = AgentConfigurationBuilderFactory(self.clients, self.RR2)

        agent_lookup_method = {
            RT.PlatformAgentInstance: self.RR2.find_platform_agent_instance_of_platform_device_using_has_agent_instance,
            RT.InstrumentAgentInstance: self.RR2.find_instrument_agent_instance_of_instrument_device_using_has_agent_instance,
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
            ret[d] = ConfigurationBuilder.prepare(will_launch=False)

        return ret


    def _build_child_list(self):
        dev_id = self._get_device()._id

        log.debug("Getting child platform device ids")
        if self._use_network_parent():
            log.debug("Using hasNetworkParnet")
            assocs = self.RR2.filter_cached_associations(PRED.hasNetworkParent, lambda a: dev_id == a.o)
            child_pdevice_ids = [a.s for a in assocs]
        else:
            log.debug("Using hasDevice")
            child_pdevice_ids = self.RR2.find_platform_device_ids_of_device_using_has_device(self._get_device()._id)
        log.debug("found platform device ids: %s", child_pdevice_ids)

        log.debug("Getting child instrument device ids")
        child_idevice_ids = self.RR2.find_instrument_device_ids_of_device_using_has_device(self._get_device()._id)
        log.debug("found instrument device ids: %s", child_idevice_ids)

        child_device_ids = child_idevice_ids + child_pdevice_ids

        log.debug("combined device ids: %s", child_device_ids)
        return child_device_ids
