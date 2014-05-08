#!/usr/bin/env python

"""Shows context of resources"""

import datetime
import time

from pyon.public import RT, log, PRED, OT, ImmediateProcess, BadRequest, NotFound, LCS, EventPublisher, dict_merge, IonObject

from interface.objects import Site, TemporalBounds, AgentInstance, AgentDefinition, Device, DeviceModel, \
    ProcessStateEnum

class ResourceUseInfo(object):
    def __init__(self, container=None, verbose=False):
        self.container = container
        self.verbose = verbose
        self.rr = self.container.resource_registry
        
    def _print_output(self, message, *args):
        print message % args

    def _get_alt_id(self, res_obj, alt_ns):
        """Return a resource object's alt id for a given namespace, or None"""
        # TODO: Move into IonObjectBase
        alt_ns = alt_ns or "_"
        alt_ids = [aid[len(alt_ns) + 1:] for aid in res_obj.alt_ids if aid.startswith(alt_ns + ":")]
        if alt_ids:
            return alt_ids[0]

    def list_agents(self):
        # Show running agent instances (active or not)
        agent_entries = self.container.directory.find_child_entries("/Agents", direct_only=True)
        agent_objs = []
        for agent_entry in agent_entries:
            attrs = agent_entry.attributes
            agent_type = "?"
            agent_name = attrs.get("name", "")
            if agent_name.startswith("eeagent"):
                agent_type = "EEAgent"
            elif agent_name.startswith("haagent"):
                agent_type = "HAAgent"
            elif "ExternalDatasetAgent" in agent_name:
                agent_type = "DatasetAgent"
            elif "InstrumentAgent" in agent_name:
                agent_type = "InstrumentAgent"
            elif "PlatformAgent" in agent_name:
                agent_type = "PlatformAgent"
            agent_objs.append(dict(agent_type=agent_type, name=agent_name, entry=agent_entry))

        self._print_output("Agents: %s", len(agent_objs))
        for agent_obj in sorted(agent_objs, key=lambda o: (o["agent_type"], o["name"])):
            agent_entry = agent_obj["entry"]
            attrs = agent_entry.attributes
            resource_id = attrs.get("resource_id", "")
            self._print_output(" %s %s %s on %s", agent_obj["agent_type"], resource_id, agent_entry.key, attrs.get("container", ""))
            # Process
            try:
                targ_obj1 = self.rr.read(agent_entry.key)
                self._print_output("  %s %s: %s", targ_obj1.type_, targ_obj1._id, ProcessStateEnum._str_map[targ_obj1.process_state])
                if self.verbose:
                    # ProcessDefinition
                    targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasProcessDefinition, id_only=False)
                    for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                        self._print_output("    %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)

            except NotFound:
                pass
            # Resource
            try:
                targ_obj1 = self.rr.read(resource_id)
                self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                # AgentInstance
                targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasAgentInstance, id_only=False)
                for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                    self._print_output("   %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)
                    if not self.verbose:
                        continue
                    # AgentDefinition
                    targ_objs3, _ = self.rr.find_objects(targ_obj2._id, PRED.hasAgentDefinition, id_only=False)
                    for targ_obj3 in sorted(targ_objs3, key=lambda o: o.name):
                        self._print_output("    %s %s %s '%s'", targ_obj3.type_, self._get_alt_id(targ_obj3, "PRE"), targ_obj3._id, targ_obj3.name)
                        # Model
                        targ_objs4, _ = self.rr.find_objects(targ_obj3._id, PRED.hasModel, id_only=False)
                        for targ_obj4 in sorted(targ_objs4, key=lambda o: o.name):
                            self._print_output("     %s %s %s '%s'", targ_obj4.type_, self._get_alt_id(targ_obj4, "PRE"), targ_obj4._id, targ_obj4.name)
                # DataProduct
                if self.verbose:
                    targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasOutputProduct, id_only=False)
                    for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                        self._print_output("   %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)
                        # Stream
                        targ_objs3, _ = self.rr.find_objects(targ_obj2._id, PRED.hasStream, id_only=False)
                        for targ_obj3 in sorted(targ_objs3, key=lambda o: o.name):
                            self._print_output("    %s %s: %s on %s", targ_obj3.type_, targ_obj3._id, targ_obj3.stream_route.routing_key, targ_obj3.stream_route.exchange_point)

            except NotFound:
                pass

    def list_containers(self):
        # CapabilityContainer
        targ_objs, _ = self.rr.find_resources(RT.CapabilityContainer, id_only=False)
        self._print_output("CapabilityContainers: %s", len(targ_objs))
        for targ_obj in sorted(targ_objs, key=lambda o: o.name):
            self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Process
            targ_objs1, _ = self.rr.find_objects(targ_obj._id, PRED.hasProcess, id_only=False)
            for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                self._print_output("  %s %s '%s': %s", targ_obj1.type_, targ_obj1._id, targ_obj1.name, ProcessStateEnum._str_map[targ_obj1.process_state])
                # ProcessDefinition
                targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasProcessDefinition, id_only=False)
                for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                    self._print_output("   %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)

    def list_services(self):
        # ServiceDefinition
        targ_objs, _ = self.rr.find_resources(RT.ServiceDefinition, id_only=False)
        self._print_output("ServiceDefinitions: %s", len(targ_objs))
        for targ_obj in sorted(targ_objs, key=lambda o: o.name):
            self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Service
            targ_objs1, _ = self.rr.find_subjects(None, PRED.hasServiceDefinition, targ_obj._id, id_only=False)
            for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                # Process
                targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasProcess, id_only=False)
                for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                    self._print_output("   %s %s '%s': %s", targ_obj2.type_, targ_obj2._id, targ_obj2.name, ProcessStateEnum._str_map[targ_obj2.process_state])
                    # ProcessDefinition
                    targ_objs3, _ = self.rr.find_objects(targ_obj2._id, PRED.hasProcessDefinition, id_only=False)
                    for targ_obj3 in sorted(targ_objs3, key=lambda o: o.name):
                        self._print_output("    %s %s %s '%s'", targ_obj3.type_, self._get_alt_id(targ_obj3, "PRE"), targ_obj3._id, targ_obj3.name)

    def list_persistence(self):
        # Show ingestion streams, workers (active or not) etc
        ingconf_ids, _ = self.rr.find_resources(RT.IngestionConfiguration, id_only=True)
        if not ingconf_ids:
            log.warn("Could not find system IngestionConfiguration")
        ingconf_id = ingconf_ids[0]
        targ_objs, _ = self.rr.find_objects(ingconf_id, PRED.hasSubscription, id_only=False)
        self._print_output("Subscriptions: %s", len(targ_objs))
        for targ_obj in sorted(targ_objs, key=lambda o: o.name):
            self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # ExchangeName
            targ_objs1, _ = self.rr.find_subjects(RT.ExchangeName, PRED.hasSubscription, targ_obj._id, id_only=False)
            for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                # Process
                targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasIngestionWorker, id_only=False)
                for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                    self._print_output("   %s %s '%s': %s", targ_obj2.type_, targ_obj2._id, targ_obj2.name, ProcessStateEnum._str_map[targ_obj2.process_state])
                    # ProcessDefinition
                    targ_objs3, _ = self.rr.find_objects(targ_obj2._id, PRED.hasProcessDefinition, id_only=False)
                    for targ_obj3 in sorted(targ_objs3, key=lambda o: o.name):
                        self._print_output("    %s %s %s '%s'", targ_obj3.type_, self._get_alt_id(targ_obj3, "PRE"), targ_obj3._id, targ_obj3.name)
            # Stream
            targ_objs1, _ = self.rr.find_objects(targ_obj._id, PRED.hasStream, id_only=False)
            for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                targ_objs2, _ = self.rr.find_subjects(RT.Dataset, PRED.hasStream, targ_obj1._id, id_only=False)
                for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                    self._print_output("   %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)

    def show_use(self, resource_id):
        # For given resource, list all uses
        res_obj = self.rr.read(resource_id)
        self._print_output("LISTING USE of %s %s %s '%s'", res_obj.type_, self._get_alt_id(res_obj, "PRE"), resource_id, res_obj.name)
        if isinstance(res_obj, AgentDefinition):
            # Models
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasModel, id_only=False)
            self._print_output("DeviceModels: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Agent Instances
            targ_objs, _ = self.rr.find_subjects(None, PRED.hasAgentDefinition, object=resource_id, id_only=False)
            self._print_output("AgentInstances: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
                targ_objs1, _ = self.rr.find_subjects(None, PRED.hasAgentInstance, object=targ_obj._id, id_only=False)
                for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                    self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)

        elif isinstance(res_obj, DeviceModel):
            # Site
            targ_objs, _ = self.rr.find_subjects(None, PRED.hasModel, object=resource_id, id_only=False)
            site_objs = [o for o in targ_objs if isinstance(o, Site)]
            self._print_output("Sites: %s", len(site_objs))
            for targ_obj in sorted(site_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Device
            device_objs = [o for o in targ_objs if isinstance(o, Device)]
            self._print_output("Devices: %s", len(device_objs))
            for targ_obj in sorted(device_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Agent Definition
            adef_objs = [o for o in targ_objs if isinstance(o, AgentDefinition)]
            self._print_output("AgentDefinitions: %s", len(adef_objs))
            for targ_obj in sorted(adef_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif isinstance(res_obj, Device):
            # Model
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasModel, id_only=False)
            self._print_output("DeviceModels: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Deployment
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, id_only=False)
            self._print_output("Deployments: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                start, end = "?", "?"
                for const in targ_obj.constraint_list:
                    if const.type_ == OT.TemporalBounds:
                        start = datetime.datetime.fromtimestamp(int(const.start_datetime)).strftime('%Y-%m-%d')
                        end = datetime.datetime.fromtimestamp(int(const.end_datetime)).strftime('%Y-%m-%d')
                self._print_output(" %s %s %s '%s': %s %s to %s", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"),
                          targ_obj._id, targ_obj.name, targ_obj.lcstate, start, end)
            # AgentInstance
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasAgentInstance, id_only=False)
            self._print_output("AgentInstances: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Output DataProducts
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasOutputProduct, id_only=False)
            self._print_output("Output DataProducts (via hasOutputProduct): %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Linked DataProducts
            targ_objs, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasSource, resource_id, id_only=False)
            self._print_output("Linked DataProducts (via hasSource): %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Deployed to Site
            targ_objs, _ = self.rr.find_subjects(None, PRED.hasDevice, resource_id, id_only=False)
            site_objs = [o for o in targ_objs if isinstance(o, Site)]
            self._print_output("Deployed to Site: %s", len(site_objs))
            for targ_obj in sorted(site_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Parent Device
            device_objs = [o for o in targ_objs if isinstance(o, Device)]
            self._print_output("Parent Device: %s", len(device_objs))
            for targ_obj in sorted(device_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Child Device
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
            self._print_output("Child Devices: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif isinstance(res_obj, Site):
            # Model
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasModel, id_only=False)
            self._print_output("DeviceModels: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Deployment
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasDeployment, id_only=False)
            self._print_output("Deployments: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                start, end = "?", "?"
                for const in targ_obj.constraint_list:
                    if const.type_ == OT.TemporalBounds:
                        start = datetime.datetime.fromtimestamp(int(const.start_datetime)).strftime('%Y-%m-%d')
                        end = datetime.datetime.fromtimestamp(int(const.end_datetime)).strftime('%Y-%m-%d')
                self._print_output(" %s %s %s '%s': %s %s to %s", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"),
                          targ_obj._id, targ_obj.name, targ_obj.lcstate, start, end)
            # Linked DataProducts
            targ_objs, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasSource, resource_id, id_only=False)
            self._print_output("Linked DataProducts (via hasSource): %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Deployed Device
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasDevice, id_only=False)
            self._print_output("Deployed Device: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Parent Site
            targ_objs, _ = self.rr.find_subjects(None, PRED.hasSite, resource_id, id_only=False)
            self._print_output("Parent Site: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Child Site
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasSite, id_only=False)
            self._print_output("Child Sites: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif res_obj.type_ == RT.ParameterDictionary:
            # StreamDefinition
            targ_objs, _ = self.rr.find_subjects(RT.StreamDefinition, PRED.hasParameterDictionary, object=resource_id, id_only=False)
            self._print_output("StreamDefinitions: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
                targ_objs1, _ = self.rr.find_subjects(RT.Stream, PRED.hasStreamDefinition, object=targ_obj._id, id_only=False)
                for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                    self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                targ_objs1, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasStreamDefinition, object=targ_obj._id, id_only=False)
                for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                    self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
            # ParameterContext
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasParameterContext, id_only=False)
            self._print_output("ParameterContexts: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif res_obj.type_ == RT.ParameterContext:
            # ParameterDictionary
            targ_objs, _ = self.rr.find_subjects(RT.ParameterDictionary, PRED.hasParameterContext, object=resource_id, id_only=False)
            self._print_output("ParameterDictionarys: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif res_obj.type_ == RT.Dataset:
            # DataProduct
            targ_objs, _ = self.rr.find_subjects(RT.DataProduct, PRED.hasDataset, object=resource_id, id_only=False)
            self._print_output("DataProducts: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
                targ_objs1, _ = self.rr.find_objects(targ_obj._id, PRED.hasStreamDefinition, id_only=False)
                for targ_obj1 in sorted(targ_objs1, key=lambda o: o.name):
                    self._print_output("  %s %s %s '%s'", targ_obj1.type_, self._get_alt_id(targ_obj1, "PRE"), targ_obj1._id, targ_obj1.name)
                    targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasParameterDictionary, id_only=False)
                    for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                        self._print_output("   %s %s %s '%s'", targ_obj2.type_, self._get_alt_id(targ_obj2, "PRE"), targ_obj2._id, targ_obj2.name)

            # Stream
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasStream, id_only=False)
            self._print_output("Streams: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif res_obj.type_ == RT.ActorIdentity:
            # UserInfo
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasInfo, id_only=False)
            self._print_output("UserInfos: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # UserCredential
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasCredentials, id_only=False)
            self._print_output("UserCredentials: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # Member Orgs
            targ_objs, _ = self.rr.find_subjects(RT.Org, PRED.hasMembership, object=resource_id, id_only=False)
            self._print_output("Member of Orgs: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: (o.type_, o.name)):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
            # UserRole
            targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasRole, id_only=False)
            self._print_output("UserRoles: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: o.name):
                self._print_output(" %s %s %s '%s' in %s", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name, targ_obj.org_governance_name)
            # Resources
            targ_objs, _ = self.rr.find_subjects(None, PRED.hasOwner, object=resource_id, id_only=False)
            self._print_output("Owned Resources: %s", len(targ_objs))
            for targ_obj in sorted(targ_objs, key=lambda o: (o.type_, o.name)):
                self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

        elif res_obj.type_ == RT.ProcessDefinition:
            pass

        else:
            log.warn("No details for type: %s", res_obj.type_)

        targ_objs, _ = self.rr.find_objects(resource_id, PRED.hasOwner, id_only=False)
        self._print_output("Owned by Actors: %s", len(targ_objs))
        for targ_obj in sorted(targ_objs, key=lambda o: o.name):
            self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)
        targ_objs, _ = self.rr.find_subjects(RT.Org, PRED.hasResource, object=resource_id, id_only=False)
        self._print_output("Shared in Orgs: %s", len(targ_objs))
        for targ_obj in sorted(targ_objs, key=lambda o: o.name):
            self._print_output(" %s %s %s '%s'", targ_obj.type_, self._get_alt_id(targ_obj, "PRE"), targ_obj._id, targ_obj.name)

    def show_dataset(self, resource_id):
        # Show details for a dataset (coverage)
        pass
