#!/usr/bin/env python

"""Shows context of resources"""

import datetime
import pprint
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
                self._print_res(targ_obj1, 2)
                if self.verbose:
                    # ProcessDefinition
                    targ_objs2, _ = self.rr.find_objects(targ_obj1._id, PRED.hasProcessDefinition, id_only=False)
                    for targ_obj2 in sorted(targ_objs2, key=lambda o: o.name):
                        self._print_res(targ_obj2, 4)

            except NotFound:
                pass
            # Resource
            try:
                targ_obj1 = self.rr.read(resource_id)
                self._print_res(targ_obj1, 2)
                scheme = [
                    dict(query=["find_objects", PRED.hasAgentInstance], recurse=[
                        dict(query=["find_objects", PRED.hasAgentDefinition], query_filter=lambda o: self.verbose, recurse=[
                            dict(query=["find_objects", PRED.hasModel]),
                        ]),
                    ]),
                    dict(query=["find_objects", PRED.hasOutputProduct], query_filter=lambda o: self.verbose, recurse=[
                        dict(query=["find_objects", PRED.hasStream]),
                    ]),
                ]
                self._print_scheme(targ_obj1, scheme, indent=2)

            except NotFound:
                pass

    def list_containers(self):
        scheme = [
            dict(name="CapabilityContainers", query=["find_resources", RT.CapabilityContainer], recurse=[
                dict(query=["find_objects", PRED.hasProcess], recurse=[
                    dict(query=["find_objects", PRED.hasProcessDefinition]),
                ]),
            ]),
        ]
        self._print_scheme(None, scheme)

    def list_services(self):
        scheme = [
            dict(name="ServiceDefinitions", query=["find_resources", RT.ServiceDefinition], recurse=[
                dict(query=["find_subjects", PRED.hasServiceDefinition], recurse=[
                    dict(query=["find_objects", PRED.hasProcess], recurse=[
                        dict(query=["find_objects", PRED.hasProcessDefinition]),
                    ]),
                ]),
            ]),
        ]
        self._print_scheme(None, scheme)

    def list_persistence(self):
        # Show ingestion streams, workers (active or not) etc
        ingconf_objs, _ = self.rr.find_resources(RT.IngestionConfiguration, id_only=False)
        if not ingconf_objs:
            log.warn("Could not find system IngestionConfiguration")
        ingconf_obj = ingconf_objs[0]
        scheme = [
            dict(name="Subscriptions", query=["find_objects", PRED.hasSubscription], recurse=[
                dict(query=["find_subjects", PRED.hasSubscription, RT.ExchangeName], recurse=[
                    dict(query=["find_objects", PRED.hasIngestionWorker], recurse=[
                        dict(query=["find_objects", PRED.hasProcessDefinition]),
                    ]),
                ]),
                dict(query=["find_objects", PRED.hasStream], recurse=[
                    dict(query=["find_subjects", PRED.hasStream, RT.Dataset]),
                ]),
            ]),
        ]
        self._print_scheme(ingconf_obj, scheme)

    def show_use(self, resource_id):
        # For given resource, list all uses
        res_obj = self.rr.read(resource_id)
        self._print_output("SHOWING USE of %s %s %s '%s'", res_obj.type_, self._get_alt_id(res_obj, "PRE"), resource_id, res_obj.name)
        if self.verbose:
            pprint.pprint(res_obj.__dict__)
        if isinstance(res_obj, AgentDefinition):
            scheme = [
                dict(name="DeviceModels", query=["find_objects", PRED.hasModel]),
                dict(name="AgentInstances", query=["find_subjects", PRED.hasAgentDefinition], recurse=[
                    dict(query=["find_subjects", PRED.hasAgentInstance]),
                ]),
            ]
            self._print_scheme(res_obj, scheme)

        elif isinstance(res_obj, DeviceModel):
            scheme = [
                dict(name="Sites", query=["find_subjects", PRED.hasModel], query_filter=lambda o: isinstance(o, Site)),
                dict(name="Devices", query=["find_subjects", PRED.hasModel], query_filter=lambda o: isinstance(o, Device)),
                dict(name="AgentDefinitions", query=["find_subjects", PRED.hasModel], query_filter=lambda o: isinstance(o, AgentDefinition)),
            ]
            self._print_scheme(res_obj, scheme)

        elif isinstance(res_obj, Device):
            # Attributes
            self._print_output(" S#%s P#%s", res_obj.serial_number, res_obj.ooi_property_number)

            scheme = [
                dict(name="DeviceModels", query=["find_objects", PRED.hasModel]),
                dict(name="Deployments", query=["find_objects", PRED.hasDeployment]),
                dict(name="AgentInstances", query=["find_objects", PRED.hasAgentInstance]),
                dict(name="Output DataProducts (via hasOutputProduct)", query=["find_objects", PRED.hasOutputProduct]),
                dict(name="Linked DataProducts (via hasSource)", query=["find_subjects", PRED.hasSource, RT.DataProduct]),
                dict(name="Deployed to Site", query=["find_subjects", PRED.hasDevice], query_filter=lambda o: isinstance(o, Site)),
                dict(name="Parent Device", query=["find_subjects", PRED.hasDevice], query_filter=lambda o: isinstance(o, Device)),
                dict(name="Child Devices", query=["find_objects", PRED.hasDevice]),
            ]
            self._print_scheme(res_obj, scheme)

        elif isinstance(res_obj, Site):
            scheme = [
                dict(name="DeviceModels", query=["find_objects", PRED.hasModel]),
                dict(name="Deployments", query=["find_objects", PRED.hasDeployment]),
                dict(name="Linked DataProducts (via hasSource)", query=["find_subjects", PRED.hasSource, RT.DataProduct]),
                dict(name="Deployed Device", query=["find_objects", PRED.hasDevice]),
                dict(name="Parent Site", query=["find_subjects", PRED.hasSite]),
                dict(name="Child Sites", query=["find_objects", PRED.hasSite]),
            ]
            self._print_scheme(res_obj, scheme)

        elif res_obj.type_ == RT.ParameterDictionary:
            scheme = [
                dict(name="StreamDefinitions", query=["find_subjects", PRED.hasParameterDictionary, RT.StreamDefinition], recurse=[
                    dict(query=["find_subjects", PRED.hasStreamDefinition, RT.Stream]),
                    dict(query=["find_subjects", PRED.hasStreamDefinition, RT.DataProduct]),
                ]),
                dict(name="ParameterContexts", query=["find_objects", PRED.hasParameterContext]),
            ]
            self._print_scheme(res_obj, scheme)

        elif res_obj.type_ == RT.ParameterContext:
            scheme = [
                dict(name="ParameterDictionaries", query=["find_subjects", PRED.hasParameterContext, RT.ParameterDictionary]),
            ]
            self._print_scheme(res_obj, scheme)

        elif res_obj.type_ == RT.Dataset:
            scheme = [
                dict(name="DataProducts", query=["find_subjects", PRED.hasDataset, RT.DataProduct], recurse=[
                    dict(query=["find_objects", PRED.hasStreamDefinition], recurse=[
                        dict(query=["find_objects", PRED.hasParameterDictionary]),
                    ]),
                ]),
                dict(name="Streams", query=["find_objects", PRED.hasStream]),
            ]
            self._print_scheme(res_obj, scheme)

        elif res_obj.type_ == RT.ActorIdentity:
            scheme = [
                dict(name="UserInfos", query=["find_objects", PRED.hasInfo]),
                dict(name="UserCredentials", query=["find_objects", PRED.hasCredentials]),
                dict(name="Member of Orgs", query=["find_subjects", PRED.hasMembership, RT.Org], sort_func=lambda o: (o.type_, o.name)),
                dict(name="UserRoles", query=["find_objects", PRED.hasRole]),
                dict(name="Owned Resources", query=["find_subjects", PRED.hasOwner]),
            ]
            self._print_scheme(res_obj, scheme)

        else:
            log.warn("No details for type: %s", res_obj.type_)

        scheme = [
            dict(name="Owned by Actors", query=["find_objects", PRED.hasOwner]),
            dict(name="Shared in Orgs", query=["find_subjects", PRED.hasResource, RT.Org]),
        ]
        self._print_scheme(res_obj, scheme)

    def _print_scheme(self, res_obj, scheme, indent=0):
        for entry in scheme:
            query = entry["query"]
            query_func = query[0]
            query_pred = query[1]
            query_type = query[2] if len(query) > 2 else None
            if query_func == "find_objects":
                targ_objs, _ = self.rr.find_objects(res_obj._id, query_pred, query_type, id_only=False)
            elif query_func == "find_subjects":
                targ_objs, _ = self.rr.find_subjects(query_type, query_pred, object=res_obj._id, id_only=False)
            elif query_func == "find_resources":
                targ_objs, _ = self.rr.find_resources(query_pred, id_only=False)
            else:
                raise BadRequest("Unknown query function: %s" % query_func)

            if "query_filter" in entry:
                filter_exp = entry["query_filter"]
                targ_objs = [targ_obj for targ_obj in targ_objs if filter_exp(targ_obj)]
            if "name" in entry:
                self._print_output("%s%s: %s", " "*indent, entry["name"], len(targ_objs))
            sort_func = lambda o: o.name
            if "sort_func" in entry:
                sort_func = entry["sort_func"]
            for targ_obj in sorted(targ_objs, key=sort_func):
                self._print_res(targ_obj, indent+1)
                if "recurse" in entry:
                    self._print_scheme(targ_obj, entry["recurse"], indent=indent+1)

    def _print_res(self, res_obj, indent=1):
        res_type = res_obj.type_
        if res_type == RT.UserRole:
            self._print_output("%s%s %s %s '%s' in %s", " "*indent, res_obj.type_, self._get_alt_id(res_obj, "PRE"),
                               res_obj._id, res_obj.name, res_obj.org_governance_name)
        elif res_type == RT.Deployment:
            start, end = "?", "?"
            for const in res_obj.constraint_list:
                if const.type_ == OT.TemporalBounds:
                    start = datetime.datetime.fromtimestamp(int(const.start_datetime)).strftime('%Y-%m-%d')
                    end = datetime.datetime.fromtimestamp(int(const.end_datetime)).strftime('%Y-%m-%d')
            self._print_output("%s%s %s %s '%s': %s %s to %s", " "*indent, res_obj.type_, self._get_alt_id(res_obj, "PRE"),
                      res_obj._id, res_obj.name, res_obj.lcstate, start, end)
        elif res_type == RT.Stream:
            self._print_output("%s%s %s: %s on %s", " "*indent, res_obj.type_, res_obj._id,
                               res_obj.stream_route.routing_key, res_obj.stream_route.exchange_point)
        elif res_type == RT.Process:
            self._print_output("%s%s %s: %s", " "*indent, res_obj.type_, res_obj._id,
                               ProcessStateEnum._str_map[res_obj.process_state])
        elif isinstance(res_obj, Device):
            self._print_output("%s%s %s %s '%s': S#%s P#%s", " "*indent, res_obj.type_, self._get_alt_id(res_obj, "PRE"),
                               res_obj._id, res_obj.name, res_obj.serial_number, res_obj.ooi_property_number)
        else:
            self._print_output("%s%s %s %s '%s'", " "*indent, res_obj.type_, self._get_alt_id(res_obj, "PRE"),
                               res_obj._id, res_obj.name)

    def show_dataset(self, resource_id):
        # Show details for a dataset (coverage)
        pass
