#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.policy
@author   Ian Katz
"""

from pyon.public import PRED, RT, LCS
from pyon.ion.resource import get_maturity_visibility
from ion.services.sa.instrument.flag import KeywordFlag

class ResourceLCSPolicy(object):

    def __init__(self, clients):
        self.clients = clients

        if hasattr(clients, "resource_registry"):
            self.RR = self.clients.resource_registry

        self.on_policy_init()

    def on_policy_init(self):
        pass

    def _make_result(self, result, message):
        if result:
            return True, ""
        else:
            return False, message

    def _make_pass(self):
        return self._make_result(True, "")

    def _make_fail(self, message):
        return self._make_result(False, message)

    def _make_warn(self, message):
        return self._make_result(True, message)

    def _get_resource_type(self, resource):
        """
        get the type of a resource... simple wrapper
        @param resource a resource
        """
        restype = type(resource).__name__

        return restype


    def _get_resource_type_by_id(self, resource_id):
        """
        get the type of a resource by id
        @param resource_id a resource id
        """
        assert(type("") == type(resource_id))
        return self._get_resource_type(self.RR.read(resource_id))

    def _find_having(self, subject_type, association_predicate, some_object):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object the object "owned" by the association type
        """
        #log.debug("_find_having, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_subjects(subject_type,
                                       association_predicate,
                                       some_object,
                                       False)
        return ret

    def _find_stemming(self, primary_object_id, association_predicate, some_object_type):
        """
        find resources stemming from _____:
          find resource IDs of the given object type that
          are associated with the primary object
        @param primary_object_id the id of the primary object
        @param association_predicate the association type
        @param some_object_type the type of associated object
        """
        #log.debug("_find_stemming, from %s" % self._toplevel_call())
        ret, _ = self.RR.find_objects(primary_object_id,
                                      association_predicate,
                                      some_object_type,
                                      False)
        return ret

    def _has_keyworded_attachment(self, resource_id, desired_keyword):
        for a in self._find_stemming(resource_id, PRED.hasAttachment, RT.Attachment):
            for k in a.keywords:
                if desired_keyword == k:
                    return self._make_pass()
        return self._make_fail("No attachment found with keyword='%s'" % desired_keyword)

    def _resource_lcstate_in(self, resource_obj, permissible_states=None):

        if permissible_states is None:
            permissible_states = []
                
        parts = get_maturity_visibility(resource_obj.lcstate)

        return self._make_result(parts[0] in permissible_states,
                                 "'%s' resource is in state '%s', wanted [%s]" %
                                 (self._get_resource_type(resource_obj), parts[0], str(permissible_states)))
                      

class AgentPolicy(ResourceLCSPolicy):

    def lce_precondition_plan(self, agent_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, agent_id):
        former = self.lce_precondition_plan(agent_id)
        if not former[0]: return former

        agent_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentAgent == agent_type:
            return self._make_result(0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.InstrumentModel)),
                                     "No model associated with agent")

        if RT.PlatformAgent == agent_type:
            return self._make_result(0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.PlatformModel)),
                                     "No model associated with agent")

        return self._make_fail("Wrong resource type (got '%s')" % agent_type)

    def lce_precondition_integrate(self, agent_id):
        former = self.lce_precondition_develop(agent_id)
        if not former[0]: return former


        #if not checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.EGG_URL)

    def lce_precondition_deploy(self, agent_id):
        former = self.lce_precondition_integrate(agent_id)
        if not former[0]: return former

        #if no checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.CERTIFICATION)

    def lce_precondition_retire(self, agent_id):
        ret = (0 == self._find_having(RT.InstrumentAgentInstance, PRED.hasAgentDefinition, agent_id))
        return self._make_result(ret, "InstrumentAgentInstance(s) are still using this InstrumentAgent")

class ModelPolicy(ResourceLCSPolicy):
    def lce_precondition_plan(self, model_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, model_id):
        # todo
        return self._make_pass()

    def lce_precondition_integrate(self, model_id):
        # todo
        return self._make_pass()

    def lce_precondition_deploy(self, model_id):
        # todo
        return self._make_pass()

    def lce_precondition_retire(self, model_id):
        # todo: more than checking agents, devices, and sites?

        model_type = self._get_resource_type_by_id(model_id)

        if RT.SensorModel == model_type:
            return 0 == len(self._find_having(RT.SensorDevice, PRED.hasModel, model_id))

        if RT.InstrumentModel == model_type:
            if 0 < len(self._find_having(RT.InstrumentDevice, PRED.hasModel, model_id)):
                return self._make_fail("InstrumentDevice(s) are using this model")
            if 0 < len(self._find_having(RT.InstrumentAgent, PRED.hasModel, model_id)):
                return self._make_fail("InstrumentAgent(s) are using this model")
            if 0 < len(self._find_having(RT.InstrumentSite, PRED.hasModel, model_id)):
                return self._make_fail("InstrumentSite(s) are using this model")
            return self._make_pass()

        if RT.PlatformModel == model_type:
            if 0 < len(self._find_having(RT.PlatformDevice, PRED.hasModel, model_id)):
                return self._make_fail("PlatformDevice(s) are using this model")
            if 0 < len(self._find_having(RT.PlatformAgent, PRED.hasModel, model_id)):
                return self._make_fail("PlatformAgent(s) are using this model")
            if 0 < len(self._find_having(RT.PlatformSite, PRED.hasModel, model_id)):
                return self._make_fail("PlatformSite(s) are using this model")
            return self._make_pass()

        return self._make_fail("Wrong resource type (got '%s')" % model_type)


class DevicePolicy(ResourceLCSPolicy):

    def invalid_custom_attrs(self, device_id, model_id):
        model_obj  = self.RR.read(model_id)
        device_obj = self.RR.read(device_id)

        bad = {}
        for k, v in device_obj.custom_attributes.iteritems():
            if not k in model_obj.custom_attributes:
                bad[k] = v

        if {} == bad:
            return ""
        else:
            return "Device contains custom attributes undefined by associated model: %s" % str(bad)


    def lce_precondition_plan(self, device_id):
        obj = self.RR.read(device_id)

        return self._make_result("" != obj.name, "Name was not defined")

    def lce_precondition_develop(self, device_id):
        former = self.lce_precondition_plan(device_id)
        if not former[0]: return former

        #have an agent/deployed, model/deployed

        obj = self.RR.read(device_id)
        device_type = self._get_resource_type(device_id)

        if "" == obj.serial_number:
            return self._make_fail("Device has no serial number")

        if RT.InstrumentDevice == device_type:
            models = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentModel)
            if 0 == len(models):
                return self._make_fail("Device has no associated model")
            if not self._resource_lcstate_in(models[0], [LCS.DEPLOYED]):
                return self._make_fail("Device's associated model is not in '%s'" % LCS.DEPLOYED)

            #validate custom fields
            bad = self.invalid_custom_attrs(device_id, models[0])
            if "" != bad:
                return self._make_fail(bad)

            return self._has_keyworded_attachment(device_id, KeywordFlag.VENDOR_TEST_RESULTS)

        if RT.PlatformDevice == device_type:
            models = self._find_stemming(device_id, PRED.hasModel, RT.PlatformModel)
            if 0 == len(models):
                return self._make_fail("Device has no associated model")
            if not self._resource_lcstate_in(models[0], [LCS.DEPLOYED]):
                return self._make_fail("Device's associated model is not in '%s'" % LCS.DEPLOYED)

            #validate custom fields
            bad = self.invalid_custom_attrs(device_id, models[0])
            if "" != bad:
                return self._make_fail(bad)

            return self._has_keyworded_attachment(device_id, KeywordFlag.VENDOR_TEST_RESULTS)

        return self._make_fail("Wrong resource type (got '%s')" % device_type)


    def lce_precondition_integrate(self, device_id):
        former = self.lce_precondition_develop(device_id)
        if not former[0]: return former

        #Have an instrument site/deployed, site has agent, site agent == device agent

        device_type = self._get_resource_type_by_id(device_id)

        if RT.PlatformDevice == device_type:
            agents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)
            if 0 == len(agents):
                return self._make_fail("Device has no associated agent instance")
            tmp = self._resource_lcstate_in(agents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            return self._make_pass()

        if RT.InstrumentDevice == device_type:
            agents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)
            if 0 == len(agents):
                return self._make_fail("Device has no associated agent instance")
            tmp = self._resource_lcstate_in(agents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            parents = self._find_having(RT.PlatformDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.INTEGRATED, LCS.DEPLOYED])
            if not tmp[0]: return tmp

            return self._make_pass()

        if RT.SensorDevice == device_type:
            parents = self._find_having(RT.InstrumentDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.INTEGRATED, LCS.DEPLOYED])
            if not tmp[0]: return tmp
            return self._make_pass()

        #todo: verify comms with device??

        return self._make_fail("Wrong resource type (got '%s')" % device_type)


    def lce_precondition_deploy(self, device_id):
        former = self.lce_precondition_integrate(device_id)
        if not former[0]: return former

        # Have associated agent instance, has a parent subsite which is deployed, platform device has platform site, all deployed.  

        device_type = self._get_resource_type_by_id(device_id)

        if RT.SensorDevice == device_type:
            parents = self._find_having(RT.InstrumentDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp
            return self._make_pass()

        if RT.InstrumentDevice == device_type:
            parents = self._find_having(RT.PlatformDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            sites = self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)
            if 0 == len(sites):
                return self._make_fail("Device does not have an assigned site")
            tmp = self._resource_lcstate_in(sites[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.InstrumentAgent)
            if 0 == len(siteagents): return self._make_fail("No site agent found")
        
            agentinsts = self._find_stemming(device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)
            if 0 == len(agentinsts): return self._make_fail("No agent instance found")
            agents = self._find_stemming(agentinsts[0], PRED.hasAgentDefinition, RT.InstrumentAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.
            if siteagents[0]._id != agents[0]._id: return False

            # all sensor devices must be deployed
            for dev in self._find_stemming(device_id, PRED.hasDevice, RT.SensorDevice):
                if not self._resource_lcstate_in(dev, [LCS.DEPLOYED]): return False

            return self._make_pass()
            
        if RT.PlatformDevice == device_type:
            sites = self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)
            if 0 == len(sites):
                return self._make_fail("Device does not have an assigned site")
            tmp = self._resource_lcstate_in(sites[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            siteagents = self._find_stemming(sites[0]._id, PRED.hasAgent, RT.PlatformAgent)
            if 0 == len(siteagents): return self._make_fail("No site agent found")

            agentinsts = self._find_stemming(device_id, PRED.hasAgentInstance, RT.PlatformAgentInstance)
            if 0 == len(agentinsts): return self._make_fail("No agent instance found")
            agents = self._find_stemming(agentinsts[0], PRED.hasAgentDefinition, RT.PlatformAgent)
            # we check the develop precondition here, which checks that there's an agent. so assume it.

            #todo: remove "site hasAgent agent"
            #todo: add check that model is supported
            if siteagents[0]._id != agents[0]._id: return False



            return self._make_pass()

        return False

    def lce_precondition_retire(self, device_id):

        device_type = self._get_resource_type_by_id(device_id)

        if RT.SensorDevice == device_type:
            return self._make_pass()

        if RT.InstrumentDevice == device_type:
            if 0 < len(self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)):
                return self._make_fail("Device is still assigned to a site")
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Device is still assigned to a deployment")

        if RT.PlatformDevice == device_type:
            if 0 < len(self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)):
                return self._make_fail("Device is still assigned to a site")
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Device is still assigned to a deployment")

        return False

class SitePolicy(ResourceLCSPolicy):
    def lce_precondition_plan(self, site_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, site_id):
        former = self.lce_precondition_plan(site_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_integrate(self, site_id):
        former = self.lce_precondition_develop(site_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_deploy(self, site_id):
        former = self.lce_precondition_integrate(site_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_retire(self, site_id):
        # todo:
        return self._make_pass()


class DataProductPolicy(ResourceLCSPolicy):
    def lce_precondition_plan(self, data_product_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, data_product_id):
        former = self.lce_precondition_plan(data_product_id)
        if not former[0]: return former

        # todo: mandatory fields?
        return self._has_keyworded_attachment(data_product_id, KeywordFlag.CERTIFICATION)

    def lce_precondition_integrate(self, data_product_id):
        former = self.lce_precondition_develop(data_product_id)
        if not former[0]: return former

        if 0 == len(self._find_stemming(data_product_id, PRED.hasStream, RT.Stream)):
            return self._make_fail("Product has no associated stream")

        pducers = self._find_stemming(data_product_id, PRED.hasDataProducer, RT.DataProducer)
        if 0 == len(pducers):
            return self._make_fail("Product has no associated producer")

        if 0 < len(self._find_stemming(pducers[0], PRED.hasInputDataProducer, RT.DataProducer)):
            return self._make_pass()
        elif 0 < len(self._find_stemming(pducers[0], PRED.hasOutputDataProducer, RT.DataProducer)):
            return self._make_pass()
        else:
            return self._make_fail("Product's producer has neither input nor output data producer")

    def lce_precondition_deploy(self, data_product_id):
        former = self.lce_precondition_integrate(data_product_id)
        if not former[0]: return former

        datasets = self._find_stemming(data_product_id, PRED.hasDataset, RT.Dataset)
        if 0 == len(datasets):
            return self._make_warn("Dataset not available")
        else:
            return self._resource_lcstate_in(datasets[0])


    def lce_precondition_retire(self, data_product_id):
        # todo:
        return self._make_pass()


class DataProcessPolicy(ResourceLCSPolicy):
    def lce_precondition_plan(self, data_process_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, data_process_id):
        former = self.lce_precondition_plan(data_process_id)
        if not former[0]: return former

        # todo: required fields

        return self._make_pass()

    def lce_precondition_integrate(self, data_process_id):
        former = self.lce_precondition_develop(data_process_id)
        if not former[0]: return former

        # todo: currently, nothing.  "MAY be assoc with QA test results"

        return self._make_pass()

    def lce_precondition_deploy(self, data_process_id):
        former = self.lce_precondition_integrate(data_process_id)
        if not former[0]: return former

        #todo: something about python egg, not sure yet

        return self._has_keyworded_attachment(data_process_id, KeywordFlag.CERTIFICATION)



    def lce_precondition_retire(self, data_process_id):
        # todo:
        return self._make_pass()


# currently same as DataProcess
class DataProcessDefinitionPolicy(DataProcessPolicy):
    pass

"""
# in case i need another one
class XxPolicy(Policy):
    def lce_precondition_plan(self, x_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, x_id):
        former = self.lce_precondition_plan(x_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_integrate(self, x_id):
        former = self.lce_precondition_develop(x_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_deploy(self, x_id):
        former = self.lce_precondition_integrate(x_id)
        if not former[0]: return former

        # todo

        return self._make_pass()

    def lce_precondition_retire(self, x_id):
        # todo:
        return self._make_pass()

"""