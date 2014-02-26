#!/usr/bin/env python

"""
@package  ion.util.resource_lcs_policy
@author   Ian Katz
"""

from ooi.logging import log

from pyon.public import PRED, RT, LCS
from pyon.ion.resource import LCE
from ion.services.sa.instrument.flag import KeywordFlag


class ResourceLCSPolicy(object):

    def __init__(self, clients):
        self.clients = clients

        if hasattr(clients, "resource_registry"):
            self.RR = self.clients.resource_registry

        self.lce_precondition = {}
        self.lce_precondition[LCE.PLAN]       = self.lce_precondition_plan
        self.lce_precondition[LCE.INTEGRATE]  = self.lce_precondition_integrate
        self.lce_precondition[LCE.DEVELOP]    = self.lce_precondition_develop
        self.lce_precondition[LCE.DEPLOY]     = self.lce_precondition_deploy
        self.lce_precondition[LCE.RETIRE]     = self.lce_precondition_retire
        self.lce_precondition[LCE.DELETE]     = self.lce_precondition_delete

        self.lce_precondition[LCE.ANNOUNCE]   = self.lce_precondition_announce
        self.lce_precondition[LCE.UNANNOUNCE] = self.lce_precondition_unannounce
        self.lce_precondition[LCE.ENABLE]     = self.lce_precondition_enable
        self.lce_precondition[LCE.DISABLE]    = self.lce_precondition_disable

        self.on_policy_init()

    def on_policy_init(self):
        pass

    # maturity
    def lce_precondition_plan(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_develop(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_integrate(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_deploy(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_retire(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_delete(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    # visibility
    def lce_precondition_announce(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_unannounce(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_enable(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    def lce_precondition_disable(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")

    # force delete
    def precondition_delete(self, agent_id):
        return self._make_warn("ResourceLCSPolicy base class not overridden!")


    def policy_fn_delete_precondition(self, id_field):

        def freeze():
            def policy_fn(msg, headers):
                #The validation interceptor should have already verified that these are in the msg dict
                resource_id = msg[id_field]
                log.debug("policy_fn for force_delete got %s=(%s)'%s'",
                          id_field,
                          type(resource_id).__name__,
                          resource_id)

                ret = self.precondition_delete(resource_id)
                #check_lcs_precondition_satisfied(resource_id, lifecycle_event)
                isok, msg = ret
                log.debug("policy_fn for '%s %s' successfully returning %s - %s",
                          "force_delete",
                          id_field,
                          isok,
                          msg)
                return ret

            return policy_fn

        return freeze()

    def policy_fn_lcs_precondition(self, id_field):

        def freeze():
            def policy_fn(msg, headers):
                #The validation interceptor should have already verified that these are in the msg dict
                resource_id = msg[id_field]
                lifecycle_event = msg['lifecycle_event']
                log.debug("policy_fn got LCE=%s for %s=(%s)'%s'",
                          lifecycle_event,
                          id_field,
                          type(resource_id).__name__,
                          resource_id)

                ret = self.check_lcs_precondition_satisfied(resource_id, lifecycle_event)
                isok, msg = ret
                log.debug("policy_fn for '%s %s' successfully returning %s - %s",
                          lifecycle_event,
                          id_field,
                          isok,
                          msg)
                return ret

            return policy_fn

        return freeze()

    def check_lcs_precondition_satisfied(self, resource_id, transition_event):
        # check that the resource exists
        resource = self.RR.read(resource_id)
        resource_type = resource.type_

        # check that precondition function exists
        if not transition_event in self.lce_precondition:
            self._make_fail("%s lifecycle precondition method for event '%s' not defined! Choices: %s" %
                            (resource_type, transition_event, str(self.lce_precondition.keys())))

        precondition_fn = self.lce_precondition[transition_event]

        # check that the precondition is met
        verbose = False
        if not verbose:
            return precondition_fn(resource_id)
        else:
            isok, errmsg = precondition_fn(resource_id)
            if isok:
                return isok, errmsg
            else:
                return isok, (("Couldn't apply '%s' LCS transition to %s '%s'; "
                               + "failed precondition: %s")
                              % (transition_event, resource_type, resource_id, errmsg))

    def _make_result(self, result, message):
        if result:
            return True, ""
        else:
            return False, message

    def _make_pass(self):
        return True, ""

    def _make_fail(self, message):
        return False, message

    def _make_warn(self, message):
        return True, message

    def _get_resource_type_by_id(self, resource_id):
        """
        get the type of a resource by id
        @param resource_id a resource id
        """
        assert type(resource_id) is str
        try:
            resource = self.RR.read(resource_id)
            return resource.type_
        except Exception as e:
            e.message = "resource_lcs_policy:_get_resource_type_by_id: %s" % e.message
            raise e

    def _find_having(self, subject_type, association_predicate, some_object_id):
        """
        find resources having ____:
          find resource IDs of the predefined type that
          have the given association attached
        @param association_predicate one of the association types
        @param some_object_id the object "owned" by the association type
        """
        assert type(some_object_id) is str
        ret, _ = self.RR.find_subjects(subject_type,
                                       association_predicate,
                                       some_object_id,
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
        assert type(primary_object_id) is str
        ret, _ = self.RR.find_objects(primary_object_id,
                                      association_predicate,
                                      some_object_type,
                                      False)
        return ret

    def _has_keyworded_attachment(self, resource_id, desired_keyword):
        if True:
            log.warn("Ignoring (non)existence of keyword '%s' for resource '%s' policy check for beta testing",
                     desired_keyword, resource_id)
            return self._make_pass() # HACK for beta testing purposes
        assert type(resource_id) is str
        for a in self._find_stemming(resource_id, PRED.hasAttachment, RT.Attachment):
            for k in a.keywords:
                if desired_keyword == k:
                    return self._make_pass()
        return self._make_fail("No attachment found with keyword='%s'" % desired_keyword)

    def _resource_lcstate_in(self, resource_obj, permissible_states=None):
        assert type(resource_obj) is str
        if permissible_states is None:
            permissible_states = []
                
        return self._make_result(resource_obj.lcstate in permissible_states,
                                 "'%s' resource is in state '%s', wanted [%s]" %
                                 (resource_obj.type_, resource_obj.lcstate, str(permissible_states)))



class AgentPolicy(ResourceLCSPolicy):

    def lce_precondition_plan(self, agent_id):
        # always OK
        return self._make_pass()

    def lce_precondition_develop(self, agent_id):
        former = self.lce_precondition_plan(agent_id)
        if not former[0]:
            return former

        agent_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentAgent == agent_type:
            return self._make_result(0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.InstrumentModel)),
                                     "No model associated with agent")

        if RT.PlatformAgent == agent_type:
            return self._make_result(0 < len(self._find_stemming(agent_id, PRED.hasModel, RT.PlatformModel)),
                                     "No model associated with agent")

        return self._make_fail("Wrong agent resource type (got '%s')" % agent_type)

    def lce_precondition_integrate(self, agent_id):
        former = self.lce_precondition_develop(agent_id)
        if not former[0]:
            return former


        #if not checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.EGG_URL)

    def lce_precondition_deploy(self, agent_id):
        former = self.lce_precondition_integrate(agent_id)
        if not former[0]:
            return former

        #if no checking platform agents yet, uncomment this
        #if RT.PlatformAgent == self._get_resource_type_by_id(agent_id): return True

        return self._has_keyworded_attachment(agent_id, KeywordFlag.CERTIFICATION)

    def lce_precondition_retire(self, agent_id):
        return self._make_pass()

    def lce_precondition_delete(self, agent_id):
        ret = (0 == self._find_having(RT.InstrumentAgentInstance, PRED.hasAgentDefinition, agent_id))
        return self._make_result(ret, "InstrumentAgentInstance(s) are still using this InstrumentAgent")


    def precondition_delete(self, agent_id):
        agent_type = self._get_resource_type_by_id(agent_id)

        if RT.InstrumentAgent == agent_type:
            ret = (0 == len(self._find_having(RT.InstrumentAgentInstance, PRED.hasAgentDefinition, agent_id)))
            return self._make_result(ret, "InstrumentAgentInstance(s) are still using this InstrumentAgent")

        if RT.PlatformAgent == agent_type:
            ret = (0 == len(self._find_having(RT.PlatformAgentInstance, PRED.hasAgentDefinition, agent_id)))
            return self._make_result(ret, "PlatformAgentInstance(s) are still using this PlatformAgent")

        return self._make_fail("Wrong agent resource type (got '%s')" % agent_type)


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
        return self._make_pass()

    def lce_precondition_delete(self, model_id):
        # todo: more than checking agents, devices, and sites?

        model_type = self._get_resource_type_by_id(model_id)

        if RT.SensorModel == model_type:
            if 0 < len(self._find_having(RT.SensorDevice, PRED.hasModel, model_id)):
                return self._make_fail("SensorDevice(s) are using this model")

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

        return self._make_fail("Wrong model resource type (got '%s')" % model_type)

    def precondition_delete(self, model_id):

        model_type = self._get_resource_type_by_id(model_id)

        if RT.SensorModel == model_type:
            if 0 < len(self._find_having(RT.SensorDevice, PRED.hasModel, model_id)):
                return self._make_fail("SensorDevice(s) are using this model")
            return self._make_pass()

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

        return self._make_fail("Wrong model resource type (got '%s')" % model_type)


class DevicePolicy(ResourceLCSPolicy):

    def invalid_custom_attrs(self, device_id, model_id):
        assert(type("") == type(device_id) == type(model_id))
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

        if 0 == len(self._find_having(RT.Org, PRED.hasResource, device_id)):
            return self._make_fail("Device is not associated with any Org")

        return self._make_result("" != obj.name, "Name was not defined")

    def lce_precondition_develop(self, device_id):
        former = self.lce_precondition_plan(device_id)
        if not former[0]: return former

        #have an agent/deployed, model/deployed

        obj = self.RR.read(device_id)
        device_type = self._get_resource_type_by_id(device_id)

        if "" == obj.serial_number:
            return self._make_fail("Device has no serial number")

        if RT.InstrumentDevice == device_type:
            models = self._find_stemming(device_id, PRED.hasModel, RT.InstrumentModel)
            if 0 == len(models):
                return self._make_fail("Device has no associated model")
            if not self._resource_lcstate_in(models[0], [LCS.DEPLOYED]):
                return self._make_fail("Device's associated model is not in '%s'" % LCS.DEPLOYED)

            #validate custom fields
            bad = self.invalid_custom_attrs(device_id, models[0]._id)
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
            bad = self.invalid_custom_attrs(device_id, models[0]._id)
            if "" != bad:
                return self._make_fail(bad)

            return self._has_keyworded_attachment(device_id, KeywordFlag.VENDOR_TEST_RESULTS)

        return self._make_fail("Wrong device resource type for develop(got '%s')" % device_type)


    def lce_precondition_integrate(self, device_id):
        former = self.lce_precondition_develop(device_id)
        if not former[0]: return former

        #Have an instrument site/deployed, site has agent, site agent == device agent

        device_type = self._get_resource_type_by_id(device_id)

        if device_type == RT.PlatformDevice:
            agents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.PlatformAgentInstance)
            dagents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance)
            if len(agents) + len(dagents) != 1:
                return self._make_fail("Device has %d associated agent instances, not 1" % (len(agents) + len(dagents)))
            tmp = self._resource_lcstate_in(agents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            #any parent must be integrated or deployed
            parents = self._find_having(RT.PlatformDevice, PRED.hasDevice, device_id)
            if 0 < len(parents):
                tmp = self._resource_lcstate_in(parents[0], [LCS.INTEGRATED, LCS.DEPLOYED])
                if not tmp[0]: return tmp

            return self._make_pass()

        if device_type == RT.InstrumentDevice:
            agents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.InstrumentAgentInstance)
            dagents = self._find_stemming(device_id, PRED.hasAgentInstance, RT.ExternalDatasetAgentInstance)
            if len(agents) + len(dagents) != 1:
                return self._make_fail("Device has %d associated agent instances, not 1" % (len(agents) + len(dagents)))
            tmp = self._resource_lcstate_in(agents[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp

            parents = self._find_having(RT.PlatformDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.INTEGRATED, LCS.DEPLOYED])
            if not tmp[0]: return tmp

            return self._make_pass()

        if device_type == RT.SensorDevice:
            parents = self._find_having(RT.InstrumentDevice, PRED.hasDevice, device_id)
            if 0 == len(parents):
                return self._make_fail("Device is not attached to a parent")
            tmp = self._resource_lcstate_in(parents[0], [LCS.INTEGRATED, LCS.DEPLOYED])
            if not tmp[0]: return tmp

            return self._make_pass()

        #todo: verify comms with device??

        return self._make_fail("Wrong device resource type for integrate(got '%s')" % device_type)


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

            return self._make_pass()
            
        if RT.PlatformDevice == device_type:
            #any parent must be integrated or deployed
            parents = self._find_having(RT.PlatformDevice, PRED.hasDevice, device_id)
            if 0 < len(parents):
                tmp = self._resource_lcstate_in(parents[0], [LCS.DEPLOYED])
                if not tmp[0]: return tmp

            sites = self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)
            if 0 == len(sites):
                return self._make_fail("Device does not have an assigned site")
            tmp = self._resource_lcstate_in(sites[0], [LCS.DEPLOYED])
            if not tmp[0]: return tmp


            #todo: add check that model is supported



            return self._make_pass()

        return self._make_fail("Wrong device resource type for deploy (got '%s')" % device_type)

    def lce_precondition_retire(self, device_id):
        return self._make_pass()

    def lce_precondition_delete(self, device_id):

        device_type = self._get_resource_type_by_id(device_id)

        if RT.SensorDevice == device_type:
            return self._make_pass()

        if RT.InstrumentDevice == device_type:
            if 0 < len(self._find_having(RT.InstrumentSite, PRED.hasDevice, device_id)):
                return self._make_fail("Device is still assigned to a site")
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Device is still assigned to a deployment")
            return self._make_pass()

        if RT.PlatformDevice == device_type:
            if 0 < len(self._find_having(RT.PlatformSite, PRED.hasDevice, device_id)):
                return self._make_fail("Device is still assigned to a site")
            if 0 < len(self._find_stemming(device_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Device is still assigned to a deployment")
            return self._make_pass()

        return self._make_fail("Wrong device resource type for delete(got '%s')" % device_type)

    def precondition_delete(self, device_id):
        return self.lce_precondition_delete(device_id)


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
        return self._make_pass()

    def lce_precondition_delete(self, site_id):
        # todo: Sites and all subclasses can not be retired if they have children or if they are
        # not associated to a deployment
        site_type = self._get_resource_type_by_id(site_id)

        if RT.InstrumentSite == site_type:
            if 0 < len(self._find_stemming(site_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Site is still assigned to a deployment")

        if RT.PlatformSite == site_type:
            if 0 < len(self._find_stemming(site_id, PRED.hasSite, RT.PlatformDevice)):
                return self._make_fail("Device is still assigned a child platform site")
            if 0 < len(self._find_stemming(site_id, PRED.hasDevice, RT.InstrumentDevice)):
                return self._make_fail("Device is still hasSite a child instrument site")
            if 0 < len(self._find_stemming(site_id, PRED.hasDeployment, RT.Deployment)):
                return self._make_fail("Site is still assigned to a deployment")

        return self._make_fail("Wrong site resource type (got '%s')" % site_type)

    def precondition_delete(self, site_id):
        return self.lce_precondition_delete(site_id)


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
            return self._resource_lcstate_in(datasets[0], [LCS.DEPLOYED])


    def lce_precondition_retire(self, data_product_id):
        return self._make_pass()

    def lce_precondition_delete(self, data_product_id):
        if 0 < len(self._find_stemming(data_product_id, PRED.hasStream, RT.Stream)):
            return self._make_fail("Associated to a stream")
        if 0 < len(self._find_stemming(data_product_id, PRED.hasDataset, RT.Dataset)):
            return self._make_fail("Associated to a dataset")
        if 0 < len(self._find_stemming(data_product_id, PRED.hasInputDataProducer, RT.DataProducer)):
            return self._make_fail("Associated to an input data producer")
        if 0 < len(self._find_stemming(data_product_id, PRED.hasOutputDataProducer, RT.DataProducer)):
            return self._make_fail("Associated to an output data producer")
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
        return self._make_pass()

    def lce_precondition_delete(self, data_process_id):
        # todo:
        return self._make_pass()


# currently same as DataProcess
class DataProcessDefinitionPolicy(DataProcessPolicy):
    pass
