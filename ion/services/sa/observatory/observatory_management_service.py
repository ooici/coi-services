#!/usr/bin/env python

'''
@package ion.services.sa.observatory Implementation of IObservatoryManagementService interface
@file ion/services/sa/observatory/observatory_management_service.py
@author
@brief Observatory Management Service to keep track of observatories sites, logical platform sites, instrument sites,
and the relationships between them
'''




from pyon.core.exception import NotFound, BadRequest
from pyon.public import CFG, IonObject, log, RT, PRED, LCS, LCE

#from pyon.util.log import log
from ion.services.sa.resource_impl.org_impl import OrgImpl
from ion.services.sa.resource_impl.observatory_impl import ObservatoryImpl
from ion.services.sa.resource_impl.subsite_impl import SubsiteImpl
from ion.services.sa.resource_impl.platform_site_impl import PlatformSiteImpl
from ion.services.sa.resource_impl.instrument_site_impl import InstrumentSiteImpl

#for logical/physical associations, it makes sense to search from MFMS
from ion.services.sa.resource_impl.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.resource_impl.platform_device_impl import PlatformDeviceImpl

from interface.services.sa.iobservatory_management_service import BaseObservatoryManagementService
from interface.objects import OrgTypeEnum

INSTRUMENT_OPERATOR_ROLE  = 'INSTRUMENT_OPERATOR'
OBSERVATORY_OPERATOR_ROLE = 'OBSERVATORY_OPERATOR'
DATA_OPERATOR_ROLE        = 'DATA_OPERATOR'


class ObservatoryManagementService(BaseObservatoryManagementService):


    def on_init(self):
        IonObject("Resource")  # suppress pyflakes error
        CFG, log, RT, PRED, LCS, LCE, NotFound, BadRequest, log  #suppress pyflakes errors about "unused import"

        self.override_clients(self.clients)


    def override_clients(self, new_clients):
        """
        Replaces the service clients with a new set of them... and makes sure they go to the right places
        """

        #shortcut names for the import sub-services
        if hasattr(self.clients, "resource_registry"):
            self.RR    = self.clients.resource_registry
            
        if hasattr(self.clients, "instrument_management"):
            self.IMS   = self.clients.instrument_management
            

        #farm everything out to the impls

        self.org              = OrgImpl(self.clients)
        self.observatory      = ObservatoryImpl(self.clients)
        self.subsite          = SubsiteImpl(self.clients)
        self.platform_site    = PlatformSiteImpl(self.clients)
        self.instrument_site  = InstrumentSiteImpl(self.clients)

        self.instrument_device   = InstrumentDeviceImpl(self.clients)
        self.platform_device     = PlatformDeviceImpl(self.clients)



    
    ##########################################################################
    #
    # CRUD OPS
    #
    ##########################################################################


    def create_marine_facility(self, org=None):
        """Create an Org (domain of authority) that realizes a marine facility. This Org will have
        set up roles for a marine facility. Shared resources, such as a device can only be
        registered in one marine facility Org, and additionally in many virtual observatory Orgs. The
        marine facility operators will have more extensive permissions and will supercede virtual
        observatory commands

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        log.debug("ObservatoryManagementService.create_marine_facility(): %s" % org)
        
        # create the org
        org.org_type = OrgTypeEnum.MARINE_FACILITY
        org_id = self.clients.org_management.create_org(org)

        #Instantiate initial set of User Roles for this marine facility
        instrument_operator_role = IonObject(RT.UserRole, name=INSTRUMENT_OPERATOR_ROLE, 
                                             label='Instrument Operator', description='Marine Facility Instrument Operator')
        self.clients.org_management.add_user_role(org_id, instrument_operator_role)
        observatory_operator_role = IonObject(RT.UserRole, name=OBSERVATORY_OPERATOR_ROLE, 
                                             label='Observatory Operator', description='Marine Facility Observatory Operator')
        self.clients.org_management.add_user_role(org_id, observatory_operator_role)
        data_operator_role = IonObject(RT.UserRole, name=DATA_OPERATOR_ROLE, 
                                             label='Data Operator', description='Marine Facility Data Operator')
        self.clients.org_management.add_user_role(org_id, data_operator_role)
        
        return org_id

    def create_virtual_observatory(self, org=None):
        """Create an Org (domain of authority) that realizes a virtual observatory. This Org will have
        set up roles for a virtual observatory. Shared resources, such as a device can only be
        registered in one marine facility Org, and additionally in many virtual observatory Orgs. The
        marine facility operators will have more extensive permissions and will supercede virtual
        observatory commands

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        log.debug("ObservatoryManagementService.create_virtual_observatory(): %s" % org)

        # create the org
        org.org_type = OrgTypeEnum.VIRTUAL_OBSERVATORY
        org_id = self.clients.org_management.create_org(org)

        return org_id


    def create_observatory(self, observatory=None):
        """Create a Observatory resource. An observatory  is coupled
        with one Org. The Org is created and associated as part of this call.

        @param observatory    Observatory
        @retval observatory_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """

        # create the marine facility
        observatory_id = self.observatory.create_one(observatory)

        return observatory_id


    def read_observatory(self, observatory_id=''):
        """Read a Observatory resource

        @param observatory_id    str
        @retval observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        return self.observatory.read_one(observatory_id)


    def update_observatory(self, observatory=None):
        """Update a Observatory resource

        @param observatory    Observatory
        @throws NotFound    object with specified id does not exist
        """
        return self.observatory.update_one(observatory)


    def delete_observatory(self, observatory_id=''):
        """Delete a Observatory resource

        @param observatory_id    str
        @throws NotFound    object with specified id does not exist
        """
        
        # find the org for this MF
        org_ids, _ = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasObservatory, observatory_id, id_only=True)
        if len(org_ids) == 0:
            log.warn("ObservatoryManagementService.delete_observatory(): no org for MF " + observatory_id)
        else:
            if len(org_ids) > 1:
                log.warn("ObservatoryManagementService.delete_observatory(): more than 1 org for MF " + observatory_id)
                # TODO: delete the others and/or raise exception???
            # delete the set of User Roles for this marine facility that this service created
            self.clients.org_management.remove_user_role(org_ids[0], INSTRUMENT_OPERATOR_ROLE)
            self.clients.org_management.remove_user_role(org_ids[0], OBSERVATORY_OPERATOR_ROLE)
            self.clients.org_management.remove_user_role(org_ids[0], DATA_OPERATOR_ROLE)
            # delete the org
            self.clients.org_management.delete_org(org_ids[0])
        
        return self.observatory.delete_one(observatory_id)


    def create_subsite(self, subsite=None, parent_id=''):
        """Create a Subsite resource. A subsite is a frame of reference within an observatory. Its parent is
        either the observatory or another subsite.

        @param subsite    Subsite
        @param parent_id    str
        @retval subsite_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        subsite_id = self.subsite.create_one(subsite)

        if parent_id:
            self.subsite.link_parent(subsite_id, parent_id)

        return subsite_id

    def read_subsite(self, subsite_id=''):
        """Read a Subsite resource

        @param subsite_id    str
        @retval subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        return self.subsite.read_one(subsite_id)

    def update_subsite(self, subsite=None):
        """Update a Subsite resource

        @param subsite    Subsite
        @throws NotFound    object with specified id does not exist
        """
        return self.subsite.update_one(subsite)

    def delete_subsite(self, subsite_id=''):
        """Delete a subsite resource, removes assocations to parents

        @param subsite_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.subsite.delete_one(subsite_id)



    def create_platform_site(self, platform_site=None, parent_id=''):
        """Create a PlatformSite resource. A platform_site is a frame of reference within an observatory. Its parent is
        either the observatory or another platform_site.

        @param platform_site    PlatformSite
        @param parent_id    str
        @retval platform_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        platform_site_id = self.platform_site.create_one(platform_site)

        if parent_id:
            self.platform_site.link_parent(platform_site_id, parent_id)

        return platform_site_id


    def read_platform_site(self, platform_site_id=''):
        """Read a PlatformSite resource

        @param platform_site_id    str
        @retval platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        return self.platform_site.read_one(platform_site_id)

    def update_platform_site(self, platform_site=None):
        """Update a PlatformSite resource

        @param platform_site    PlatformSite
        @throws NotFound    object with specified id does not exist
        """
        return self.platform_site.update_one(platform_site)


    def delete_platform_site(self, platform_site_id=''):
        """Delete a PlatformSite resource, removes assocations to parents

        @param platform_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.platform_site.delete_one(platform_site_id)



    def create_instrument_site(self, instrument_site=None, parent_id=''):
        """Create a InstrumentSite resource. A instrument_site is a frame of reference within an observatory. Its parent is
        either the observatory or another instrument_site.

        @param instrument_site    InstrumentSite
        @param parent_id    str
        @retval instrument_site_id    str
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        """
        instrument_site_id = self.instrument_site.create_one(instrument_site)

        if parent_id:
            self.instrument_site.link_parent(instrument_site_id, parent_id)

        return instrument_site_id

    def read_instrument_site(self, instrument_site_id=''):
        """Read a InstrumentSite resource

        @param instrument_site_id    str
        @retval instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        return self.instrument_site.read_one(instrument_site_id)

    def update_instrument_site(self, instrument_site=None):
        """Update a InstrumentSite resource

        @param instrument_site    InstrumentSite
        @throws NotFound    object with specified id does not exist
        """
        return self.instrument_site.update_one(instrument_site)

    def delete_instrument_site(self, instrument_site_id=''):
        """Delete a InstrumentSite resource, removes assocations to parents

        @param instrument_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        self.instrument_site.delete_one(instrument_site_id)



    


    ############################
    #
    #  ASSOCIATIONS
    #
    ############################


    def assign_site_to_site(self, child_site_id='', parent_site_id=''):
        """Connects a child site (any subtype) to a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        parent_site_obj = self.subsite.read_one(parent_site_id)
        parent_site_type = parent_site_obj._get_type()

        self.subsite.link_site(parent_site_id, child_site_id)

        # TODO: MM - Commented out checks - too restrictive
        #if RT.Subsite == parent_site_type:
        #    self.subsite.link_site(parent_site_id, child_site_id)
        #elif RT.PlatformSite == parent_site_type:
        #    self.platform_site.link_site(parent_site_id, child_site_id)
        #else:
        #    raise BadRequest("Tried to assign a child site to a %s resource" % parent_site_type)



    def unassign_site_from_site(self, child_site_id='', parent_site_id=''):
        """Disconnects a child site (any subtype) from a parent site (any subtype)

        @param child_site_id    str
        @param parent_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        parent_site_obj = self.subsite.read_one(parent_site_id)
        parent_site_type = parent_site_obj._get_type()

        if RT.Subsite == parent_site_type:
            self.subsite.unlink_site(parent_site_id, child_site_id)
        elif RT.PlatformSite == parent_site_type:
            self.platform_site.unlink_site(parent_site_id, child_site_id)
        else:
            raise BadRequest("Tried to unassign a child site from a %s resource" % parent_site_type)


    def assign_instrument_device_to_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        """Connects a instrument device to instrument site

        @param instrument_device_id    str
        @param instrument_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        
        self.instrument_site.link_site(instrument_device_id, instrument_site_id)


    def unassign_instrument_device_from_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        """Connects a instrument device to instrument site

        @param instrument_device_id    str
        @param instrument_site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.instrument_site.unlink_site(instrument_device_id, instrument_site_id)


    def assign_platform_device_to_platform_site(self, platform_device_id='', platform_site_id=''):
        """Connects a platform device to platform site

        @param platform_device_id    str
        @param platform_site_id    str
        @throws NotFound    object with specified id does not exist
        """
        
        self.platform_site.link_site(platform_device_id, platform_site_id)


    def unassign_platform_device_from_platform_site(self, platform_device_id='', platform_site_id=''):
        """Connects a platform device to platform site

        @param platform_device_id    str
        @param platform_site_id    str
        @throws NotFound    object with specified id does not exist
        """

        self.platform_site.unlink_site(platform_device_id, platform_site_id)


    def assign_site_to_observatory(self, site_id='', observatory_id=''):
        self.observatory.link_site(observatory_id, site_id)


    def unassign_site_from_observatory(self, site_id="", observatory_id=''):
        self.observatory.unlink_site(observatory_id, site_id)


    def assign_instrument_model_to_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.instrument_site.link_model(instrument_site_id, instrument_model_id)

    def unassign_instrument_model_from_instrument_site(self, instrument_model_id='', instrument_site_id=''):
        self.instrument_site.unlink_model(instrument_site_id, instrument_model_id)

    def assign_platform_model_to_platform_site(self, platform_model_id='', platform_site_id=''):
        self.platform_site.link_model(platform_site_id, platform_model_id)

    def unassign_platform_model_from_platform_site(self, platform_model_id='', platform_site_id=''):
        self.platform_site.link_model(platform_site_id, platform_model_id)

    

    def assign_resource_to_observatory_org(self, resource_id='', org_id=''):
        if not org_id:
            raise BadRequest("Org id not given")
        if not resource_id:
            raise BadRequest("Resource id not given")

        log.debug("assign_resource_to_observatory_org: org_id=%s, resource_id=%s " % (org_id, resource_id))
        self.clients.org_management.share_resource(org_id, resource_id)


    def unassign_resource_from_observatory_org(self, resource_id='', org_id=''):
        if not org_id:
            raise BadRequest("Org id not given")
        if not resource_id:
            raise BadRequest("Resource id not given")

        self.clients.org_management.unshare_resource(org_id, resource_id)







    ##########################################################################
    #
    # DEPLOYMENTS
    #
    ##########################################################################



    def deploy_instrument_device_to_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        self.instrument_device.link_deployment(instrument_device_id, instrument_site_id)

    def undeploy_instrument_device_from_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        self.instrument_device.unlink_deployment(instrument_device_id, instrument_site_id)

    def deploy_platform_device_to_platform_site(self, platform_device_id='', platform_site_id=''):
        self.platform_device.link_deployment(platform_device_id, platform_site_id)

    def undeploy_platform_device_from_platform_site(self, platform_device_id='', platform_site_id=''):
        self.platform_device.unlink_deployment(platform_device_id, platform_site_id)


    def deploy_as_primary_instrument_device_to_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        self.instrument_device.assign_primary_deployment(instrument_device_id, instrument_site_id)

    def undeploy_primary_instrument_device_from_instrument_site(self, instrument_device_id='', instrument_site_id=''):
        self.instrument_device.unassign_primary_deployment(instrument_device_id, instrument_site_id)

    def deploy_as_primary_platform_device_to_platform_site(self, platform_device_id='', platform_site_id=''):
        self.platform_device.link_primary_deployment(platform_device_id, platform_site_id)

    def undeploy_primary_platform_device_from_platform_site(self, platform_device_id='', platform_site_id=''):
        self.platform_device.unlink_primary_deployment(platform_device_id, platform_site_id)



    ##########################################################################
    #
    # FIND OPS
    #
    ##########################################################################



    def find_observatories(self, filters=None):
        """

        """
        return self.observatory.find_some(filters)


    def find_subsites(self, filters=None):
        """

        """
        return self.site.find_some(filters)


    def find_instrument_sites(self, filters=None):
        """

        """
        return self.instrument_site.find_some(filters)


    def find_platform_sites(self, filters=None):
        """

        """
        return self.platform_site.find_some(filters)


    def find_org_by_observatory(self, observatory_id=''):
        """

        """
        return self.org.find_having_observatory(observatory_id)


        
    def find_data_product_by_platform_site(self, platform_site_id=''):
        ret = []
        for i in self.find_instrument_device_by_platform_site(platform_site_id):
            for dp in self.IMS.find_data_product_by_instrument_device(i):
                if not dp in ret:
                    ret.append(dp)

        return ret

    


    def find_related_frames_of_reference(self, input_resource_id='', output_resource_type_list=None):

        # the relative depth of each resource type in our tree
        depth = {RT.InstrumentSite: 4,
                 RT.PlatformSite: 3,
                 RT.Subsite: 2,
                 RT.Observatory: 1,
                 }

        input_obj  = self.RR.read(input_resource_id)
        input_type = input_obj._get_type()

        #input type checking
        if not input_type in depth:
            raise BadRequest("Input resource type (got %s) must be one of %s" % 
                             (input_type, str(depth.keys())))
        for t in output_resource_type_list:
            if not t in depth:
                raise BadRequest("Output resource types (got %s) must be one of %s" %
                                 (str(output_resource_type_list), str(depth.keys())))

                             

        subordinates = [x for x in output_resource_type_list if depth[x] > depth[input_type]]
        superiors    = [x for x in output_resource_type_list if depth[x] < depth[input_type]]

        acc = {}
        acc[input_type] = [input_obj]


        if subordinates:
            # figure out the actual depth we need to go
            deepest_type = input_type #initial value
            for output_type in output_resource_type_list:
                if depth[deepest_type] < depth[output_type]:
                    deepest_type = output_type

            log.debug("Deepest level for search will be '%s'" % deepest_type)

            acc = self._traverse_entity_tree(acc, input_type, deepest_type, True)


        if superiors:
            highest_type = input_type #initial value

            for output_type in output_resource_type_list:
                if depth[highest_type] > depth[output_type]:
                    highest_type = output_type

            log.debug("Highest level for search will be '%s'" % highest_type)

            acc = self._traverse_entity_tree(acc, highest_type, input_type, False)

        # Don't include input type in response            
        #TODO: maybe just remove the input resource id 
        if input_type in acc:
            acc.pop(input_type)            
        return acc
                    

    def _traverse_entity_tree(self, acc, top_type, bottom_type, downward):

        call_list = self._build_call_list(top_type, bottom_type, downward)

        # reverse the list and start calling functions
        if downward:
            call_list.reverse()
            for (p, c) in call_list:
                acc = self._find_subordinate(acc, p, c)
        else:
            for (p, c) in call_list:
                acc = self._find_superior(acc, p, c)

        return acc


    def _build_call_list(self, top_type, bottom_type, downward):
        # the possible parent types that a resource can have
        hierarchy_dependencies =  {
            RT.InstrumentSite: [RT.PlatformSite],
            RT.PlatformSite:   [RT.PlatformSite, RT.Subsite],
            RT.Subsite:        [RT.Subsite, RT.Observatory],
            }

        call_list = []
        target_type = bottom_type
        while True:
            if downward and (target_type == top_type):
                return call_list

            if (not downward) and (target_type == top_type):
                if not (target_type in hierarchy_dependencies and
                        target_type in hierarchy_dependencies[target_type]):
                    return call_list

            for requisite_type in hierarchy_dependencies[target_type]:
                #should cause errors if they stray from allowed inputs
                call_list.append((requisite_type, target_type))

                if not downward and top_type == requisite_type == target_type:
                    return call_list
            
            #latest solved type is the latest result
            target_type = requisite_type
        
                
            


    def _find_subordinate(self, acc, parent_type, child_type):
        #acc is an accumulated dictionary

        if not child_type in acc:
            acc[child_type] = []
            
        find_fn = {
            (RT.Observatory, RT.Subsite):         self.observatory.find_stemming_site,
            (RT.Subsite, RT.Subsite):             self.subsite.find_stemming_subsite,
            (RT.Subsite, RT.PlatformSite):        self.subsite.find_stemming_platform_site,
            (RT.PlatformSite, RT.PlatformSite):   self.platform_site.find_stemming_platform_site,
            (RT.PlatformSite, RT.InstrumentSite): self.platform_site.find_stemming_instrument_site,
            }[(parent_type, child_type)]
        
        log.debug("Subordinates: '%s'x%d->'%s'" % (parent_type, len(acc[parent_type]), child_type))

        #for all parents in the acc, add all their children
        for parent_obj in acc[parent_type]:
            parent_id = parent_obj._id
            for child_obj in find_fn(parent_id):
                acc[child_type].append(child_obj)

        return acc



    def _find_superior(self, acc, parent_type, child_type):
        # acc is an accumualted dictionary

        if not parent_type in acc:
            acc[parent_type] = []

        #log.debug("Superiors: '%s'->'%s'" % (parent_type, child_type))
        #if True:
        #    return acc
            
        find_fn = {
            (RT.Observatory, RT.Subsite):         self.observatory.find_having_site,
            (RT.Subsite, RT.Subsite):             self.subsite.find_having_site,
            (RT.Subsite, RT.PlatformSite):        self.subsite.find_having_site,
            (RT.PlatformSite, RT.PlatformSite):   self.platform_site.find_having_site,
            (RT.PlatformSite, RT.InstrumentSite): self.platform_site.find_having_site,
            }[(parent_type, child_type)]
        
        log.debug("Superiors: '%s'->'%s'x%d" % (parent_type, child_type, len(acc[child_type])))

        #for all children in the acc, add all their parents
        for child_obj in acc[child_type]:
            child_id = child_obj._id
            for parent_obj in find_fn(child_id):
                acc[parent_obj._get_type()].append(parent_obj)

        return acc





"""
  
    def find_data_product_by_site(self, site_id=''):
        ret = []
        for i in self.find_instrument_device_by_site(site_id):
            for dp in self.IMS.find_data_product_by_instrument_device(i):
                if not dp in ret:
                    ret.append(dp)

        return ret
  
    def find_data_product_by_marine_facility(self, marine_facility_id=''):
        ret = []
        for i in self.find_instrument_device_by_marine_facility(marine_facility_id):
            for dp in self.IMS.find_data_product_by_instrument_device(i):
                if not dp in ret:
                    ret.append(dp)

        return ret
  


"""
