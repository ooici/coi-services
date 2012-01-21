#!/usr/bin/env python

__author__ = 'Stephen P. Henrie, Michael Meisinger'

from pyon.public import CFG, IonObject, log, RT, AT

from interface.services.coi.iorg_management_service import BaseOrgManagementService
from pyon.core.exception import Conflict, Inconsistent, NotFound
from pyon.public import AT, RT
from pyon.util.log import log

class OrgManagementService(BaseOrgManagementService):

    """
    Services to define and administer a facility (synonymous Org, community), to enroll/remove members and to provide
    access to the resources of an Org to enrolled or affiliated entities (identities). Contains contract
    and commitment repository
    """

    def create_org(self, org=None):
        """Persists the provided Org object. The id string returned
        is the internal id by which Org will be identified in the data store.

        @param org    Org
        @retval org_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        org_id, version = self.clients.resource_registry.create(org)
        return org_id

    def update_org(self, org=None):
        """Updates the provided Org object.  Throws NotFound exception if
        an existing version of Org is not found.  Throws Conflict if
        the provided Policy object is not based on the latest persisted
        version of the object.

        @param org    Org
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(org)

    def read_org(self, org_id=''):
        """Returns the Org object for the specified policy id.
        Throws exception if id does not match any persisted Policy
        objects.

        @param org_id    str
        @retval org    Org
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        return org

    def delete_org(self, org_id=''):
        """Permanently deletes Org object with the specified
        id. Throws exception if id does not match any persisted Policy.

        @param org_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        self.clients.resource_registry.delete(org_id)

    def affiliate_org(self, org_id='', affiliate_org_id=''):
        """Creates an association between multiple Orgs as an affiliation
        so that they may coordinate activities between them.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param affiliate_org_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org1 = self.clients.resource_registry.read(org_id)
        if not org1:
            raise NotFound("Org %s does not exist" % org_id)

        org2 = self.clients.resource_registry.read(affiliate_org_id)
        if not org2:
            raise NotFound("Org %s does not exist" % affiliate_org_id)

        aid = self.clients.resource_registry.create_association(org_id, AT.hasOrgAffiliation, affiliate_org_id)
        if not aid:
            return False

        return True


    def unaffiliate_org(self, org_id='', affiliate_org_id=''):
        """Removes an association between multiple Orgs as an affiliation.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param affiliate_org_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org1 = self.clients.resource_registry.read(org_id)
        if not org1:
            raise NotFound("Org %s does not exist" % org_id)

        org2 = self.clients.resource_registry.read(affiliate_org_id)
        if not org2:
            raise NotFound("Org %s does not exist" % affiliate_org_id)

        aid = self.clients.resource_registry.get_association(org_id, AT.hasOrgAffiliation, affiliate_org_id)
        if not aid:
            raise NotFound("The affiliation association between the specified Orgs is not found")

        aid2 = self.clients.resource_registry.delete_association(aid)
        if not aid2:
            return False

        return True

    def enroll_member(self, org_id='', user_id=''):
        """Enrolls a specified user into the specified Org so that they may find and negotiate to use resources
        of the Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        user = self.clients.resource_registry.read(user_id)
        if not user:
            raise NotFound("User %s does not exist" % user_id)

        aid = self.clients.resource_registry.create_association(org_id, AT.hasMembership, user_id)
        if not aid:
            return False

        return True

    def cancel_member_enrollment(self, org_id='', user_id=''):
        """Cancels the membership of a specifid user within the specified Org. Once canceled, the user will no longer
        have access to the resource of that Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        user = self.clients.resource_registry.read(user_id)
        if not user:
            raise NotFound("User %s does not exist" % user_id)

        aid = self.clients.resource_registry.get_association(org_id, AT.hasMembership, user_id)
        if not aid:
            raise NotFound("The membership association between the specified user and Org is not found")

        aid2 = self.clients.resource_registry.delete_association(aid)
        if not aid2:
            return False

        return True

    def is_enrolled(self, org_id='', user_id=''):
        """Returns True if the specified user_id is enrolled in the Org and False if not.
        Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param user_id    str
        @retval is_enrolled    bool
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        user = self.clients.resource_registry.read(user_id)
        if not user:
            raise NotFound("User %s does not exist" % user_id)

        aid = self.clients.resource_registry.get_association(org_id, AT.hasMembership, user_id)
        if not aid:
            return False

        return True
    

    def share_resource(self, org_id='', resource_id=''):
        """Share a specified resource with the specified Org. Once shared, the resource will be added to a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        aid = self.clients.resource_registry.create_association(org_id, AT.hasResource, resource_id)
        if not aid:
            return False

        return True

    def unshare_resource(self, org_id='', resource_id=''):
        """Unshare a specified resource with the specified Org. Once unshared, the resource will be removed from a directory
        of available resources within the Org. Throws a NotFound exception if neither id is found.

        @param org_id    str
        @param resource_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        resource = self.clients.resource_registry.read(resource_id)
        if not resource:
            raise NotFound("Resource %s does not exist" % resource_id)

        aid = self.clients.resource_registry.get_association(org_id, AT.hasMembership, resource_id)
        if not aid:
            raise NotFound("The shared association between the specified resource and Org is not found")

        aid2 = self.clients.resource_registry.delete_association(aid)
        if not aid2:
            return False

        return True



