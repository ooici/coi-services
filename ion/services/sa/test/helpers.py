from pyon.public import IonObject
from pyon.public import RT
from pyon.util.log import log

from interface.objects import AttachmentType


_sa_test_helpers_ionobj_count = {}

def any_old(resource_type, extra_fields=None):
    """
    Create any old resource... a generic and unique object of a given type
    @param resource_type the resource type
    @param extra_fields dict of any extra fields to set
    """
    if not extra_fields:
        extra_fields = {}

    if resource_type not in _sa_test_helpers_ionobj_count:
        _sa_test_helpers_ionobj_count[resource_type] = 0

    _sa_test_helpers_ionobj_count[resource_type] = _sa_test_helpers_ionobj_count[resource_type] + 1

    name = "%s_%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    desc = "My %s #%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    log.debug("Creating any old %s IonObject (#%d)" % (resource_type, _sa_test_helpers_ionobj_count[resource_type]))

    ret = IonObject(resource_type, name=name, description=desc)
    
    #add any extra fields
    for k, v in extra_fields.iteritems():
        setattr(ret, k, v)

    return ret

def add_keyworded_attachment(resource_registry_client, resource_id, keywords, extra_fields=None):
    """
    create a generic attachment to a given resource -- a generic and unique attachment

    @param resource_registry_client a service client
    @param resource_id string the resource to get the attachment
    @param keywords list of string keywords
    @param extra_fields dict of extra fields to set.  "keywords" can be set here with no ill effects
    """
    
    if not extra_fields:
        extra_fields = {}

    if not "attachment_type" in extra_fields:
        extra_fields["attachment_type"] = AttachmentType.ASCII
        
    if not "content" in extra_fields:
        extra_fields["content"] = "these are contents"

    if not "keywords" in extra_fields:
        extra_fields["keywords"] = []

    for k in keywords:
        extra_fields["keywords"].append(k)

    ret = any_old(RT.Attachment, extra_fields)

    resource_registry_client.create_attachment(resource_id, ret)

    return ret
                    
    


