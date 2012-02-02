from pyon.public import IonObject
from pyon.util.log import log

_sa_test_helpers_ionobj_count = {}

def any_old(resource_type, extra_fields={}):
    """
    Create any old resource... a generic and unique object of a given type
    @param resource_type the resource type
    @param extra_fields dict of any extra fields to set
    """
    if resource_type not in _sa_test_helpers_ionobj_count:
        _sa_test_helpers_ionobj_count[resource_type] = 0

    _sa_test_helpers_ionobj_count[resource_type] = _sa_test_helpers_ionobj_count[resource_type] + 1

    name = "%s %d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    desc = "My %s #%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    log.debug("Creating any old %s IonObject (#%d)" % (resource_type, _sa_test_helpers_ionobj_count[resource_type]))

    ret = IonObject(resource_type, name=name, description=desc)
    
    #add any extra fields
    for k, v in extra_fields.iteritems():
        setattr(ret, k, v)

    return ret



