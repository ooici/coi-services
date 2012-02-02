from pyon.public import IonObject
from pyon.util.log import log

_sa_test_helpers_ionobj_count = {}

def any_old(resource_type):
    """
    Create any old resource... a generic and unique object of a given type
    @param resource_type the resource type
    """
    if resource_type not in _sa_test_helpers_ionobj_count:
        _sa_test_helpers_ionobj_count[resource_type] = 0

    _sa_test_helpers_ionobj_count[resource_type] = _sa_test_helpers_ionobj_count[resource_type] + 1

    name = "%s %d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    desc = "My %s #%d" % (resource_type, _sa_test_helpers_ionobj_count[resource_type])
    log.debug("Creating any old %s IonObject (#%d)" % (resource_type, _sa_test_helpers_ionobj_count[resource_type]))

    return IonObject(resource_type, name=name, description=desc)


