'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/utility/resource_tree.py
@description Builds a D3 JSON Hierarchy Tree based on a resource
'''
from pyon.container.cc import Container
from ion.services.dm.utility.jsonify import JSONtree as jt

tree_depth_max = 5

def build(resource_id, depth=0):
    ''' Constructs a JSONtree for the specified resource.

    The tree is built downward so all associations from this resource down are included.
    '''
    if isinstance(resource_id, unicode):
        resource_id = resource_id.encode('ascii')
    rr_cli = Container.instance.resource_registry

    resource = rr_cli.read(resource_id)
    root = jt(resource.name or resource._id)
    root.id = resource_id
    obj_list, assoc_list = rr_cli.find_objects(subject=resource_id,id_only=True)

    if not obj_list:
        root.leaf = True
        return root

    for obj,assoc in zip(obj_list,assoc_list):
        if obj and not (depth > tree_depth_max):
            root.add_child(build(obj,depth+1),assoc.p)

    return root
