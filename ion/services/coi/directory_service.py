#!/usr/bin/env python

__author__ = 'Thomas R. Lennan, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest
from pyon.ion.directory import Directory

from interface.services.coi.idirectory_service import BaseDirectoryService


class DirectoryService(BaseDirectoryService):
    """
    Provides a directory of services and other resources specific to an Org.
    The directory is backed by a persistent datastore and is system/Org wide.
    """

    def on_init(self):
        self.directory = self.container.directory

        # For easier interactive debugging
        self.dss = None
        self.ds = self.directory.dir_store
        try:
            self.dss = self.directory.dir_store.server[self.directory.dir_store.datastore_name]
        except Exception:
            pass

    def register(self, parent='/', key='', attributes={}):
        return self.directory.register(parent, key, **attributes)

    def unregister(self, parent='/', key=''):
        return self.directory.unregister(parent, key)

    def lookup(self, qualified_key=''):
        return self.directory.lookup(qualified_key)

    def find(self, parent='/', pattern=''):
        raise BadRequest("Not Implemented")

    _restypes = [
        'UIInternalResourceType',
        'UIInformationLevel',
        'UIScreenLabel',
        'UIAttribute',
        'UIBlock',
        'UIGroup',
        'UIRepresentation',
        'UIResourceType',
        'UIView',
        'UIBlockAttribute',
        'UIBlockRepresentation',
        'UIGroupBlock',
        'UIViewGroup']

    _assoctypes = [
        'hasUIGroup',
        'hasUIScreenLabel',
        'hasUIBlockAttribute',
        'hasUIAttribute',
        'hasUIBlockRepresentation',
        'hasUIRepresentation',
        'hasUIGroupBlock',
        'hasUIBlock',
        'hasUIViewGroup',
        'hasUISupertype',
        'hasUIResource']

    def get_ui_specs(self, user_id=''):
        rr = self.container.resource_registry
        ui_specs = {}
        ui_objs = {}
        ui_specs['objects'] = ui_objs
        for rt in self._restypes:
            res_list,_ = rr.find_resources(rt, id_only=False)
            res_ids = [res_obj._id for res_obj in res_list]
            ui_specs[rt] = res_ids
            for res_obj in res_list:
                ui_objs[res_obj._id] = res_obj

        ui_sub_assoc = {}
        ui_specs['associated_to'] = ui_sub_assoc
        ui_obj_assoc = {}
        ui_specs['associated_from'] = ui_obj_assoc

        for pred in self._assoctypes:
            assoc_list = rr.find_associations(predicate=pred, id_only=False)
            #assoc_ids = [assoc_obj._id for assoc_obj in assoc_list]
            #ui_specs[pred] = assoc_ids
            for assoc_obj in assoc_list:
                #ui_objs[assoc_obj._id] = assoc_obj

                if assoc_obj.s in ui_sub_assoc:
                    ui_sub_assoc[assoc_obj.s].append([pred, assoc_obj.o])
                else:
                    ui_sub_assoc[assoc_obj.s] = [[pred, assoc_obj.o]]

                if assoc_obj.o in ui_obj_assoc:
                    ui_obj_assoc[assoc_obj.o].append([pred, assoc_obj.s])
                else:
                    ui_obj_assoc[assoc_obj.o] = [[pred, assoc_obj.s]]

        ui_views = {}
        ui_specs['views'] = ui_views
        for obj in ui_objs.values():
            if obj._get_type() == "UIView":
                group_list = []
                ui_views[obj._id] = group_list
                for viewassoc_p, group_id in ui_sub_assoc.get(obj._id, []):
                    if viewassoc_p == "hasUIGroup":
                        # TODO: Sort by position
                        block_list = []
                        group_list.append([group_id, block_list])
                        for groupassoc_p, block_id in ui_sub_assoc.get(group_id, []):
                            if groupassoc_p == "hasUIBlock":
                                # TODO: Sort by position
                                attr_list = []
                                block_list.append([block_id, attr_list])
                                for blockassoc_p, attr_id in ui_sub_assoc.get(block_id, []):
                                    if blockassoc_p == "hasUIAttribute":
                                        # TODO: Sort by position
                                        attr_list.append(attr_id)

        return ui_specs

