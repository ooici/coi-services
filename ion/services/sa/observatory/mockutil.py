#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from mock import Mock, patch
from nose.plugins.attrib import attr

from pyon.ion.identifier import create_unique_resource_id, create_unique_association_id, create_unique_event_id
from pyon.public import IonObject
from pyon.util.containers import get_ion_ts

class MockUtil(object):
    def __init__(self):
        self.res_objs = {}
        self.res_id_list = []
        self.associations = []
        self.events = []

    def create_process_mock(self):
        self.process_mock = Mock()
        return self.process_mock

    def create_container_mock(self, capabilities=None):
        self.container_mock = Mock()
        self.container_mock.resource_registry = Mock()

        return self.container_mock

    def create_device_status_manager_mock(self):
        self.dsm_mock = Mock()
        self.dsm_mock.read_states = Mock()

        return self.dsm_mock

    def load_mock_resources(self, res_list):
        for res_entry in res_list:
            name = res_entry.get('name', 'NO_NAME')
            lcstate = res_entry.get('lcstate', 'DEPLOYED')
            lcav = res_entry.get('availability', 'AVAILABLE')
            attr = res_entry.get('attr', {})
            res_id = create_unique_resource_id()
            res_id = res_entry.get('_id', res_id)
            res_obj = IonObject(res_entry['rt'], name=name, **attr)
            res_obj._id = res_id
            res_obj.lcstate = lcstate
            res_obj.availability = lcav
            res_obj.ts_created = get_ion_ts()
            res_obj.ts_updated = res_obj.ts_created

            self.res_objs[res_id] = res_obj
            self.res_id_list.append(res_id)

        self.container_mock.resource_registry.read_mult = Mock()
        def side_effect(res_id_list):
            return [self.res_objs[res_id] for res_id in res_id_list]
        self.container_mock.resource_registry.read_mult.side_effect = side_effect

    def load_mock_associations(self, assoc_list):
        for assoc_entry in assoc_list:
            sid = assoc_entry[0]
            oid = assoc_entry[2]
            st = self.res_objs[sid]._get_type()
            ot = self.res_objs[oid]._get_type()
            ass_obj = IonObject('Association', s=sid, st=st, o=oid, ot=ot, p=assoc_entry[1], ts=get_ion_ts())
            ass_obj._id = "%s_%s_%s" % (sid, assoc_entry[1], oid)
            self.associations.append(ass_obj)

        self.container_mock.resource_registry.find_associations = Mock()
        def side_effect(subject=None, predicate=None, obj=None, **kwargs):
            if predicate:
                assocs = [assoc for assoc in self.associations if assoc.p == predicate]
            else:
                assocs = self.associations
            return assocs
        self.container_mock.resource_registry.find_associations.side_effect = side_effect

    def load_mock_events(self, event_list):
        is_first = len(self.events) == 0

        for cnt, event_entry in enumerate(event_list):
            origin = event_entry.get('o', None)
            origin_type = event_entry.get('ot', None)
            sub_type = event_entry.get('st', None)
            attr = event_entry.get('attr', {})
            evt_obj = IonObject(event_entry['et'], origin=origin, origin_type=origin_type, sub_type=sub_type, ts_created=get_ion_ts(), **attr)
            evt_obj._id = str(cnt)
            self.events.append(evt_obj)

        if is_first:
            self.container_mock.event_repository.find_events = Mock()
            def side_effect(event_type=None, **kwargs):
                return [(evt._id, None, evt) for evt in reversed(self.events) if evt.type_ == event_type]
            self.container_mock.event_repository.find_events.side_effect = side_effect

    def assign_mockres_find_objects(self, filter_predicate=None):
        self.container_mock.resource_registry.find_objects = Mock()
        assocs = self.associations
        if filter_predicate:
            assocs = [assoc for assoc in self.associations if assoc.p == filter_predicate]
        res_ids = [a.o for a in assocs]
        self.container_mock.resource_registry.find_objects.return_value = [res_ids, assocs]

    def load_mock_device_statuses(self, status_by_device):
        def side_effect(device_list=None, **kwargs):
            return [status_by_device.get(dev_id, None) for dev_id in device_list]
        self.dsm_mock.read_states.side_effect = side_effect
