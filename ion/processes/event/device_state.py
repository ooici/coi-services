#!/usr/bin/env python

"""Deals with persisting device state from events"""

import pprint

from pyon.core import bootstrap
from pyon.public import log, OT
from pyon.util.containers import get_ion_ts

STATE_PREFIX = "state_"
RECOGNIZED_EVENTS = {OT.ResourceAgentStateEvent,
                     OT.DeviceStatusEvent,
                     OT.DeviceAggregateStatusEvent,
                     OT.ResourceAgentResourceStateEvent,
                     OT.DeviceStatusAlertEvent}


class DeviceStateManager(object):

    def __init__(self, container=None):
        self.container = container or bootstrap.container_instance
        self.store = self.container.object_store

    def read_states(self, device_ids):
        state_ids = [STATE_PREFIX + rid for rid in device_ids]
        state_list = self.store.read_doc_mult(state_ids, strict=False)
        return state_list

    def process_events(self, event_list):
        """Callback interface for event processor"""
        self.persist_events_state(event_list)

    def persist_events_state(self, event_list):
        # Get events for each device
        events_by_dev = {}
        for evt in event_list:
            if evt.type_ not in RECOGNIZED_EVENTS:
                continue
            origin = evt.origin
            if origin:
                events_by_dev.setdefault(origin, []).append(evt)

        if not events_by_dev:
            return

        # Get existing states
        dev_ids = events_by_dev.keys()
        dev_states = self.read_states(dev_ids)
        states_by_dev = dict(zip(dev_ids, dev_states))
        existing_states = [did for did, ds in states_by_dev.iteritems() if ds is not None]
        log.debug("Found %s persisted device states", len(existing_states))

        # Update state using event
        for dev_id, dev_evts in events_by_dev.iteritems():
            dev_state = states_by_dev.get(dev_id, None)
            if dev_state is None:
                dev_state = self.create_device_state(STATE_PREFIX+dev_id, dev_id)
                states_by_dev[dev_id] = dev_state
            for evt in dev_evts:
                self.update_device_state(dev_state, dev_id, evt)

        # Persist events
        upd_dev_states = [ds for did, ds in states_by_dev.iteritems() if did in existing_states]
        self.store.update_doc_mult(upd_dev_states)

        new_dev_states = [ds for did, ds in states_by_dev.iteritems() if did not in existing_states]
        new_dev_state_ids = [STATE_PREFIX+ds["device_id"] for ds in new_dev_states]
        self.store.create_doc_mult(new_dev_states, object_ids=new_dev_state_ids)

        log.info("Processed and persisted %s status events for %s devices (%s upd/%s new)",
                 len(event_list), len(dev_ids), len(upd_dev_states), len(new_dev_states))

    def update_device_state(self, device_state, device_id, event):
        if event.type_ == OT.ResourceAgentStateEvent:
            device_state["state"]["prior"] = device_state["state"]["current"]
            device_state["state"]["current"] = event.state
            device_state["state"]["ts_changed"] = event.ts_created
        elif event.type_ == OT.ResourceAgentResourceStateEvent:
            device_state["res_state"]["prior"] = device_state["res_state"]["current"]
            device_state["res_state"]["current"] = event.state
            device_state["res_state"]["ts_changed"] = event.ts_created
        elif event.type_ == OT.DeviceStatusEvent:
            device_state["dev_status"]["status"] = event.status
            device_state["dev_status"]["sub_type"] = event.sub_type
            device_state["dev_status"]["time_stamps"] = event.time_stamps
            device_state["dev_status"]["valid_values"] = event.valid_values
            device_state["dev_status"]["values"] = event.values
            device_state["dev_status"]["ts_changed"] = event.ts_created
        elif event.type_ == OT.DeviceAggregateStatusEvent:
            device_state["agg_status"]["status"] = event.status
            device_state["agg_status"]["time_stamps"] = event.time_stamps
            device_state["agg_status"]["prev_status"] = event.prev_status
            device_state["agg_status"]["status_name"] = event.status_name
            device_state["agg_status"]["roll_up_status"] = event.roll_up_status
            device_state["agg_status"]["valid_values"] = event.valid_values
            device_state["agg_status"]["values"] = event.values
            device_state["agg_status"]["ts_changed"] = event.ts_created
        elif event.type_ == OT.DeviceStatusAlertEvent:
            device_state["dev_alert"]["status"] = event.status
            device_state["dev_alert"]["sub_type"] = event.sub_type
            device_state["dev_alert"]["name"] = event.name
            device_state["dev_alert"]["stream_name"] = event.stream_name
            device_state["dev_alert"]["time_stamps"] = event.time_stamps
            device_state["dev_alert"]["valid_values"] = event.valid_values
            device_state["dev_alert"]["values"] = event.values
            device_state["dev_alert"]["value_id"] = event.value_id
            device_state["dev_alert"]["ts_changed"] = event.ts_created

        device_state["ts_updated"] = get_ion_ts()

    def create_device_state(self, state_id, device_id):
        cur_ts = get_ion_ts()
        new_state = dict(device_id=device_id,
                         ts_created=cur_ts,
                         ts_updated=cur_ts,
                         state=dict(current="",
                                    prior="",
                                    ts_changed=""),
                         res_state=dict(current="",
                                        prior="",
                                        ts_changed=""),
                         dev_alert=dict(status=0,
                                        time_stamps=[],
                                        name="",
                                        stream_name="",
                                        valid_values=[],
                                        values=[],
                                        value_id="",
                                        ts_changed=""),
                         dev_status=dict(status=0,
                                         time_stamps=[],
                                         valid_values=[],
                                         values=[],
                                         ts_changed=""),
                         agg_status=dict(status=0,
                                         time_stamps=[],
                                         prev_status=0,
                                         status_name=0,
                                         roll_up_status=False,
                                         valid_values=[],
                                         values=[],
                                         ts_changed=""),
                         )
        return new_state
