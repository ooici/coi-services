#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_simulator
@file    ion/agents/platform/oms/simulator/oms_simulator.py
@author  Carlos Rueda
@brief   OMS simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.agents.platform.oms.oms_client import OmsClient
from ion.agents.platform.oms.oms_client import VALID_PORT_ATTRIBUTES
from ion.agents.platform.oms.oms_client import InvalidResponse
from ion.agents.platform.util.network import NNode
from ion.agents.platform.oms.simulator.oms_alarms import AlarmInfo
from ion.agents.platform.oms.simulator.oms_alarms import AlarmNotifier
from ion.agents.platform.oms.simulator.oms_alarms import AlarmGenerator

import yaml
import time

from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()


class OmsSimulator(OmsClient):
    """
    Implementation of OmsClient for testing purposes.
    """

    def __init__(self, yaml_filename='ion/agents/platform/oms/simulator/network.yml'):
        pyobj = yaml.load(file(yaml_filename))

        self._get_platform_types(pyobj)

        self._build_network(pyobj)

        self._next_value = 990000

        # registered alarm listeners: {url: [(alarm_type, reg_time), ...], ...},
        # where reg_time is the time of (latest) registration.
        # NOTE: for simplicity, we don't keep info about unregistered listeners
        self._reg_alarm_listeners = {}

        self._alarm_notifier = AlarmNotifier()
        self._alarm_generator = AlarmGenerator(self._alarm_notifier)
        self._alarm_generator.start()
        log.debug("alarm generator started")

    def _get_platform_types(self, pyobj):
        """
        Constructs:
          - self._platform_types: {platform_type : description} map
        """
        assert 'platform_types' in pyobj
        self._platform_types = {}
        for ptypeObj in pyobj["platform_types"]:
            assert 'platform_type' in ptypeObj
            assert 'description' in ptypeObj
            platform_type = ptypeObj['platform_type']
            description = ptypeObj['description']
            self._platform_types[platform_type] = description


    def _build_network(self, pyobj):
        """
        Constructs:
          - self._idp: {platform_id : NNode} map
          - self._dummy_root: The "dummy" root node; its children are the actual roots.
        """
        assert 'network' in pyobj
        self._idp = {}
        self._dummy_root = None

        def create_node(platform_id, platform_types=None):
            assert not platform_id in self._idp
            pn = NNode(platform_id, platform_types)
            self._idp[platform_id] = pn
            return pn

        def build_node(platObj, parent_node):
            assert 'platform_id' in platObj
            assert 'platform_types' in platObj
            platform_id = platObj['platform_id']
            platform_types = platObj['platform_types']
            for platform_type in platform_types:
                assert platform_type in self._platform_types
            ports = platObj['ports'] if 'ports' in platObj else []
            attrs = platObj['attrs'] if 'attrs' in platObj else []
            pn = create_node(platform_id, platform_types)
            parent_node.add_subplatform(pn)
            pn.set_ports(ports)
            pn.set_attributes(attrs)
            if 'subplatforms' in platObj:
                for subplat in platObj['subplatforms']:
                    subplat_id = subplat['platform_id']
                    if subplat_id in pn.subplatforms:
                        raise Exception('%s: duplicate subplatform ID for parent %s' % (
                            subplat_id, platform_id))
                    build_node(subplat, pn)
            return pn

        self._idp.clear()
        self._dummy_root = create_node(platform_id='')

        for platObj in pyobj["network"]:
            build_node(platObj, self._dummy_root)

    def ping(self):
        return "pong"

    def getPlatformMap(self):
        return self._dummy_root.get_map([])

    def getRootPlatformID(self):
        subplatforms = self._dummy_root.subplatforms
        assert len(subplatforms) == 1
        actual_root = list(subplatforms.itervalues())[0]
        return actual_root.platform_id

    def getSubplatformIDs(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        nnode = self._idp[platform_id]
        return {platform_id: list(nnode.subplatforms.iterkeys())}

    def getPlatformTypes(self):
        return self._platform_types

    def getPlatformMetadata(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        nnode = self._idp[platform_id]

        # TODO capture/include appropriate elements
        md = {}
        if nnode.name:
            md['name'] = nnode.name
        if nnode.parent:
            md['parent_platform_id'] = nnode.parent.platform_id
        md['platform_types'] = nnode.platform_types

        return {platform_id: md}

    def getPlatformAttributes(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        attrs = self._idp[platform_id].attrs
        ret_infos = {}
        for attrName in attrs:
            attr = attrs[attrName]
            ret_infos[attrName] = attr.defn

        return {platform_id: ret_infos}

    def dump(self):
        """string representation of the network"""
        return "platform_types: %s\nnetwork:\n%s" % (
            self._platform_types, self._dummy_root.dump())

    def getPlatformAttributeValues(self, platform_id, attrNames, from_time):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        timestamp = time.time()
        attrs = self._idp[platform_id].attrs
        vals = {}
        for attrName in attrNames:
            if attrName in attrs:
                attr = attrs[attrName]
                val = attr._value

                if val is None:
                    val = self._next_value
                    self._next_value += 1

                if val is not None and from_time < timestamp:
                    vals[attrName] = (val, timestamp)
                else:
                    vals[attrName] = ('', '')
            else:
                vals[attrName] = InvalidResponse.ATTRIBUTE_NAME_VALUE

        return {platform_id: vals}

    def getPlatformPorts(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        ports = {}
        for port_id, port in self._idp[platform_id].ports.iteritems():
            ports[port_id] = {'comms': port.comms, 'attrs': port.attrs}

        return {platform_id: ports}

    def setUpPort(self, platform_id, port_id, attributes):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port_attrs = self._idp[platform_id].get_port(port_id).attrs

        # result will contain the attributes that were set
        result = {}
        for key, val in attributes.iteritems():
            if key in VALID_PORT_ATTRIBUTES:
                # 1. set the value of the port attribute:
                # TODO validate the value
                port_attrs[key] = val

                # 2. in the result, indicate that the value was set:
                result[key] = val
            else:
                log.warn("setUpPort called with unrecognized attribute: %s"% key)

        return {platform_id: {port_id: result}}

    def turnOnPort(self, platform_id, port_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)
        if port._on:
            log.warn("port %s in platform %s already turned on." % (port_id, platform_id))
        else:
            port._on = True
            log.info("port %s in platform %s turned on." % (port_id, platform_id))

        return {platform_id: {port_id: port._on}}

    def turnOffPort(self, platform_id, port_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)
        if not port._on:
            log.warn("port %s in platform %s already turned off." % (port_id, platform_id))
        else:
            port._on = False
            log.info("port %s in platform %s turned off." % (port_id, platform_id))

        return {platform_id: {port_id: port._on}}

    def describeAlarmTypes(self, alarm_type_ids):
        if len(alarm_type_ids) == 0:
            return AlarmInfo.ALARM_TYPES

        result = {}
        for k in alarm_type_ids:
            if not k in AlarmInfo.ALARM_TYPES:
                result[k] = InvalidResponse.ALARM_TYPE
            else:
                result[k] = AlarmInfo.ALARM_TYPES[k]

        return result

    def getAlarmsByPlatformType(self, platform_types):
        if len(platform_types) == 0:
            platform_types = self._platform_types.keys()

        result = {}
        for platform_type in platform_types:
            if not platform_type in self._platform_types:
                result[platform_type] = InvalidResponse.PLATFORM_TYPE
                continue

            result[platform_type] = [v for v in AlarmInfo.ALARM_TYPES.itervalues() \
                if v['platform_type'] == platform_type]

        return result

    def _validate_alarm_listener_url(self, url):
        """
        Does a basic, static validation of the url.
        """
        # TODO implement it; for now always returning True
        return True

    def registerAlarmListener(self, url, alarm_types):
        log.info("registerAlarmListener: url=%r, alarm_types=%s",
                 url, str(alarm_types))

        if not self._validate_alarm_listener_url(url):
            return {url: InvalidResponse.ALARM_LISTENER_URL}

        if not url in self._reg_alarm_listeners:
            # create entry for this new url
            existing_pairs = self._reg_alarm_listeners[url] = []
        else:
            existing_pairs = self._reg_alarm_listeners[url]

        if len(existing_pairs):
            existing_types, reg_times = zip(*existing_pairs)
        else:
            existing_types = reg_times = []

        result_list = []
        for alarm_type in alarm_types:
            if not alarm_type in AlarmInfo.ALARM_TYPES:
                result_list.append((alarm_type, InvalidResponse.ALARM_TYPE))
                continue

            if alarm_type in existing_types:
                # already registered:
                reg_time = reg_times[existing_types.index(alarm_type)]
                result_list.append((alarm_type, reg_time))
            else:
                #
                # new registration
                #
                reg_time = self._alarm_notifier.add_listener(url, alarm_type)
                existing_pairs.append((alarm_type, reg_time))
                result_list.append((alarm_type, reg_time))

        return {url: result_list}

    def unregisterAlarmListener(self, url, alarm_types):
        log.info("unregisterAlarmListener: url=%r, alarm_types=%s",
                 url, str(alarm_types))

        if not url in self._reg_alarm_listeners:
            return {url: InvalidResponse.ALARM_LISTENER_URL}

        existing_pairs = self._reg_alarm_listeners[url]

        assert len(existing_pairs), "we don't keep any url with empty list"

        existing_types, reg_times = zip(*existing_pairs)

        result_list = []
        for alarm_type in alarm_types:
            if not alarm_type in AlarmInfo.ALARM_TYPES:
                result_list.append((alarm_type, InvalidResponse.ALARM_TYPE))
                continue

            if alarm_type in existing_types:
                #
                # registered, so remove it
                #
                unreg_time = self._alarm_notifier.remove_listener(url, alarm_type)
                del existing_pairs[existing_types.index(alarm_type)]
                result_list.append((alarm_type, unreg_time))
            else:
                # not registered, report 0
                unreg_time = 0
                result_list.append((alarm_type, unreg_time))

        if not len(existing_pairs):
            # we don't keep any url with empty list
            del self._reg_alarm_listeners[url]

        return {url: result_list}

    def getRegisteredAlarmListeners(self):
        return self._reg_alarm_listeners
