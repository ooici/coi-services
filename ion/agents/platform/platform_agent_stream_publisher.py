#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent_stream_publisher
@file    ion/agents/platform/platform_agent_stream_publisher.py
@author  Carlos Rueda
@brief   Stream publishing support for platform agents.
         NOTE immediate goal is to extract the functionality from the platform
         agent code itself for modularization purposes and
         facilitate alignment with AgentStreamPublisher. For the moment there
         is some replication here of part of the functions there.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from pyon.ion.stream import StreamPublisher

from interface.objects import StreamRoute

from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
import numpy

from coverage_model.parameter import ParameterDictionary

from pyon.core.bootstrap import get_obj_registry
from pyon.core.object import IonObjectDeserializer

import uuid

import logging


class PlatformAgentStreamPublisher(object):
    """
    Stream publishing support for platform agents.
    NOTE immediate goal of this module is to extract the functionality
    from the platform agent code itself. A next goal is to align
    (eventually extending) AgentStreamPublisher.
    """

    def __init__(self, agent):
        self._agent = agent

        self._platform_id = agent._platform_id
        self.resource_id  = agent.resource_id
        self._pp          = agent._pp
        self.CFG          = agent.CFG

        # Dictionaries used for data publishing.
        self._data_streams = {}
        self._param_dicts = {}
        self._stream_defs = {}
        self._data_publishers = {}

        self._connection_ID = None
        self._connection_index = {}

        # Set of parameter names received in event notification but not
        # configured. Allows to log corresponding warning only once.
        self._unconfigured_params = set()

        stream_info = self.CFG.get('stream_config', None)
        if stream_info is None:
            # should not happen: PlatformAgent._validate_configuration validates this.
            log.error("%r: No stream_config given in CFG", self._platform_id)
            return

        for stream_name, stream_config in stream_info.iteritems():
            self._construct_stream_and_publisher(stream_name, stream_config)

        log.debug("%r: PlatformAgentStreamPublisher complete", self._platform_id)

    def _construct_stream_and_publisher(self, stream_name, stream_config):

        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r: _construct_stream_and_publisher: "
                      "stream_name:%r, stream_config:\n%s",
                      self._platform_id, stream_name,
                      self._pp.pformat(stream_config))

        decoder = IonObjectDeserializer(obj_registry=get_obj_registry())

        if 'stream_def_dict' not in stream_config:
            # should not happen: PlatformAgent._validate_configuration validates this.
            log.error("'stream_def_dict' key not in configuration for stream %r" % stream_name)
            return

        stream_def_dict = stream_config['stream_def_dict']
        stream_def_dict['type_'] = 'StreamDefinition'
        stream_def_obj = decoder.deserialize(stream_def_dict)
        self._stream_defs[stream_name] = stream_def_obj

        routing_key           = stream_config['routing_key']
        stream_id             = stream_config['stream_id']
        exchange_point        = stream_config['exchange_point']
        parameter_dictionary  = stream_def_dict['parameter_dictionary']
        log.debug("%r: got parameter_dictionary from stream_def_dict", self._platform_id)

        self._data_streams[stream_name] = stream_id
        self._param_dicts[stream_name] = ParameterDictionary.load(parameter_dictionary)
        stream_route = StreamRoute(exchange_point=exchange_point, routing_key=routing_key)
        publisher = self._create_publisher(stream_id, stream_route)
        self._data_publishers[stream_name] = publisher

        log.debug("%r: created publisher for stream_name=%r", self._platform_id, stream_name)

    def _create_publisher(self, stream_id, stream_route):
        publisher = StreamPublisher(process=self._agent,
                                    stream_id=stream_id,
                                    stream_route=stream_route)
        return publisher

    def reset_connection(self):
        self._connection_ID = uuid.uuid4()
        self._connection_index = {stream_name : 0 for
                                  stream_name in self._data_streams.keys()}
        log.debug("%r: reset_connection: connection_id=%s, connection_index=%s",
                  self._platform_id, self._connection_ID.hex, self._connection_index)

    def handle_attribute_value_event(self, driver_event):
        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            # show driver_event as retrieved (driver_event.vals_dict might be large)
            log.trace("%r: driver_event = %s", self._platform_id, driver_event)
            log.trace("%r: vals_dict:\n%s",
                      self._platform_id, self._pp.pformat(driver_event.vals_dict))

        elif log.isEnabledFor(logging.DEBUG):  # pragma: no cover
            log.debug("%r: driver_event = %s", self._platform_id, driver_event.brief())

        stream_name = driver_event.stream_name

        publisher = self._data_publishers.get(stream_name, None)
        if not publisher:
            log.warn('%r: no publisher configured for stream_name=%r. '
                     'Configured streams are: %s',
                     self._platform_id, stream_name, self._data_publishers.keys())
            return

        param_dict = self._param_dicts[stream_name]
        stream_def = self._stream_defs[stream_name]

        if isinstance(stream_def, str):
            rdt = RecordDictionaryTool(param_dictionary=param_dict.dump(),
                                       stream_definition_id=stream_def)
        else:
            rdt = RecordDictionaryTool(stream_definition=stream_def)

        self._publish_granule_with_multiple_params(publisher, driver_event,
                                                   param_dict, rdt)

    def _publish_granule_with_multiple_params(self, publisher, driver_event,
                                              param_dict, rdt):

        stream_name = driver_event.stream_name

        pub_params = {}
        selected_timestamps = None

        for param_name, param_value in driver_event.vals_dict.iteritems():

            in_rdt = False
            param_name = param_name.lower()
            if param_name in rdt:
                in_rdt = True
            else:
                if param_name not in self._unconfigured_params:
                    # an unrecognized attribute for this platform:
                    self._unconfigured_params.add(param_name)
                    log.warn('%r: got attribute value event for unconfigured parameter %r in stream %r'
                             ' rdt.keys=%s',
                             self._platform_id, param_name, stream_name, list(rdt.iterkeys()))

            if not in_rdt:
                # OOIION-1551 The following is temporary to facilitate
                # transitioning to the use of the new attr-id format in
                # resources like the preload. So, for the moment, the following
                # checks whether the name part is in the RDT to then use that
                # instead of simply logging a warning and skipping this
                # parameter.
                # Note: we still log the warning above for the original
                # param_name to facilitate general inspection.
                separator = param_name.rfind('|')
                if separator >= 0:
                    # try the attr-name part:
                    attr_name = param_name[:separator]
                    if attr_name in rdt:
                        # so, we found the attr-name part in the RDT; let's
                        # use that and note that we will be ignoring the
                        # attr-instance part in this case. Again, this is
                        # temporary to facilitate review/transition.
                        log.warn('%r: OOIION-1551: will use %r, which was found '
                                 'in RDT. The original %r was not.',
                                 self._platform_id, attr_name, param_name)
                        in_rdt = True
                        param_name = attr_name
                    else:
                        if attr_name not in self._unconfigured_params:
                            self._unconfigured_params.add(attr_name)
                            log.warn('%r: got attribute value event for unconfigured parameter %r in stream %r'
                                     ' rdt.keys=%s',
                                     self._platform_id, attr_name, stream_name, list(rdt.iterkeys()))

            if not in_rdt:
                continue

            # separate values and timestamps:
            vals, timestamps = zip(*param_value)

            self._agent._dispatch_value_alerts(stream_name, param_name, vals)

            # Use fill_value in context to replace any None values:
            param_ctx = param_dict.get_context(param_name)
            if param_ctx:
                fill_value = param_ctx.fill_value
                log.debug("%r: param_name=%r fill_value=%s",
                          self._platform_id, param_name, fill_value)
                # do the replacement:
                vals = [fill_value if val is None else val for val in vals]

                if log.isEnabledFor(logging.TRACE):  # pragma: no cover
                    log.trace("%r: vals array after replacing None with fill_value:\n%s",
                              self._platform_id, self._pp.pformat(vals))

            else:
                log.warn("%r: unexpected: parameter context not found for %r",
                         self._platform_id, param_name)

            # Set values in rdt:
            rdt[param_name] = numpy.array(vals)

            pub_params[param_name] = vals

            selected_timestamps = timestamps

        if selected_timestamps is None:
            # that is, all param_name's were unrecognized; just return:
            return

        self._publish_granule(stream_name, publisher, param_dict, rdt,
                              pub_params, selected_timestamps)

    def _publish_granule(self, stream_name, publisher, param_dict, rdt,
                         pub_params, timestamps):

        # Set timestamp info in rdt:
        if param_dict.temporal_parameter_name is not None:
            temp_param_name = param_dict.temporal_parameter_name
            rdt[temp_param_name]       = numpy.array(timestamps)
            #@TODO: Ensure that the preferred_timestamp field is correct
            rdt['preferred_timestamp'] = numpy.array(['internal_timestamp'] * len(timestamps))
            log.warn('Preferred timestamp is unresolved, using "internal_timestamp"')
        else:
            log.warn("%r: Not including timestamp info in granule: "
                     "temporal_parameter_name not defined in parameter dictionary",
                     self._platform_id)

        g = rdt.to_granule(data_producer_id=self.resource_id,
                           connection_id=self._connection_ID.hex,
                           connection_index=str(self._connection_index[stream_name]))
        try:
            publisher.publish(g)

            if log.isEnabledFor(logging.TRACE):  # pragma: no cover
                log.trace("%r: Platform agent published data granule on stream %r: "
                          "%s  timestamps: %s",
                          self._platform_id, stream_name,
                          self._pp.pformat(pub_params), self._pp.pformat(timestamps))
            elif log.isEnabledFor(logging.DEBUG):  # pragma: no cover
                summary_params = {attr_id: "(%d vals)" % len(vals)
                                  for attr_id, vals in pub_params.iteritems()}
                summary_timestamps = "(%d vals)" % len(timestamps)
                log.debug("%r: Platform agent published data granule on stream %r: "
                          "%s  timestamps: %s",
                          self._platform_id, stream_name,
                          summary_params, summary_timestamps)

            log.debug("%r: granule published with connection_id=%s, connection_index=%i",
                      self._platform_id,
                      self._connection_ID.hex,
                      self._connection_index[stream_name])
            self._connection_index[stream_name] += 1

        except Exception:
            log.exception("%r: Platform agent could not publish data on stream %s.",
                          self._platform_id, stream_name)

    def reset(self):
        self._unconfigured_params.clear()
