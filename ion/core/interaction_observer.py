#!/usr/bin/env python

"""Observes message interactions through the Exchange"""

__author__ = 'Prashant Kediyal, Michael Meisinger'
__license__ = 'Apache 2.0'

import string

from pyon.util.containers import get_ion_ts
from pyon.event.event import EventSubscriber
from pyon.ion.conversation_log import ConvSubscriber


MAX_MSGLOG = 3000
SLICE = 100


class InteractionObserver(object):
    """
    Observes ongoing interactions in the Exchange. Logs them to disk and makes them available
    in the local container (for development purposes) and on request.
    """

    def start(self):
        self.msg_log = []

        self.event_sub = None
        self.conv_sub = None

        #Conv subscription
        self.conv_sub = ConvSubscriber(callback=self._msg_received)
        self.conv_sub.start()

        # Event subscription
        self.event_sub = EventSubscriber(pattern=EventSubscriber.ALL_EVENTS,
                                         callback=self._event_received,
                                         queue_name="event_persister")
        self.event_sub.start()

        self.started = True

    def stop(self):
        # Stop event subscriber
        self.event_sub.stop()
        # Stop conv subscriber
        self.conv_sub.stop()
        self.started = False

    def _msg_received(self, msg, *args, **kwargs):
        self.log_message(args[0])

    def _event_received(self, event, *args, **kwargs):
        if 'origin' in event:
            args[0]['origin'] = event.origin
        if 'origin_type' in event:
            args[0]['origin_type'] = event.origin_type
        if 'sub_type' in event:
            args[0]['sub_type'] = event.sub_type
        self.log_message(args[0], True)

    def log_message(self, mhdrs, evmsg=False):
        """
        @param evmsg    This message is an event, render it as such!
        """
        mhdrs['_content_type'] = mhdrs.get('format', None)

        # TUPLE: timestamp (MS), type, boolean if its an event
        msg_rec = (get_ion_ts(), mhdrs, evmsg)
        self.msg_log.append(msg_rec)

        # Truncate if too long in increments of slice
        if len(self.msg_log) > MAX_MSGLOG + SLICE:
            self.msg_log = self.msg_log[SLICE:]

    def _get_data(self, msglog, response_msgs):
        """
        Provides msc data in python format, to be converted either to msc text or to json
        for use with msc web monitor.
        Returns a list of hashes in the form:
        { to, from, content, type, ts, error (boolean), to_raw, from_raw, topline }
        """
        msgdata = []

        for msgtup in msglog:

            datatemp = {"to": None, "from": None, "content": None, "type": None,
                        "ts": None, "error": False}
            msg = msgtup[1]

            convid = msg.get('conv-id', None)

            if (convid in response_msgs):
                response = response_msgs.pop(convid)
                sname = response.get('sender')
                rname = response.get('receiver')

            else:
                if (msg.get('sender-type', 'unknown') == 'service'):
                    sname = msg.get('sender-service', msg.get('sender-name',
                                    msg.get('sender', 'unknown')))
                else:
                    sname = msg.get('sender-name', msg.get('sender', 'unknown'))
                rname = msg.get('receiver', 'unknown')

                if (convid is not None):
                    response_msgs[convid] = {'sender': rname, 'receiver': sname}

            # from_raw is displayed as the header on the webpage
            datatemp["from_raw"] = sname
            sname = self._sanitize(sname)
            datatemp["from"] = sname
            datatemp["ts"] = msg.get("ts", "Unknown")
            datatemp["to_raw"] = rname
            rname = self._sanitize(rname)
            datatemp["to"] = rname

            if msgtup[2]:
                # this is an EVENT, show it as a box!
                datatemp["type"] = "events"

                #todo: not sure if we can hard code the splitting mechanism like done below !!
                datatemp["from"] = "events,"+ (msg.get('routing_key').split('._._.')[0]).split('.')[1]
                datatemp["from_raw"] = "events,"+(msg.get('routing_key').split('._._.')[0]).split('.')[1]
                datatemp["to"] = "events,"+ (msg.get('routing_key').split('._._.')[0]).split('.')[1]
                datatemp["to_raw"] = "events,"+ (msg.get('routing_key').split('._._.')[0]).split('.')[1]

                evlabel = "%s \nOrigin: %s" % (msg.get('routing_key'), msg.get('origin'))
                datatemp["content"] = evlabel
                datatemp["topline"] = msg.get('sub_type', '') + " " + msg.get('origin_type', '')
                if (datatemp['topline'] == " "):
                    datatemp['topline'] = msg.get('origin')

            else:

                mlabel = "%s\n(%s->%s)\n<%s>" % (msg.get('op', None), sname.rsplit(",", 1)[-1],
                         rname.rsplit(",", 1)[-1], msg.get('_content_type', '?content-type?'))
                datatemp["content"] = mlabel
                datatemp["topline"] = mlabel.split("\n")[0]

                if msg.get('protocol', None) == 'rpc':

                    datatemp["type"] = "rpcres"

                    performative = msg.get('performative', None)
                    if performative == 'request':
                        datatemp["type"] = "rpcreq"
                    elif performative == 'timeout':
                        pass  # timeout, unfortunately you don't see this @TODO

                    if performative == 'failure' or performative == 'error':
                        datatemp["error"] = True

                else:
                    # non rpc -> perhaps a data message?
                    datatemp["type"] = "data"

            msgdata.append(datatemp)

        return msgdata, response_msgs

    @classmethod
    def _sanitize(cls, input):
        return string.replace(string.replace(input, ".", "_"), "-", "_")
