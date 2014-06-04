"""
@author Swarbhanu Chatterjee
@file ion/services/dm/utility/uns_utility_methods.py
@description A module containing common utility methods used by UNS and the notification workers.
"""
from pyon.core import bootstrap
from pyon.public import get_sys_name, OT, IonObject, CFG
from pyon.util.ion_time import IonTime
from pyon.util.log import log
from pyon.core.exception import BadRequest, NotFound
from interface.objects import NotificationRequest, Event, DeviceStatusType, AggregateStatusType, InformationStatus
from pyon.util.containers import get_ion_ts
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
import smtplib
import gevent
import pprint
import string
from email.mime.text import MIMEText
from gevent import Greenlet



class fake_smtplib(object):

    def __init__(self,host):
        self.host = host
        self.sent_mail = gevent.queue.Queue()

    @classmethod
    def SMTP(cls,host):
        log.info("In fake_smtplib.SMTP method call. class: %s, host: %s", str(cls), str(host))
        return cls(host)

    def sendmail(self, msg_sender= None, msg_recipients=None, msg=None):
        log.warning('Sending fake message from: %s, to: "%s"', msg_sender,  msg_recipients)
        #log.info("Fake message sent: %s", msg)
        self.sent_mail.put((msg_sender, msg_recipients[0], msg))
        log.debug("size of the sent_mail queue::: %s", self.sent_mail.qsize())

    def quit(self):
        """
        Its a fake smtp client used only for tests. So no need to do anything here...
        """
        pass

def setting_up_smtp_client():
    """
    Sets up the smtp client
    """

    #------------------------------------------------------------------------------------
    # the default smtp server
    #------------------------------------------------------------------------------------
#    smtp_client = None
    smtp_host = CFG.get_safe('server.smtp.host')
    smtp_port = CFG.get_safe('server.smtp.port', 25)
#    smtp_sender = CFG.get_safe('server.smtp.sender')
#    smtp_password = CFG.get_safe('server.smtp.password')

    if CFG.get_safe('system.smtp',False): #Default is False - use the fake_smtp
        log.debug('Using the real SMTP library to send email notifications! host = %s', smtp_host)

#        smtp_client = smtplib.SMTP(smtp_host)
#        smtp_client.ehlo()
#        smtp_client.starttls()
#        smtp_client.login(smtp_sender, smtp_password)

        smtp_client = smtplib.SMTP(smtp_host, smtp_port)
        log.debug("In setting up smtp client using the smtp client: %s", smtp_client)
        log.debug("Message received after ehlo exchange: %s", str(smtp_client.ehlo()))
#        smtp_client.login(smtp_sender, smtp_password)
    else:
        log.debug('Using a fake SMTP library to simulate email notifications!')

        smtp_client = fake_smtplib.SMTP(smtp_host)

    return smtp_client


def convert_timestamp_to_human_readable(timestamp=''):

    it = IonTime(int(timestamp)/1000.)
    return str(it)


def convert_events_to_email_message(events=None, notifications_map=None, rr_client=None):

    if events is None: events = []

    # map event origins to resource objects to provide additional context in the email
    event_origin_to_resource_map = {}

    if 0 == len(events): raise BadRequest("Tried to convert events to email, but none were supplied")

    web_ui_url = CFG.get_safe('system.web_ui_url', None)
    log.debug("found CFG.system.web_ui_url = %s" % web_ui_url)
    if web_ui_url is None:
        web_ui_txt = ""
    else:
        web_ui_txt = ": %s" % web_ui_url

    msg_body = ""

    #collect all the resources from the RR in one call
    event_origin_to_resource_map = _collect_resources_from_event_origins(events=None, rr_client=None)

    resource_human_readable = "<uninitialized string>"
    for idx, event in enumerate(events, 1):

        ts_created = convert_timestamp_to_human_readable(event.ts_created)

        # build human readable resource string
        resource_human_readable = "'%s' with ID='%s' (not found)" % (event.origin_type, event.origin)
        notification_name = _get_notification_name(event_id=event._id, notifications_map=notifications_map)

        resource = ''
        # pull the resource from the map if the origin id was found
        if event.origin in event_origin_to_resource_map:
            resource = event_origin_to_resource_map[event.origin]
            resource_human_readable = "%s '%s'" % (type(resource).__name__, resource.name)


        if 1 == len(events):
            eventtitle = "Type"
        else:
            eventtitle = "%s Type" % idx

        msg_body += string.join(("\r\n",
                                 "Event %s: %s" %  (eventtitle, event.type_),
                                 "",
                                 "Notification Request Name: %s" %  notification_name,
                                 "",
                                 "Resource: %s" %  resource_human_readable,
                                 "",
                                 "Date & Time: %s" %  ts_created,
                                 "",
                                 "Description: %s" % get_event_summary(event) or event.description or "Not provided",
#                                 "",
#                                 "Event object as a dictionary: %s," %  str(event),
                                 "\r\n",
                                 "------------------------",
                                 "\r\n",
                                 ),         # necessary!
                                 "\r\n")


    msg_body += ("\r\n\r\nAutomated alert from the OOI ION system (%s). " % get_sys_name()) +\
                "This notification was received based on " +\
                "your current subscription settings for this event type from this resource. To unsubscribe " +\
                "from notifications of this event type, please access the actions menu for the resource " +\
                ("listed above in the ION interface%s.  \r\n\r\n" % web_ui_txt) +\
                "Do not reply to this email.  This email address is not monitored and the emails will not be read.\r\n"


    #log.debug("The email has the following message body: %s", msg_body)

    msg_subject = ""
    if 1 == len(events):
        msg_subject += "ION event " + events[0].type_ + " from " + resource_human_readable
    else:
        msg_subject += "summary of %s ION events" % len(events)

    msg = MIMEText(msg_body)
    msg['Subject'] = msg_subject
    #    msg['From'] = smtp_sender
    #    msg['To'] = msg_recipient

    return msg


def _collect_resources_from_event_origins(events=None, rr_client=None):

    unique_origin_ids = set()
    origin_id_to_resource_map = {}

    if not events or not rr_client:
        return origin_id_to_resource_map

    #get the unique set of event origin ids
    for event in events:
        unique_origin_ids.add(event)

    #pull all the unique origins from the RR in one call
    resources = []
    try:
        resources = rr_client.read_mult( list(unique_origin_ids) )
    except NotFound:
    # if not found, move on and do not include the details for that resource in the email msg
        pass

    #create the mapping
    for resource in resources:
       origin_id_to_resource_map[resource._id] = resource

    return origin_id_to_resource_map


def _get_notification_name(event_id='', notifications_map=None):
    notification_names = ''

    if not notifications_map:
        return notification_names

    names_list = set()
    if event_id in notifications_map:
        for notification_obj in notifications_map[event_id]:
            # loop the list of associated notification requests and append the names
            names_list.add(notification_obj.name)

        notification_names = ",".join(list(names_list))

    return notification_names



def send_email(event, msg_recipient, smtp_client, rr_client):
    """
    A common method to send email with formatting

    @param event              Event
    @param msg_recipient        str
    @param smtp_client          fake or real smtp client object

    """

    #log.debug("Got type of event to notify on: %s", event.type_)

    #------------------------------------------------------------------------------------
    # the 'from' email address for notification emails
    #------------------------------------------------------------------------------------

    ION_NOTIFICATION_EMAIL_ADDRESS = 'data_alerts@oceanobservatories.org'
    smtp_sender = CFG.get_safe('server.smtp.sender', ION_NOTIFICATION_EMAIL_ADDRESS)

    msg = convert_events_to_email_message(events=[event], notifications_map=None, rr_client=rr_client)
    msg['From'] = smtp_sender
    msg['To'] = msg_recipient
    log.debug("UNS sending email from %s to %s for event type: %s", smtp_sender,msg_recipient, event.type_)
    #log.debug("UNS using the smtp client: %s", smtp_client)

    try:
        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())
    except: # Can be due to a broken connection... try to create a connection
        smtp_client = setting_up_smtp_client()
        log.debug("Connect again...message received after ehlo exchange: %s", str(smtp_client.ehlo()))
        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())


def check_user_notification_interest(event, reverse_user_info):
    """
    A method to check which user is interested in a notification or an event.
    The input parameter event can be used interchangeably with notification in this method
    Returns the list of users interested in the notification

    @param event                Event
    @param reverse_user_info    dict

    @retval user_ids list
    """
#    log.debug("Checking for interested users. Event type: %s, reverse_user_info: %s", event.type_, reverse_user_info)

    if not isinstance(event, Event):
        raise BadRequest("The input parameter should have been an Event.")

    if reverse_user_info.has_key('event_origin') and\
       reverse_user_info.has_key('event_origin_type') and\
       reverse_user_info.has_key('event_type') and\
       reverse_user_info.has_key('event_subtype'):
        pass
    else:
        raise BadRequest("Missing keys in reverse_user_info. Reverse_user_info not properly set up.")


    if not event or not reverse_user_info:
        raise BadRequest("Missing input parameters for method, check_user_notification_interest().")

    users = set()

    """
    Prioritize... First check event type. If that matches proceed to check origin if that attribute of the event obj is filled,
    If that matches too, check for sub_type if that attribute is filled for the event object...
    If this matches too, check for origin_type if that attribute of the event object is not empty.
    """

    if event.type_: # for an incoming event with origin type specified
        if reverse_user_info['event_type'].has_key(event.type_):
            user_list_1 = reverse_user_info['event_type'][event.type_]
            if reverse_user_info['event_type'].has_key(''): # for users who subscribe to any event types
                user_list_1.extend(reverse_user_info['event_type'][''])
            users = set(user_list_1)
#            log.debug("For event_type = %s, UNS got interested users here  %s", event.type_, users)
        else:
#            log.debug("After checking event_type = %s, UNS got no interested users here", event.type_)
            return []

    if event.origin: # for an incoming event that has origin specified (this should be true for almost all events)
        if reverse_user_info['event_origin'].has_key(event.origin):
            user_list_2 = set(reverse_user_info['event_origin'][event.origin])
            if reverse_user_info['event_origin'].has_key(''): # for users who subscribe to any event origins
                user_list_2.extend(reverse_user_info['event_origin'][''])
            users = set.intersection(users, user_list_2)
#            log.debug("For event origin = %s too, UNS got interested users here  %s", event.origin, users)
        else:
#            log.debug("After checking  event origin = %s, UNS got no interested users here", event.origin)
            return []

    if event.sub_type:  # for an incoming event with the sub type specified
        if reverse_user_info['event_subtype'].has_key(event.sub_type):
            user_list_3 = reverse_user_info['event_subtype'][event.sub_type]
            if reverse_user_info['event_subtype'].has_key(''): # for users who subscribe to any event subtypes
                user_list_3.extend(reverse_user_info['event_subtype'][''])
            users = set.intersection(users, user_list_3)
#        else:
#            log.debug("After checking event_subtype = %s, UNS got no interested users here", event.sub_type)
#            return []

    if event.origin_type:  # for an incoming event with origin type specified
        if reverse_user_info['event_origin_type'].has_key(event.origin_type):
            user_list_4 = reverse_user_info['event_origin_type'][event.origin_type]
            if reverse_user_info['event_origin_type'].has_key(''): # for users who subscribe to any event origin types
                user_list_4.extend(reverse_user_info['event_origin_type'][''])
            users = set.intersection(users, user_list_4)
        else:
#            log.debug("After checking event_origin_type = %s, UNS got no interested users here", event.origin_type)
            return []

#    log.debug("The interested users found here are: %s, for event: %s", list(users), event)
    return list( users)

def calculate_reverse_user_info(user_info=None):
    """
    Calculate a reverse user info... used by the notification workers and the UNS

    @param user_info            dict
    @retval reverse_user_info   dict

    The reverse_user_info dictionary has the following form:

    reverse_user_info = {'event_type' : { <event_type_1> : ['user_1', 'user_2'..],
                                             <event_type_2> : ['user_3'],... },

                        'event_subtype' : { <event_subtype_1> : ['user_1', 'user_2'..],
                                               <event_subtype_2> : ['user_3'],... },

                        'event_origin' : { <event_origin_1> : ['user_1', 'user_2'..],
                                              <event_origin_2> : ['user_3'],... },

                        'event_origin_type' : { <event_origin_type_1> : ['user_1', 'user_2'..],
                                                   <event_origin_type_2> : ['user_3'],... },
    """

    if not user_info:
        return {}

    reverse_user_info = {}

    dict_1 = {}
    dict_2 = {}
    dict_3 = {}
    dict_4 = {}

    for user_id, value in user_info.iteritems():

        notifications = value['notifications']

        notifications_disabled = value.get('notifications_disabled', False)
        notifications_daily_digest = value.get('notifications_daily_digest', False)

        # Ignore users who do NOT want REALTIME notifications or who have disabled the delivery switch
        # However, if notification preferences have not been set at all for the user, do not bother

        if notifications_disabled or not not notifications_daily_digest:
            continue

        if notifications:

            for notification in notifications:

                # If the notification has expired, do not keep it in the reverse user info that the notification
                # workers use
                if notification.temporal_bounds.end_datetime:
                    continue

                if not isinstance(notification, NotificationRequest):
                    continue

                if dict_1.has_key(notification.event_type) and notification.event_type != '':
                    dict_1[notification.event_type].append(user_id)
                    # to remove duplicate user names
                    dict_1[notification.event_type] = list(set(dict_1[notification.event_type]))
                elif notification.event_type != '':
                    dict_1[notification.event_type] = [user_id]

                if dict_2.has_key(notification.event_subtype) and notification.event_subtype != '':
                    dict_2[notification.event_subtype].append(user_id)
                    # to remove duplicate user names
                    dict_2[notification.event_subtype] = list(set(dict_2[notification.event_subtype]))
                elif notification.event_subtype != '':
                    dict_2[notification.event_subtype] = [user_id]

                if dict_3.has_key(notification.origin) and notification.origin != '':
                    dict_3[notification.origin].append(user_id)
                    # to remove duplicate user names
                    dict_3[notification.origin] = list(set(dict_3[notification.origin]))
                elif notification.origin != '':
                    dict_3[notification.origin] = [user_id]

                if dict_4.has_key(notification.origin_type) and notification.origin_type != '':
                    dict_4[notification.origin_type].append(user_id)
                    # to remove duplicate user names
                    dict_4[notification.origin_type] = list(set(dict_4[notification.origin_type]))
                elif notification.origin_type != '':
                    dict_4[notification.origin_type] = [user_id]

                reverse_user_info['event_type'] = dict_1
                reverse_user_info['event_subtype'] = dict_2
                reverse_user_info['event_origin'] = dict_3
                reverse_user_info['event_origin_type'] = dict_4

    return reverse_user_info

def get_event_computed_attributes(event, include_event=False, include_special=False, include_formatted=False):
    """
    @param event any Event to compute attributes for
    @retval an EventComputedAttributes object for given event
    """
    evt_computed = IonObject(OT.EventComputedAttributes)
    evt_computed.event_id = event._id
    evt_computed.ts_computed = get_ion_ts()
    evt_computed.event = event if include_event else None

    try:
        summary = get_event_summary(event)
        evt_computed.event_summary = summary

        if include_special:
            spc_attrs = ["%s:%s" % (k, str(getattr(event, k))[:50]) for k in sorted(event.__dict__.keys()) if k not in ['_id', '_rev', 'type_', 'origin', 'origin_type', 'ts_created', 'base_types']]
            evt_computed.special_attributes = ", ".join(spc_attrs)

        if include_formatted:
            evt_computed.event_attributes_formatted = pprint.pformat(event.__dict__)
    except Exception as ex:
        log.exception("Error computing EventComputedAttributes for event %s: %s", event, ex)

    return evt_computed

def get_event_summary(event):
    event_types = [event.type_] + event.base_types
    summary = ""
    if "ResourceLifecycleEvent" in event_types:
        summary = "%s lifecycle state change. State: %s  Availability: %s" % (event.origin_type, event.lcstate, event.availability)
    elif "ResourceModifiedEvent" in event_types:
        summary = "%s modified: %s" % (event.origin_type, event.sub_type)
    elif "ResourceIssueReportedEvent" in event_types:
        summary = "Issue created: %s" % event.description

    elif "ResourceAgentStateEvent" in event_types:
        summary = "%s agent state change: %s" % (event.origin_type, event.state)
    elif "ResourceAgentResourceStateEvent" in event_types:
        summary = "%s agent resource state change: %s" % (event.origin_type, event.state)
    elif "ResourceAgentConfigEvent" in event_types:
        summary = "%s agent config set: %s" % (event.origin_type, event.config)
    elif "ResourceAgentResourceConfigEvent" in event_types:
        summary = "%s agent resource config set: %s" % (event.origin_type, event.config)
    elif "ResourceAgentCommandEvent" in event_types:
        summary = "%s agent command '%s(%s)' succeeded: %s" % (event.origin_type, event.command, event.execute_command, "" if event.result is None else event.result)
    elif "ResourceAgentErrorEvent" in event_types:
        summary = "%s agent command '%s(%s)' failed: %s:%s (%s)" % (event.origin_type, event.command, event.execute_command, event.error_type, event.error_msg, event.error_code)
    elif "ResourceAgentAsyncResultEvent" in event_types:
        summary = "%s agent async command '%s(%s)' succeeded: %s" % (event.origin_type, event.command, event.desc, "" if event.result is None else event.result)
    elif "ResourceAgentConnectionLostErrorEvent" in event_types:
        summary = "%s agent: %s (%s)" % (event.origin_type, event.error_msg, event.error_code)
    elif "ResourceAgentIOEvent" in event_types:
        stats_str = ",".join(["%s:%s" % (k, event.stats[k]) for k in sorted(event.stats)])
        summary = "%s agent IO: %s (%s)" % (event.origin_type, event.source_type, stats_str)
    elif "ResourceAgentEvent" in event_types:
        summary = "%s agent: %s" % (event.origin_type, event.type_)

    elif "InformationContentStatusEvent" in event_types:
        summary = "%s content status: %s (%s)" % (event.origin_type, event.sub_type, InformationStatus._str_map.get(event.status,"???"))
    elif "InformationContentEvent" in event_types:
        summary = "%s content event: %s (%s)" % (event.origin_type, event.type_, event.sub_type)

    elif "ResourceAgentResourceCommandEvent" in event_types:
        summary = "%s agent resource command '%s(%s)' executed: %s" % (event.origin_type, event.command, event.execute_command, "OK" if event.result is None else event.result)
    elif "DeviceAggregateStatusEvent" in event_types:
        summary = "%s status change: %s  previous status: %s related alerts: %s" % (AggregateStatusType._str_map.get(event.status_name,"???"), DeviceStatusType._str_map.get(event.status,"???"), DeviceStatusType._str_map.get(event.prev_status,"???"), event.values)
    elif "DeviceStatusEvent" in event_types:
        summary = "%s '%s' status change: %s   %s " % (event.origin_type, event.sub_type, DeviceStatusType._str_map.get(event.status,"???"), event.description)
        if hasattr(event, 'values') and event.values:
            summary  +=  " values: %s" % event.values

    elif "DeviceOperatorEvent" in event_types or "ResourceOperatorEvent" in event_types:
        summary = "Operator entered: %s" % event.description
    elif "ParameterQCEvent" in event_types:
        summary = "%s   Temporal values: %s  " % (event.description, event.temporal_values )
        log.debug('ParameterQCEvent  summary: %s', summary)
    elif "OrgMembershipGrantedEvent" in event_types:
        summary = "Joined Org '%s' as member" % event.org_name
    elif "OrgMembershipCancelledEvent" in event_types:
        summary = "Cancelled Org '%s' membership" % event.org_name
    elif "UserRoleGrantedEvent" in event_types:
        summary = "Granted %s in Org '%s'" % (event.role_name, event.org_name)
    elif "UserRoleRevokedEvent" in event_types:
        summary = "Revoked %s in Org '%s'" % (event.role_name, event.org_name)
    elif "ResourceSharedEvent" in event_types:
        summary = "%s shared in Org: '%s'" % (event.sub_type, event.org_name)
    elif "ResourceUnsharedEvent" in event_types:
        summary = "%s unshared in Org: '%s'" % (event.sub_type, event.org_name)
    elif "ResourceCommitmentCreatedEvent" in event_types:
        summary = "%s commitment created in Org: '%s'" % (event.commitment_type, event.org_name)
    elif "ResourceCommitmentReleasedEvent" in event_types:
        summary = "%s commitment released in Org: '%s'" % (event.commitment_type, event.org_name)
    elif "ParameterQCEvent" in event_types:
        summary = "%s" % event.description
    elif "OrgNegotiationInitiatedEvent" in event_types:
        summary = "Negotiation %s initiated in Org '%s' of type %s and description: %s" % (event.negotiation_id, event.org_name, event.sub_type, event.description )

    #        if event.description and summary:
    #            summary = summary + ". " + event.description
    #        elif event.description:
    #            summary = event.description
    return summary


def load_notifications(container=None):
    """
    result is dict:
        key: tuple(origin,origin_type,event_type,event_subtype), matches Event to NotificationRequest
        value: set() containing tuple(notification,user_info)
    """

    # clients
    container = container or bootstrap.container_instance
    resource_registry = container.resource_registry

    # uses ObservatoryUtil or ResourceRegistry to find children (id) associated with a NotificationRequest
    def _notification_children(notification_origin, notification_type, observatory_util=None):
        if observatory_util is None:
             observatory_util = ObservatoryUtil()
        children = []
        if notificatioN_type == OT.NotificationTypeEnum.PLATFORM:
            device_relations = observatory_util.get_child_devices(notification_origin)
            children = [did for pt,did,dt in device_relations[notification_origin]]
        elif type == OT.NotificationTypeEnum.SITE:
            child_site_dict, ancestors = observatory_util.get_child_sites(notification_origin)
            children = child_site_dict.keys()
        elif type == OT.NotificationTypeEnum.FACILITY:
            objects, _ = resource_registry.find_objects(subject=notification_origin, predicate=PRED.hasResource, id_only=False)
            for o in objects:
                if o.type_ == RT.DataProduct
                or o.type_ == RT.InstrumentSite
                or o.type_ == RT.InstrumentDevice
                or o.type_ == RT.PlatformSite
                or o.type_ == RT.PlatformDevice:
                    children.append(o._id)
        if notification_origin in children:
            children.remove(notification_origin)
        return children

    # time when we're loading, used for expired notifications
    current_datetime = get_ion_ts()

    # return dict, keyed by (origin,origin_type,event_type,event_subtype) tuple, contains list of (notification, user) values
    notifications = {}

    # all users (full objects)
    users, _ = resource_registry.find_resources(restype=RT.UserInfo)

    # subject: UserInfo
    subjects = [u._id for u in users]
    # hasNotification associations is only way to NotificationRequests, load all associations
    objects, associations = resource_registry.find_objects_mult(subjects=subjects, id_only=False)
    # object: NotificationRequest
    # association.p: hasNotification
    # association.s: UserInfo
    for notification, association in zip(objects, associations):

        if association.p == PRED.hasNotification:

            user = [v for k,v in users.items() if v._id == association.p][0]

            # NotificationRequest disabled by system process?
            if notification.disabled_by_system:
                continue

            # NotificationRequest expired? (note this is relative to current time)
            if int(notification.temporal_bounds.end_datetime) < current_datetime:
                continue

            # create tuple key (origin,origin_type,event_type,event_subtype)
            origin = notification.origin
            origin_type = notification.origin_type
            event_type = notification.event_type
            event_subtype = notification.event_subtype
            key = (origin, origin_type, event_type, event_subtype)

            # store tuple by key containing set of (NotificationRequest,UserInfo)
            if key not in notifications:
                notifications[key] = set()
            value = (notification, user)
            notifications[key].add(value)

            # add children if applicable - children have same (notification, user) value
            if notification.type != OT.NotificationTypeEnum.SIMPLE and notification.origin:
                children = _notification_children(notification_origin=notification.origin, notification_type=notification.type)
                for child in children: # child is _id
                    key = (child, None, event_type, event_subtype) # TODO None hardcoded here because children can not specify type
                    notifications[key].add(value)

    return notifications

