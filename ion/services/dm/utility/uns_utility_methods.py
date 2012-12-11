'''
@author Swarbhanu Chatterjee
@file ion/services/dm/utility/uns_utility_methods.py
@description A module containing common utility methods used by UNS and the notification workers.
'''
from pyon.public import get_sys_name, CFG
from pyon.util.arg_check import validate_is_not_none
from pyon.util.log import log
from pyon.core.exception import NotFound, BadRequest
from pyon.event.event import EventPublisher
from interface.objects import NotificationRequest, Event
import smtplib
import gevent
from gevent.timeout import Timeout
import string
from email.mime.text import MIMEText
from gevent import Greenlet
import datetime

class FakeScheduler(object):

    def __init__(self):
        self.event_publisher = EventPublisher("SchedulerEvent")

    def set_task(self, task_time, message):

        #------------------------------------------------------------------------------------
        # get the current time. Ex: datetime.datetime(2012, 7, 12, 14, 30, 6, 769776)
        #------------------------------------------------------------------------------------

        current_time = datetime.datetime.today()

        #------------------------------------------------------------------------------------
        # Calculate the time to wait
        #------------------------------------------------------------------------------------
        wait_time = datetime.timedelta( days = task_time.day - current_time.day,
            hours = task_time.hour - current_time.hour,
            minutes = task_time.minute - current_time.minute,
            seconds = task_time.second - current_time.second)

        log.info("Fake scheduler calculated wait_time = %s" % wait_time)

        seconds = wait_time.total_seconds()

        if seconds < 0:
            log.warning("Calculated wait time: %s seconds. Publishing immediately.")
            seconds = 0

        log.info("Total seconds of wait time = %s" % seconds)

        # this has to be replaced by something better
        gevent.sleep(seconds)

        self.event_publisher.publish_event(origin='Scheduler', description = message)
        log.info("Fake scheduler published a SchedulerEvent")


class fake_smtplib(object):

    def __init__(self,host):
        self.host = host
        self.sent_mail = gevent.queue.Queue()

    @classmethod
    def SMTP(cls,host):
        log.info("In fake_smtplib.SMTP method call. class: %s, host: %s" % (str(cls), str(host)))
        return cls(host)

    def sendmail(self, msg_sender= None, msg_recipients=None, msg=None):
        log.warning('Sending fake message from: %s, to: "%s"' % (msg_sender,  msg_recipients))
        log.info("Fake message sent: %s" % msg)
        self.sent_mail.put((msg_sender, msg_recipients[0], msg))
        log.debug("size of the sent_mail queue::: %s" % self.sent_mail.qsize())

    def quit(self):
        """
        Its a fake smtp client used only for tests. So no need to do anything here...
        """
        pass

def setting_up_smtp_client():
    '''
    Sets up the smtp client
    '''

    #------------------------------------------------------------------------------------
    # the default smtp server
    #------------------------------------------------------------------------------------
    smtp_client = None
    smtp_host = CFG.get_safe('server.smtp.host')
    smtp_port = CFG.get_safe('server.smtp.port', 25)
    smtp_sender = CFG.get_safe('server.smtp.sender')
    smtp_password = CFG.get_safe('server.smtp.password')

    if CFG.get_safe('system.smtp',False): #Default is False - use the fake_smtp
        log.debug('Using the real SMTP library to send email notifications! host = %s' % smtp_host)

#        smtp_client = smtplib.SMTP(smtp_host)
#        smtp_client.ehlo()
#        smtp_client.starttls()
#        smtp_client.login(smtp_sender, smtp_password)

        smtp_client = smtplib.SMTP(smtp_host, smtp_port)
        log.debug("In setting up smtp client using the smtp client: %s" % smtp_client)
        log.debug("Message received after ehlo exchange: %s" % str(smtp_client.ehlo()))
#        smtp_client.login(smtp_sender, smtp_password)
    else:
        log.debug('Using a fake SMTP library to simulate email notifications!')

        smtp_client = fake_smtplib.SMTP(smtp_host)

    return smtp_client

def _convert_unix_to_ntp(unix_seconds = None):

    if type(unix_seconds) == str: unix_seconds = int(unix_seconds.strip(" "))

    diff = datetime.datetime(1970, 1, 1, 0,0,0) - datetime.datetime(1900, 1, 1, 0, 0, 0)

    return unix_seconds + diff.total_seconds()

def _convert_ntp_to_unix(ntp_seconds = None):

    if type(ntp_seconds) == str: ntp_seconds = int(ntp_seconds.strip(" "))

    diff = datetime.datetime(1970, 1, 1, 0,0,0) - datetime.datetime(1900, 1, 1, 0, 0, 0)

    return ntp_seconds - diff.total_seconds()

def _get_time_stamp_for_special_events(message):

    time = ""
    if message.type_ == 'DeviceStatusEvent':
        time_stamps = []
        for t in message.time_stamps:
            log.debug("Got the time stamp: %s, the DeviceStatusEvent is this: %s " % (t, message))
            # Convert to the format, 2010-09-12T06:19:54
            time_stamps.append(_convert_to_human_readable(t))
        # Convert the timestamp list to a string
        time = str(time_stamps)

    elif message.type_ == 'DeviceCommsEvent':
        # Convert seconds since epoch to human readable form
        t = message.time_stamp
        log.debug("Got the time stamp: %s, the DeviceCommsEvent is this: %s" % (t, message))
        # Convert to the format, 2010-09-12T06:19:54
        time = _convert_to_human_readable(t)

    else:
        time = "None for this event type"

    return time

def _convert_to_human_readable(t = ''):

    # Convert milli seconds since epoch to human readable form
    if type(t) == str: t = int(t.strip(" "))
    x = datetime.datetime.fromtimestamp( t/1000 )
    # Convert to the format, 2010-09-12T06:19:54
    t = x.isoformat()

    return t

def send_email(message, msg_recipient, smtp_client):
    '''
    A common method to send email with formatting

    @param message              Event
    @param msg_recipient        str
    @param smtp_client          fake or real smtp client object

    '''

    log.debug("Got type of event to notify on: %s" % message.type_)

    # If DeviceStatusEvent or DeviceCommsEvent, gather the value of the time_stamp(s) attribute
    time = _get_time_stamp_for_special_events(message)

    # Get the diffrent attributes from the event message
    event = message.type_
    origin = message.origin
    description = message.description or "Not provided for this event"
    event_obj_as_string = str(message)
    ts_created = _convert_to_human_readable(message.ts_created)

    #------------------------------------------------------------------------------------
    # build the email from the event content
    #------------------------------------------------------------------------------------

    msg_body = string.join(("Event type: %s," %  event,
                            "",
                            "Originator: %s," %  origin,
                            "",
                            "Description: %s," % description,
                            "",
                            "Value of time_stamp(s) attribute of event: %s," %  time,
                            "",
                            "ts_created: %s," %  ts_created,
                            "",
                            "Event object as a dictionary: %s," %  event_obj_as_string,
                            "",
                            "You received this notification from ION because you asked to be "\
                            "notified about this event from this source. ",
                            "To modify or remove notifications about this event, "\
                            "please access My Notifications Settings in the ION Web UI.",
                            "Do not reply to this email.  This email address is not monitored "\
                            "and the emails will not be read."),
        "\r\n")
    msg_subject = "(SysName: " + get_sys_name() + ") ION event " + event + " from " + origin

    log.debug("msg_body::: %s" % msg_body)

    #------------------------------------------------------------------------------------
    # the 'from' email address for notification emails
    #------------------------------------------------------------------------------------

    ION_NOTIFICATION_EMAIL_ADDRESS = 'data_alerts@oceanobservatories.org'
    smtp_sender = CFG.get_safe('server.smtp.sender', ION_NOTIFICATION_EMAIL_ADDRESS)

    msg = MIMEText(msg_body)
    msg['Subject'] = msg_subject
    msg['From'] = smtp_sender
    msg['To'] = msg_recipient
    log.debug("UNS sending email from %s to %s" % ( smtp_sender,msg_recipient))
    log.debug("UNS using the smtp client: %s" % smtp_client)

    try:
        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())
    except: # Can be due to a broken connection... try to create a connection
        smtp_client = setting_up_smtp_client()
        log.debug("Connect again...message received after ehlo exchange: %s" % str(smtp_client.ehlo()))
        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())


def check_user_notification_interest(event, reverse_user_info):
    '''
    A method to check which user is interested in a notification or an event.
    The input parameter event can be used interchangeably with notification in this method
    Returns the list of users interested in the notification

    @param event                Event
    @param reverse_user_info    dict

    @retval user_ids list
    '''
    log.debug("checking for interested users. Event type: %s, reverse_user_info: %s" % (event.type_, reverse_user_info))

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

    if reverse_user_info['event_origin'].has_key(event.origin):
        if event.origin: # for an incoming event that has origin specified (this should be true for almost all events)
            user_list_1 = set(reverse_user_info['event_origin'][event.origin])
            if reverse_user_info['event_origin'].has_key(''): # for users who subscribe to any event origins
                user_list_1 += reverse_user_info['event_origin']['']
            users = user_list_1

            log.debug("For event origin = %s, UNS got interested users here  %s" % (event.origin, users))

    if reverse_user_info['event_origin_type'].has_key(event.origin_type):
        if event.origin_type: # for an incoming event with origin type specified
            user_list_2 = reverse_user_info['event_origin_type'][event.origin_type]
            if reverse_user_info['event_origin_type'].has_key(''): # for users who subscribe to any event origin types
                user_list_2 += reverse_user_info['event_origin_type']['']
            users = set.intersection(users, user_list_2)

            log.debug("For event_origin_type = %s too, UNS got interested users here  %s" % (event.origin_type, users))

    if reverse_user_info['event_type'].has_key(event.type_):
        user_list_3 = reverse_user_info['event_type'][event.type_]
        if reverse_user_info['event_type'].has_key(''): # for users who subscribe to any event types
            user_list_3 += reverse_user_info['event_type']['']
        users = set.intersection(users, user_list_3)

        log.debug("For event_type = %s too, UNS got interested users here  %s" % (event.type_, users))


    if reverse_user_info['event_subtype'].has_key(event.sub_type):
        if event.sub_type: # for an incoming event with the sub type specified
            user_list_4 = reverse_user_info['event_subtype'][event.sub_type]
            if reverse_user_info['event_subtype'].has_key(''): # for users who subscribe to any event subtypes
                user_list_4 += reverse_user_info['event_subtype']['']
            users = set.intersection(users, user_list_4)

            log.debug("For event_subtype = %s too, UNS got interested users here  %s" % (event.sub_type, users))


    users = list( users)

    return users

def calculate_reverse_user_info(user_info=None):
    '''
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
    '''

    if not user_info:
        return {}

    reverse_user_info = {}

    dict_1 = {}
    dict_2 = {}
    dict_3 = {}
    dict_4 = {}

    for user_id, value in user_info.iteritems():

        notifications = value['notifications']

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

