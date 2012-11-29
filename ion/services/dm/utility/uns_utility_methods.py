'''
@author Swarbhanu Chatterjee
@file ion/services/dm/utility/uns_utility_methods.py
@description A module containing common utility methods used by UNS and the notification workers.
'''
from pyon.public import get_sys_name, CFG
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

    def sendmail(self, msg_sender, msg_recipient, msg):
        log.warning('Sending fake message from: %s, to: "%s"' % (msg_sender,  msg_recipient))
        log.info("Fake message sent: %s" % msg)
        self.sent_mail.put((msg_sender, msg_recipient, msg))


def setting_up_smtp_client():
    '''
    Sets up the smtp client
    '''

    #------------------------------------------------------------------------------------
    # the default smtp server
    #------------------------------------------------------------------------------------

    ION_SMTP_SERVER = 'mail.oceanobservatories.org'

    smtp_host = CFG.get_safe('server.smtp.host', ION_SMTP_SERVER)
    smtp_port = CFG.get_safe('server.smtp.port', 25)
    smtp_sender = CFG.get_safe('server.smtp.sender')
    smtp_password = CFG.get_safe('server.smtp.password')

    if CFG.get_safe('system.smtp',False): #Default is False - use the fake_smtp
        log.debug('Using the real SMTP library to send email notifications!')

        smtp_client = smtplib.SMTP(smtp_host)
        smtp_client.ehlo()
        smtp_client.starttls()
        smtp_client.login(smtp_sender, smtp_password)

    else:
        log.debug('Using a fake SMTP library to simulate email notifications!')

        smtp_client = fake_smtplib.SMTP(smtp_host)

    return smtp_client

def send_email(message, msg_recipient, smtp_client):
    '''
    A common method to send email with formatting

    @param message              Event
    @param msg_recipient        str
    @param smtp_client          fake or real smtp client object

    '''

    time_stamp = message.ts_created
    event = message.type_
    origin = message.origin
    description = message.description


    #------------------------------------------------------------------------------------
    # build the email from the event content
    #------------------------------------------------------------------------------------

    msg_body = string.join(("Event: %s," %  event,
                            "",
                            "Originator: %s," %  origin,
                            "",
                            "Description: %s," % description ,
                            "",
                            "Time stamp: %s," %  time_stamp,
                            "",
                            "You received this notification from ION because you asked to be "\
                            "notified about this event from this source. ",
                            "To modify or remove notifications about this event, "\
                            "please access My Notifications Settings in the ION Web UI.",
                            "Do not reply to this email.  This email address is not monitored "\
                            "and the emails will not be read."),
        "\r\n")
    msg_subject = "(SysName: " + get_sys_name() + ") ION event " + event + " from " + origin

    #------------------------------------------------------------------------------------
    # the 'from' email address for notification emails
    #------------------------------------------------------------------------------------

    ION_NOTIFICATION_EMAIL_ADDRESS = 'ION_notifications-do-not-reply@oceanobservatories.org'

    msg_sender = ION_NOTIFICATION_EMAIL_ADDRESS

    msg = MIMEText(msg_body)
    msg['Subject'] = msg_subject
    msg['From'] = msg_sender
    msg['To'] = msg_recipient
    log.debug("UNS sending email to %s"\
    %msg_recipient)

    smtp_sender = CFG.get_safe('server.smtp.sender')

    smtp_client.sendmail(smtp_sender, msg_recipient, msg.as_string())

#    if CFG.get_safe('system.smtp',False):
#        smtp_client.close()

def check_user_notification_interest(event, reverse_user_info):
    '''
    A method to check which user is interested in a notification or an event.
    The input parameter event can be used interchangeably with notification in this method
    Returns the list of users interested in the notification

    @param event                Event
    @param reverse_user_info    dict

    @retval user_ids list
    '''

    user_list_1 = []
    user_list_2 = []
    user_list_3 = []
    user_list_4 = []

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

    if reverse_user_info['event_origin'].has_key(event.origin):
        user_list_1 = reverse_user_info['event_origin'][event.origin]

    if reverse_user_info['event_origin_type'].has_key(event.origin_type):
        user_list_2 = reverse_user_info['event_origin_type'][event.origin_type]

    if reverse_user_info['event_type'].has_key(event.type_):
        user_list_3 = reverse_user_info['event_type'][event.type_]

    if reverse_user_info['event_subtype'].has_key(event.sub_type):
        user_list_4 = reverse_user_info['event_subtype'][event.sub_type]

    users = list( set.intersection(set(user_list_1), set(user_list_2), set(user_list_3), set(user_list_4)))

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

                if dict_1.has_key(notification.event_type) and notification.event_type:
                    dict_1[notification.event_type].append(user_id)
                    # to remove duplicate user names
                    dict_1[notification.event_type] = list(set(dict_1[notification.event_type]))
                else:
                    dict_1[notification.event_type] = [user_id]

                if dict_2.has_key(notification.event_subtype) and notification.event_subtype:
                    dict_2[notification.event_subtype].append(user_id)
                    # to remove duplicate user names
                    dict_2[notification.event_subtype] = list(set(dict_2[notification.event_subtype]))
                else:
                    dict_2[notification.event_subtype] = [user_id]

                if dict_3.has_key(notification.origin) and notification.origin:
                    dict_3[notification.origin].append(user_id)
                    # to remove duplicate user names
                    dict_3[notification.origin] = list(set(dict_3[notification.origin]))
                else:
                    dict_3[notification.origin] = [user_id]

                if dict_4.has_key(notification.origin_type) and notification.origin_type:
                    dict_4[notification.origin_type].append(user_id)
                    # to remove duplicate user names
                    dict_4[notification.origin_type] = list(set(dict_4[notification.origin_type]))
                else:
                    dict_4[notification.origin_type] = [user_id]

                reverse_user_info['event_type'] = dict_1
                reverse_user_info['event_subtype'] = dict_2
                reverse_user_info['event_origin'] = dict_3
                reverse_user_info['event_origin_type'] = dict_4

    return reverse_user_info

