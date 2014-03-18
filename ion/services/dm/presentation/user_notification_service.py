#!/usr/bin/env python

"""Service to manage user requested notifications about events in the system"""

__author__ = 'Bill Bollenbacher, Swarbhanu Chatterjee, David Stuebe, Michael Meisinger'

from pyon.public import RT, PRED, get_sys_name, OT, IonObject, get_ion_ts, log, CFG, BadRequest, IonException, NotFound, Inconsistent, EventPublisher, EventSubscriber
from pyon.core.governance import ORG_MEMBER_ROLE, ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, DATA_OPERATOR, OBSERVATORY_OPERATOR, GovernanceHeaderValues, has_org_role

from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, convert_events_to_email_message, \
    get_event_computed_attributes, calculate_reverse_user_info

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from interface.objects import ComputedValueAvailability, ComputedListValue
from interface.objects import ProcessDefinition, TemporalBounds


class EmailEventProcessor(object):
    """
    For every user that has existing notification requests (who has called
    create_notification()) the UNS will contain a local EventProcessor
    instance that contains the user's notification information (email address)
    and all of the user's notifications (along with their event subscribers).
    The EventProcessors are maintained local to the UNS in a dictionary
    indexed by the user's resourceID.  When a notification is created the user's
    EventProcessor will be created if it doesn't already exist , and it will
    be deleted when the user deletes their last notification.

    The user's EventProcessor will encapsulate a list of notification objects
    that the user has requested, along with user information needed for send notifications
    (email address for LCA). It will also encapsulate a subscriber callback method
    that is passed to all event subscribers for each notification the user has created.

    Each notification object will encapsulate the notification information and a
    list of event subscribers (only one for LCA) that listen for the events in the notification.
    """

    def __init__(self):
        # the resource registry
        # TODO: This should be the client generated for the UNS (a process client)
        self.rr = ResourceRegistryServiceClient()


class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """
    def __init__(self, *args, **kwargs):
        self._schedule_ids = []
        BaseUserNotificationService.__init__(self, *args, **kwargs)

    def on_start(self):
        self.ION_NOTIFICATION_EMAIL_ADDRESS = CFG.get_safe('server.smtp.sender')

        # Create an event processor
        self.event_processor = EmailEventProcessor()

        # Dictionaries that maintain information asetting_up_smtp_clientbout users and their subscribed notifications
        self.user_info = {}

        # The reverse_user_info is calculated from the user_info dictionary
        self.reverse_user_info = {}

        self.event_publisher = EventPublisher(process=self)

        self.start_time = get_ion_ts()

        #------------------------------------------------------------------------------------
        # Create an event subscriber for Reload User Info events
        #------------------------------------------------------------------------------------

        def reload_user_info(event_msg, headers):
            """
            Callback method for the subscriber to ReloadUserInfoEvent
            """

            notification_id =  event_msg.notification_id
            log.debug("(UNS instance) received a ReloadNotificationEvent. The relevant notification_id is %s" % notification_id)

            try:
                self.user_info = self.load_user_info()
            except NotFound:
                log.warning("ElasticSearch has not yet loaded the user_index.")

            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.debug("(UNS instance) After a reload, the user_info: %s" % self.user_info)
            log.debug("(UNS instance) The recalculated reverse_user_info: %s" % self.reverse_user_info)

        # the subscriber for the ReloadUSerInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.ReloadUserInfoEvent,
            origin='UserNotificationService',
            callback=reload_user_info
        )
        self.add_endpoint(self.reload_user_info_subscriber)

    def on_quit(self):
        """
        Handles stop/terminate.

        Cleans up subscribers spawned here, terminates any scheduled tasks to the scheduler.
        """
        for sid in self._schedule_ids:
            try:
                self.clients.scheduler.cancel_timer(sid)
            except IonException as ex:
                log.info("Ignoring exception while cancelling schedule id (%s): %s: %s", sid, ex.__class__.__name__, ex)

        super(UserNotificationService, self).on_quit()

    def set_process_batch_key(self, process_batch_key = ''):
        """
        This method allows an operator to set the process_batch_key, a string.
        Once this method is used by the operator, the UNS will start listening for timer events
        published by the scheduler with origin = process_batch_key.

        @param process_batch_key str
        """
        def process(event_msg, headers):
            self.end_time = get_ion_ts()

            # run the process_batch() method
            self.process_batch(start_time=self.start_time, end_time=self.end_time)
            self.start_time = self.end_time

        # the subscriber for the batch processing
        # To trigger the batch notification, have the scheduler create a timer with event_origin = process_batch_key
        self.batch_processing_subscriber = EventSubscriber(
            event_type=OT.TimerEvent,
            origin=process_batch_key,
            callback=process
        )
        self.add_endpoint(self.batch_processing_subscriber)

    def create_notification(self, notification=None, user_id=''):
        """
        Persists the provided NotificationRequest object for the specified Origin id.
        Associate the Notification resource with the user_id string.
        returned id is the internal id by which NotificationRequest will be identified
        in the data store.

        @param notification        NotificationRequest
        @param user_id             str
        @retval notification_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        if not user_id:
            raise BadRequest("User id not provided.")

        log.debug("Create notification called for user_id: %s, and notification: %s", user_id, notification)

        #---------------------------------------------------------------------------------------------------
        # Persist Notification object as a resource if it has already not been persisted
        #---------------------------------------------------------------------------------------------------

        notification_id = None
        # if the notification has already been registered, simply use the old id
        existing_user_notifications = self.get_user_notifications(user_info_id=user_id)
        if existing_user_notifications:
            notification_id = self._notification_in_notifications(notification, existing_user_notifications)

        # since the notification has not been registered yet, register it and get the id

        temporal_bounds = TemporalBounds()
        temporal_bounds.start_datetime = get_ion_ts()
        temporal_bounds.end_datetime = ''

        if not notification_id:
            notification.temporal_bounds = temporal_bounds
            notification_id, rev = self.clients.resource_registry.create(notification)
        else:
            log.debug("Notification object has already been created in resource registry before. No new id to be generated. notification_id: %s", notification_id)
            # Read the old notification already in the resource registry
            notification = self.clients.resource_registry.read(notification_id)

            # Update the temporal bounds of the old notification resource
            notification.temporal_bounds = temporal_bounds

            # Update the notification in the resource registry
            self.clients.resource_registry.update(notification)

            log.debug("The temporal bounds for this resubscribed notification object with id: %s, is: %s", notification._id,notification.temporal_bounds)


        # Link the user and the notification with a hasNotification association
        assocs= self.clients.resource_registry.find_associations(subject=user_id,
                                                                    predicate=PRED.hasNotification,
                                                                    object=notification_id,
                                                                    id_only=True)

        if assocs:
            log.debug("Got an already existing association: %s, between user_id: %s, and notification_id: %s", assocs,user_id,notification_id)
            return notification_id
        else:
            log.debug("Creating association between user_id: %s, and notification_id: %s", user_id, notification_id )
            self.clients.resource_registry.create_association(user_id, PRED.hasNotification, notification_id)

        # read the registered notification request object because this has an _id and is more useful
        notification = self.clients.resource_registry.read(notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        #log.debug("(create notification) Publishing ReloadUserInfoEvent for notification_id: %s", notification_id)

        self.event_publisher.publish_event( event_type= OT.ReloadUserInfoEvent,
            origin="UserNotificationService",
            description= "A notification has been created.",
            notification_id = notification_id)

        return notification_id

    def update_notification(self, notification=None, user_id = ''):
        """Updates the provided NotificationRequest object.  Throws NotFound exception if
        an existing version of NotificationRequest is not found.  Throws Conflict if
        the provided NotificationRequest object is not based on the latest persisted
        version of the object.

        @param notification     NotificationRequest
        @throws BadRequest      if object does not have _id or _rev attribute
        @throws NotFound        object with specified id does not exist
        @throws Conflict        object not based on latest persisted object version
        """

        raise NotImplementedError("This method needs to be worked out in terms of implementation")

#        #-------------------------------------------------------------------------------------------------------------------
#        # Get the old notification
#        #-------------------------------------------------------------------------------------------------------------------
#
#        old_notification = self.clients.resource_registry.read(notification._id)
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # Update the notification in the notifications dict
#        #-------------------------------------------------------------------------------------------------------------------
#
#
#        self._update_notification_in_notifications_dict(new_notification=notification,
#                                                        notifications=self.notifications)
#        #-------------------------------------------------------------------------------------------------------------------
#        # Update the notification in the registry
#        #-------------------------------------------------------------------------------------------------------------------
#        '''
#        Since one user should not be able to update the notification request resource without the knowledge of other users
#        who have subscribed to the same notification request, we do not update the resource in the resource registry
#        '''
#
##        self.clients.resource_registry.update(notification)
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # reading up the notification object to make sure we have the newly registered notification request object
#        #-------------------------------------------------------------------------------------------------------------------
#
#        notification_id = notification._id
#        notification = self.clients.resource_registry.read(notification_id)
#
#        #------------------------------------------------------------------------------------
#        # Update the UserInfo object
#        #------------------------------------------------------------------------------------
#
#        user = self.update_user_info_object(user_id, notification)
#
#        #-------------------------------------------------------------------------------------------------------------------
#        # Generate an event that can be picked by notification workers so that they can update their user_info dictionary
#        #-------------------------------------------------------------------------------------------------------------------
#        log.info("(update notification) Publishing ReloadUserInfoEvent for updated notification")
#
#        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
#            origin="UserNotificationService",
#            description= "A notification has been updated.",
#            notification_id = notification_id
#        )

    def read_notification(self, notification_id=''):
        """Returns the NotificationRequest object for the specified notification id.
        Throws exception if id does not match any persisted NotificationRequest
        objects.

        @param notification_id    str
        @retval notification    NotificationRequest
        @throws NotFound    object with specified id does not exist
        """
        notification = self.clients.resource_registry.read(notification_id)

        return notification

    def delete_notification(self, notification_id=''):
        """For now, permanently deletes NotificationRequest object with the specified
        id. Throws exception if id does not match any persisted NotificationRequest.

        @param notification_id    str
        @throws NotFound    object with specified id does not exist
        """

        #-------------------------------------------------------------------------------------------------------------------
        # Stop the event subscriber for the notification
        #-------------------------------------------------------------------------------------------------------------------
        notification_request = self.clients.resource_registry.read(notification_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Update the resource registry
        #-------------------------------------------------------------------------------------------------------------------

        notification_request.temporal_bounds.end_datetime = get_ion_ts()

        self.clients.resource_registry.update(notification_request)

        #-------------------------------------------------------------------------------------------------------------------
        # Find users who are interested in the notification and update the notification in the list maintained by the UserInfo object
        #-------------------------------------------------------------------------------------------------------------------
#        user_ids, _ = self.clients.resource_registry.find_subjects(RT.UserInfo, PRED.hasNotification, notification_id, True)
#
#        for user_id in user_ids:
#            self.update_user_info_object(user_id, notification_request)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(delete notification) Publishing ReloadUserInfoEvent for notification_id: %s", notification_id)

        self.event_publisher.publish_event( event_type= OT.ReloadUserInfoEvent,
            origin="UserNotificationService",
            description= "A notification has been deleted.",
            notification_id = notification_id)

#    def delete_notification_from_user_info(self, notification_id):
#        """
#        Helper method to delete the notification from the user_info dictionary
#
#        @param notification_id str
#        """
#
#        user_ids, assocs = self.clients.resource_registry.find_subjects(object=notification_id, predicate=PRED.hasNotification, id_only=True)
#
#        for assoc in assocs:
#            self.clients.resource_registry.delete_association(assoc)
#
#        for user_id in user_ids:
#
#            value = self.user_info[user_id]
#
#            for notif in value['notifications']:
#                if notification_id == notif._id:
#                    # remove the notification
#                    value['notifications'].remove(notif)
#
#        self.reverse_user_info = calculate_reverse_user_info(self.user_info)

    def get_user_notifications(self, user_info_id=''):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_info_id str

        @retval notifications list of NotificationRequest objects
        """
        notifications = []
        user_notif_req_objs, _ = self.clients.resource_registry.find_objects(
            subject=user_info_id, predicate=PRED.hasNotification, object_type=RT.NotificationRequest, id_only=False)

        log.debug("Got %s notifications, for the user: %s", len(user_notif_req_objs), user_info_id)

        for notif in user_notif_req_objs:
            # do not include notifications that have expired
            if notif.temporal_bounds.end_datetime == '':
                    notifications.append(notif)

        return notifications


    def create_worker(self, number_of_workers=1):
        """
        Creates notification workers

        @param number_of_workers int
        @retval pids list

        """

        pids = []

        for n in xrange(number_of_workers):

            process_definition = ProcessDefinition( name='notification_worker_%s' % n)

            process_definition.executable = {
                'module': 'ion.processes.data.transforms.notification_worker',
                'class':'NotificationWorker'
            }
            process_definition_id = self.clients.process_dispatcher.create_process_definition(process_definition=process_definition)

            # ------------------------------------------------------------------------------------
            # Process Spawning
            # ------------------------------------------------------------------------------------

            pid2 = self.clients.process_dispatcher.create_process(process_definition_id)

            #@todo put in a configuration
            configuration = {}
            configuration['process'] = dict({
                'name': 'notification_worker_%s' % n,
                'type':'simple',
                'queue_name': 'notification_worker_queue'
            })

            pid  = self.clients.process_dispatcher.schedule_process(
                process_definition_id,
                configuration = configuration,
                process_id=pid2
            )

            pids.append(pid)

        return pids

    def process_batch(self, start_time = '', end_time = ''):
        """
        This method is launched when an process_batch event is received. The user info dictionary maintained
        by the User Notification Service is used to query the event repository for all events for a particular
        user that have occurred in a provided time interval, and then an email is sent to the user containing
        the digest of all the events.

        @param start_time int milliseconds
        @param end_time int milliseconds
        """
        self.smtp_client = setting_up_smtp_client()

        if end_time <= start_time:
            return

        for user_id, value in self.user_info.iteritems():

            notifications = self.get_user_notifications(user_info_id=user_id)
            notifications_disabled = value['notifications_disabled']
            notifications_daily_digest = value['notifications_daily_digest']


            # Ignore users who do NOT want batch notifications or who have disabled the delivery switch
            # However, if notification preferences have not been set for the user, use the default mechanism and do not bother
            if notifications_disabled or not notifications_daily_digest:
                    continue

            events_for_message = []

            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (start_time, end_time)

            for notification in notifications:
                # If the notification request has expired, then do not use it in the search
                if notification.temporal_bounds.end_datetime:
                    continue

                event_tuples = self.container.event_repository.find_events(
                    origin=notification.origin,
                    event_type=notification.event_type,
                    start_ts=start_time,
                    end_ts=end_time)
                events = [item[2] for item in event_tuples]

                events_for_message.extend(events)

            log.debug("Found following events of interest to user, %s: %s", user_id, events_for_message)

            # send a notification email to each user using a _send_email() method
            if events_for_message:
                self.format_and_send_email(events_for_message = events_for_message,
                                            user_id = user_id,
                                            smtp_client=self.smtp_client)

        self.smtp_client.quit()


    def format_and_send_email(self, events_for_message=None, user_id=None, smtp_client=None):
        """
        Format the message for a particular user containing information about the events he is to be notified about

        @param events_for_message list
        @param user_id str
        """
        message = str(events_for_message)
        log.debug("The user, %s, will get the following events in his batch notification email: %s", user_id, message)

        msg = convert_events_to_email_message(events_for_message, self.clients.resource_registry)
        msg["Subject"] = "(SysName: " + get_sys_name() + ") ION event "
        msg["To"] = self.user_info[user_id]['user_contact'].email
        self.send_batch_email(msg, smtp_client)


    def send_batch_email(self, msg=None, smtp_client=None):
        """
        Send the email

        @param msg MIMEText object of email message
        @param smtp_client object
        """

        if msg is None: msg = {}
        for f in ["Subject", "To"]:
            if not f in msg: raise BadRequest("'%s' not in msg %s" % (f, msg))

        msg_subject = msg["Subject"]
        msg_recipient = msg["To"]

        msg['From'] = self.ION_NOTIFICATION_EMAIL_ADDRESS
        log.debug("UNS sending batch (digest) email from %s to %s",
                  self.ION_NOTIFICATION_EMAIL_ADDRESS,
                  msg_recipient)

        smtp_sender = CFG.get_safe('server.smtp.sender')

        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())

    def update_user_info_object(self, user_id, new_notification):
        """
        Update the UserInfo object. If the passed in parameter, od_notification, is None, it does not need to remove the old notification

        @param user_id str
        @param new_notification NotificationRequest
        """

        #this is not necessary if notifiactions are not stored in the userinfo object
        raise NotImplementedError("This method is not necessary because Notifications are not stored in the userinfo object")

        #------------------------------------------------------------------------------------
        # read the user
        #------------------------------------------------------------------------------------

#        user = self.clients.resource_registry.read(user_id)
#
#        if not user:
#            raise BadRequest("No user with the provided user_id: %s" % user_id)
#
#        for item in user.variables:
#            if type(item) is dict and item.has_key('name') and item['name'] == 'notifications':
#                for notif in item['value']:
#                    if notif._id == new_notification._id:
#                        log.debug("came here for updating notification")
#                        notifications = item['value']
#                        notifications.remove(notif)
#                        notifications.append(new_notification)
#                break
#            else:
#                log.warning('Invalid variables attribute on UserInfo instance. UserInfo: %s', user)
#
#
#        #------------------------------------------------------------------------------------
#        # update the resource registry
#        #------------------------------------------------------------------------------------
#
#        log.debug("user.variables::: %s", user.variables)
#
#        self.clients.resource_registry.update(user)
#
#        return user


    def get_subscriptions(self, resource_id='', user_id = '', include_nonactive=False):
        """
        @param resource_id  a resource id (or other origin) that is the origin of events for notifications
        @param user_id  a UserInfo ID that owns the NotificationRequest
        @param include_nonactive  if False, filter to active NotificationRequest only
        Return all NotificationRequest resources where origin is given resource_id.
        """
        notif_reqs, _ = self.clients.resource_registry.find_resources_ext(
            restype=RT.NotificationRequest, attr_name="origin", attr_value=resource_id, id_only=False)

        log.debug("Got %s active and past NotificationRequests for resource/origin %s", len(notif_reqs), resource_id)

        if not include_nonactive:
            notif_reqs = [nr for nr in notif_reqs if nr.temporal_bounds.end_datetime == '']
            log.debug("Filtered to %s active NotificationRequests", len(notif_reqs))

        if user_id:
            # Get all NotificationRequests (ID) that are associated to given UserInfo_id
            user_notif_req_ids, _ = self.clients.resource_registry.find_objects(
                subject=user_id, predicate=PRED.hasNotification, object_type=RT.NotificationRequest, id_only=True)

            notif_reqs = [nr for nr in notif_reqs if nr._id in user_notif_req_ids]
            log.debug("Filtered to %s NotificationRequests associated to user %s", len(notif_reqs), user_id)

        return notif_reqs

    def get_subscriptions_attribute(self, resource_id='', user_id = '', include_nonactive=False):
        retval = self.get_subscriptions(resource_id=resource_id, user_id=user_id, include_nonactive=include_nonactive)
        container = ComputedListValue(value=retval)
        return container


    def _notification_in_notifications(self, notification = None, notifications = None):

        for notif in notifications:
            if notif.name == notification.name and \
            notif.origin == notification.origin and \
            notif.origin_type == notification.origin_type and \
            notif.event_type == notification.event_type:
                return notif._id
        return None

    def load_user_info(self):
        """
        Method to load the user info dictionary used by the notification workers and the UNS

        @retval user_info dict
        """
        users, _ = self.clients.resource_registry.find_resources(restype=RT.UserInfo)

        user_info = {}

        if not users:
            return {}

        for user in users:
            notifications = []
            notifications_disabled = False
            notifications_daily_digest = False

            #retrieve all the active notifications assoc to this user
            notifications = self.get_user_notifications(user_info_id=user)
            log.debug('load_user_info notifications:   %s', notifications)

            for variable in user.variables:
                if type(variable) is dict and variable.has_key('name'):

                    if variable['name'] == 'notifications_daily_digest':
                        notifications_daily_digest = variable['value']

                    if variable['name'] == 'notifications_disabled':
                        notifications_disabled = variable['value']

                else:
                    log.warning('Invalid variables attribute on UserInfo instance. UserInfo: %s', user)

            user_info[user._id] = { 'user_contact' : user.contact, 'notifications' : notifications,
                                    'notifications_daily_digest' : notifications_daily_digest, 'notifications_disabled' : notifications_disabled}

        return user_info


    # -------------------------------------------------------------------------
    #  Governance operations

    def check_subscription_policy(self, process, message, headers):

        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=False)

        except Inconsistent, ex:
            return False, ex.message

        if gov_values.op == 'delete_notification':
            return True, ''

        notification = message['notification']
        resource_id = notification.origin

        if notification.origin_type == RT.Org:
            org = self.clients.resource_registry.read(resource_id)
            if (has_org_role(gov_values.actor_roles, org.org_governance_name, [ORG_MEMBER_ROLE])):
                    return True, ''
        else:
            orgs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
            for org in orgs:
                if (has_org_role(gov_values.actor_roles, org.org_governance_name, [ORG_MEMBER_ROLE])):
                    return True, ''

        return False, '%s(%s) has been denied since the user is not a member in any org to which the resource id %s belongs ' % (process.name, gov_values.op, resource_id)

    def check_publish_event_policy(self, process, message, headers):

        try:
            gov_values = GovernanceHeaderValues(headers=headers, process=process, resource_id_required=False)

        except Inconsistent, ex:
            return False, ex.message

        if (message['event_type'] == 'ResourceIssueReportedEvent') and (has_org_role(gov_values.actor_roles, 'ION', [ORG_MEMBER_ROLE])):
                    return True, ''

        resource_id = message.origin

        if message.origin_type == RT.Org:
            org = self.clients.resource_registry.read(resource_id)
            if (has_org_role(gov_values.actor_roles, org.org_governance_name, [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                    return True, ''
            else:
                    return False, 'user does not have appropriate role in %s' % (org.org_governance_name)

        # Allow actor to activate/deactivate deployment in an org where the actor has the appropriate role
        orgs,_ = self.clients.resource_registry.find_subjects(subject_type=RT.Org, predicate=PRED.hasResource, object=resource_id, id_only=False)
        for org in orgs:
            log.error('org: ' +str(org))
            if (has_org_role(gov_values.actor_roles, org.org_governance_name, [ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, OBSERVATORY_OPERATOR, DATA_OPERATOR])):
                log.error('returning true: ')
                return True, ''


        return False, '%s(%s) has been denied since the user is not a member in any org to which the origin id %s belongs ' % (process.name, gov_values.op, resource_id)


    # -------------------------------------------------------------------------
    # Events operations

    def publish_event_object(self, event=None):
        """
        Publishes an event based on the given event object.
        Returns the published event object with filled out _id and other attributes.
        """
        event = self.event_publisher.publish_event_object(event_object=event)
        log.info("Event published: %s", event)

        return event

    def publish_event(self, event_type='', origin='', origin_type='', sub_type='', description='', event_attrs=None):
        """
        Publishes an event of given type based on given basic and additional attributes.
        Returns the published event object with filled out _id and other attributes.
        """
        event_attrs = event_attrs or {}

        event = self.event_publisher.publish_event(
            event_type = event_type,
            origin = origin,
            origin_type = origin_type,
            sub_type = sub_type,
            description = description,
            **event_attrs
            )
        log.info("Event published: %s", event)

        return event

    def find_events(self, origin='', type='', min_datetime='', max_datetime='', limit=-1,
                    descending=False, skip=0, computed=False):
        """
        Returns a list of events that match the specified search criteria.
        Can return a list of EventComputedAttributes if requested with event objects contained.
        Pagination arguments are supported.

        @param origin         str
        @param min_datetime   str  milliseconds
        @param max_datetime   str  milliseconds
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        """
        if limit == 0:
            limit = int(self.CFG.get_safe("service.user_notification.max_events_limit", 1000))
        if max_datetime == "now":
            max_datetime = get_ion_ts()

        event_tuples = self.container.event_repository.find_events(event_type=type, origin=origin,
                                                                   start_ts=min_datetime, end_ts=max_datetime,
                                                                   limit=limit, descending=descending, skip=skip)

        events = [item[2] for item in event_tuples]
        log.debug("find_events found %s events", len(events))

        if computed:
            computed_events = self._get_computed_events(events, include_events=True)
            events = computed_events.computed_list

        return events

    def get_recent_events(self, resource_id='', limit=0, skip=0):
        """
        Returns a list of EventComputedAttributes for events that match the resource id as origin,
        in descending order, most recent first. The total number of events is limited by default
        based on system configuration. Pagination arguments are supported.
        @param resource_id str
        @param limit int (if 0 is given
        @retval ComputedListValue with value list of 4-tuple with Event objects
        """
        if limit == 0:
            limit = int(self.CFG.get_safe("service.user_notification.max_events_limit", 1000))
        now = get_ion_ts()
        events = self.find_events(origin=resource_id, max_datetime=now,
                                  descending=True, limit=limit, skip=skip)

        computed_events = self._get_computed_events(events)

        return computed_events

    def _get_computed_events(self, events, add_usernames=True, include_events=False):
        """
        Get events for use in extended resource computed attribute
        @retval ComputedListValue with value list of 4-tuple with Event objects
        """
        events = events or []

        ret = IonObject(OT.ComputedEventListValue)
        ret.value = events
        ret.computed_list = [get_event_computed_attributes(event, include_event=include_events) for event in events]
        ret.status = ComputedValueAvailability.PROVIDED

        if add_usernames:
            try:
                actor_ids = {evt.actor_id for evt in events if evt.actor_id}
                log.debug("Looking up UserInfo for actors: %s" % actor_ids)
                if actor_ids:
                    userinfo_list, assoc_list = self.clients.resource_registry.find_objects_mult(actor_ids,
                                                                                                 predicate=PRED.hasInfo,
                                                                                                 id_only=False)
                    actor_map = {assoc.s: uinfo for uinfo, assoc in zip(userinfo_list, assoc_list)}

                    for evt, evt_cmp in zip(events, ret.computed_list):
                        ui = actor_map.get(evt.actor_id, None)
                        if ui:
                            evt_cmp["event_summary"] += " [%s %s]" % (ui.contact.individual_names_given, ui.contact.individual_name_family)

            except Exception as ex:
                log.exception("Cannot find user names for event actor_ids")

        return ret
