#!/usr/bin/env python
"""
@author Bill Bollenbacher
@author Swarbhanu Chatterjee
@author David Stuebe
@file ion/services/dm/presentation/user_notification_service.py
@description Implementation of the UserNotificationService
"""

import pprint
import string
import time
from email.mime.text import MIMEText
from datetime import datetime

from pyon.core.exception import BadRequest, IonException, NotFound
from pyon.core.bootstrap import CFG
from pyon.util.log import log
from pyon.util.containers import get_ion_ts
from pyon.public import RT, PRED, get_sys_name, Container, OT, IonObject
from pyon.event.event import EventPublisher, EventSubscriber
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client
from ion.services.dm.utility.uns_utility_methods import calculate_reverse_user_info, _convert_to_human_readable

from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.objects import ComputedValueAvailability, NotificationDeliveryModeEnum, ComputedListValue, DeviceStatusType
from interface.objects import ProcessDefinition, TemporalBounds
from interface.services.dm.iuser_notification_service import BaseUserNotificationService


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


class EmailEventProcessor(object):
    """
    A class that helps to get user subscribed to notifications
    """

    def __init__(self):
        # the resource registry
        self.rr = ResourceRegistryServiceClient()

    def add_notification_for_user(self, new_notification=None, user_id=''):
        """
        Add a notification to the user's list of subscribed notifications
        @param notification_request NotificationRequest
        @param user_id str
        """
        user = self.rr.read(user_id)

        cond = True
        for item in user.variables:
            if item.has_key('name') and item['name'] == 'notifications':
                cond = False

        if cond: # this means that this dict is not already filled in the user info object
            dict = {'name' : 'notifications', 'value' : [new_notification]}
            user.variables.append(dict)

        else: # the user has previous notifications
            for item in user.variables:
                if item.has_key('name') and item['name'] == 'notifications':
                    item['value'].append(new_notification)
                    break

        #------------------------------------------------------------------------------------
        # update the resource registry
        #------------------------------------------------------------------------------------

        self.rr.update(user)

        return user

#----------------------------------------------------------------------------------------------------------------
# Keep this note for the time when we need to also include sms delivery via email to sms providers
#        provider_email = sms_providers[provider] # self.notification.delivery_config.delivery['provider']
#        self.msg_recipient = notification_request.delivery_config.delivery['phone_number'] + provider_email
#----------------------------------------------------------------------------------------------------------------

class UserNotificationService(BaseUserNotificationService):
    """
    A service that provides users with an API for CRUD methods for notifications.
    """

    def __init__(self, *args, **kwargs):
        self._subscribers = []
        self._schedule_ids = []
        BaseUserNotificationService.__init__(self, *args, **kwargs)

    def on_start(self):

        #---------------------------------------------------------------------------------------------------
        # Get the event Repository
        #---------------------------------------------------------------------------------------------------

#        self.ION_NOTIFICATION_EMAIL_ADDRESS = 'data_alerts@oceanobservatories.org'
        self.ION_NOTIFICATION_EMAIL_ADDRESS = CFG.get_safe('server.smtp.sender')


        #---------------------------------------------------------------------------------------------------
        # Create an event processor
        #---------------------------------------------------------------------------------------------------

        self.event_processor = EmailEventProcessor()

        #---------------------------------------------------------------------------------------------------
        # load event originators, types, and table
        #---------------------------------------------------------------------------------------------------

        self.notifications = {}

        #---------------------------------------------------------------------------------------------------
        # Dictionaries that maintain information about users and their subscribed notifications
        # The reverse_user_info is calculated from the user_info dictionary
        #---------------------------------------------------------------------------------------------------
        self.user_info = {}
        self.reverse_user_info = {}

        #---------------------------------------------------------------------------------------------------
        # Get the clients
        #---------------------------------------------------------------------------------------------------

        self.discovery = DiscoveryServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.event_publisher = EventPublisher()
        self.datastore = self.container.datastore_manager.get_datastore('events')

        self.start_time = get_ion_ts()

        #------------------------------------------------------------------------------------
        # Create an event subscriber for Reload User Info events
        #------------------------------------------------------------------------------------

        def reload_user_info(event_msg, headers):
            '''
            Callback method for the subscriber to ReloadUserInfoEvent
            '''

            notification_id =  event_msg.notification_id
            log.debug("(UNS instance received a ReloadNotificationEvent. The relevant notification_id is %s" % notification_id)

            try:
                self.user_info = self.load_user_info()
            except NotFound:
                log.warning("ElasticSearch has not yet loaded the user_index.")

            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.debug("(UNS instance) After a reload, the user_info: %s" % self.user_info)
            log.debug("(UNS instance) The recalculated reverse_user_info: %s" % self.reverse_user_info)

        # the subscriber for the ReloadUSerInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type="ReloadUserInfoEvent",
            origin='UserNotificationService',
            callback=reload_user_info
        )
        self.reload_user_info_subscriber.start()
        # For cleanup of the subscriber
        self._subscribers.append(self.reload_user_info_subscriber)

    def on_quit(self):
        """
        Handles stop/terminate.

        Cleans up subscribers spawned here, terminates any scheduled tasks to the scheduler.
        """
        for sub in self._subscribers:
            sub.stop()

        for sid in self._schedule_ids:
            try:
                self.clients.scheduler.cancel_timer(sid)
            except IonException as ex:
                log.info("Ignoring exception while cancelling schedule id (%s): %s: %s", sid, ex.__class__.__name__, ex)

        super(UserNotificationService, self).on_quit()

    def __now(self):
        """
        This method defines what the UNS uses as its "current" time
        """
        return datetime.utcnow()

    def set_process_batch_key(self, process_batch_key = ''):
        """
        This method allows an operator to set the process_batch_key, a string.
        Once this method is used by the operator, the UNS will start listening for timer events
        published by the scheduler with origin = process_batch_key.

        @param process_batch_key str
        """

        def process(event_msg, headers):
            self.end_time = get_ion_ts()

            log.debug("process_batch being called with start_time = %s, end_time = %s", self.start_time, self.end_time)

            # run the process_batch() method
            self.process_batch(start_time=self.start_time, end_time=self.end_time)
            self.start_time = self.end_time

        # the subscriber for the batch processing
        """
        To trigger the batch notification, have the scheduler create a timer with event_origin = process_batch_key
        """
        self.batch_processing_subscriber = EventSubscriber(
            event_type="ResourceEvent",
            origin=process_batch_key,
            queue_name='user_notification',
            callback=process
        )
        self.batch_processing_subscriber.start()
        self._subscribers.append(self.batch_processing_subscriber)

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

        # if the notification has already been registered, simply use the old id
        notification_id = self._notification_in_notifications(notification, self.notifications)

        # since the notification has not been registered yet, register it and get the id

        temporal_bounds = TemporalBounds()
        temporal_bounds.start_datetime = get_ion_ts()
        temporal_bounds.end_datetime = ''

        if not notification_id:
            notification.temporal_bounds = temporal_bounds
            notification_id, _ = self.clients.resource_registry.create(notification)
            self.notifications[notification_id] = notification
        else:
            log.debug("Notification object has already been created in resource registry before. No new id to be generated. notification_id: %s", notification_id)
            # Read the old notification already in the resource registry
            notification = self.clients.resource_registry.read(notification_id)

            # Update the temporal bounds of the old notification resource
            notification.temporal_bounds = temporal_bounds

            # Update the notification in the resource registry
            self.clients.resource_registry.update(notification)

            log.debug("The temporal bounds for this resubscribed notification object with id: %s, is: %s", notification_id,notification.temporal_bounds)


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

        # Update the user info object with the notification
        self.event_processor.add_notification_for_user(new_notification=notification, user_id=user_id)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.debug("(create notification) Publishing ReloadUserInfoEvent for notification_id: %s", notification_id)

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
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
        user_ids, _ = self.clients.resource_registry.find_subjects(RT.UserInfo, PRED.hasNotification, notification_id, True)

        for user_id in user_ids:
            self.update_user_info_object(user_id, notification_request)

        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by a notification worker so that it can update its user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(delete notification) Publishing ReloadUserInfoEvent for notification_id: %s", notification_id)

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
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

    def find_events(self, origin='', type='', min_datetime=0, max_datetime=0, limit= -1, descending=False):
        """
        This method leverages couchdb view and simple filters. It does not use elastic search.

        Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param event_type     str
        @param min_datetime   int  seconds
        @param max_datetime   int  seconds
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified parameters does not exist
        @throws NotFound    object with specified parameters does not exist
        """

        event_tuples = self.container.event_repository.find_events(event_type=type, origin=origin, start_ts=min_datetime, end_ts=max_datetime, limit=limit, descending=descending)

        events = [item[2] for item in event_tuples]
        log.debug("(find_events) UNS found the following relevant events: %s", events)

        return events


    #todo Uses Elastic Search. Later extend this to a larger search criteria
    def find_events_extended(self, origin='', type='', min_time= 0, max_time=0, limit=-1, descending=False):
        """Uses Elastic Search. Returns a list of events that match the specified search criteria. Will throw a not NotFound exception
        if no events exist for the given parameters.

        @param origin         str
        @param type           str
        @param min_time   int seconds
        @param max_time   int seconds
        @param limit          int         (integer limiting the number of results (0 means unlimited))
        @param descending     boolean     (if True, reverse order (of production time) is applied, e.g. most recent first)
        @retval event_list    []
        @throws NotFound    object with specified parameters does not exist
        @throws NotFound    object with specified parameters does not exist
        """

        query = []

        if min_time and max_time:
            query.append( "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (min_time, max_time))

        if origin:
            query.append( 'search "origin" is "%s" from "events_index"' % origin)

        if type:
            query.append( 'search "type_" is "%s" from "events_index"' % type)

        search_string = ' and '.join(query)


        # get the list of ids corresponding to the events
        ret_vals = self.discovery.parse(search_string)
        if len(query) > 1:
            events = self.datastore.read_mult(ret_vals)
        else:
            events = [i['_source'] for i in ret_vals]

        log.debug("(find_events_extended) Discovery search returned the following event ids: %s", ret_vals)


        log.debug("(find_events_extended) UNS found the following relevant events: %s", events)

        if limit > 0:
            return events[:limit]

        #todo implement time ordering: ascending or descending

        return events

    def publish_event_object(self, event=None):
        """
        This service operation would publish the given event from an event object.

        @param event    !Event
        @retval event   !Event
        """
        event = self.event_publisher.publish_event_object(event_object=event)
        log.info("The publish_event_object(event) method of UNS was used to publish the event: %s", event )

        return event

    def publish_event(self, event_type='', origin='', origin_type='', sub_type='', description='', event_attrs=None):
        """
        This service operation assembles a new Event object based on event_type 
        (e.g. via the pyon Event publisher) with optional additional attributes from a event_attrs
        dict of arbitrary attributes.
        
        
        @param event_type   str
        @param origin       str
        @param origin_type  str
        @param sub_type     str
        @param description  str
        @param event_attrs  dict
        @retval event       !Event
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
        log.info("The publish_event() method of UNS was used to publish an event: %s", event)

        return event

    def get_recent_events(self, resource_id='', limit = 100):
        """
        Get recent events for use in extended resource computed attribute
        @param resource_id str
        @param limit int
        @retval ComputedListValue with value list of 4-tuple with Event objects
        """

        now = get_ion_ts()
        events = self.find_events(origin=resource_id, limit=limit, max_datetime=now, descending=True)

        ret = IonObject(OT.ComputedEventListValue)
        if events:
            ret.value = events
            ret.computed_list = [self._get_event_computed_attributes(event) for event in events]
            ret.status = ComputedValueAvailability.PROVIDED
        else:
            ret.status = ComputedValueAvailability.NOTAVAILABLE

        return ret

    def _get_event_computed_attributes(self, event):
        """
        @param event any Event to compute attributes for
        @retval an EventComputedAttributes object for given event
        """
        evt_computed = IonObject(OT.EventComputedAttributes)
        evt_computed.event_id = event._id
        evt_computed.ts_computed = get_ion_ts()

        try:
            summary = self._get_event_summary(event)
            evt_computed.event_summary = summary

            spc_attrs = ["%s:%s" % (k, str(getattr(event, k))[:50]) for k in sorted(event.__dict__.keys()) if k not in ['_id', '_rev', 'type_', 'origin', 'origin_type', 'ts_created', 'base_types']]
            evt_computed.special_attributes = ", ".join(spc_attrs)

            evt_computed.event_attributes_formatted = pprint.pformat(event.__dict__)
        except Exception as ex:
            log.exception("Error computing EventComputedAttributes for event %s" % event)

        return evt_computed

    def _get_event_summary(self, event):
        event_types = [event.type_] + event.base_types
        summary = ""
        if "ResourceLifecycleEvent" in event_types:
            summary = "%s lifecycle state change: %s" % (event.origin_type, event.new_state)
        elif "ResourceModifiedEvent" in event_types:
            summary = "%s modified: %s" % (event.origin_type, event.sub_type)

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

        elif "ResourceAgentResourceCommandEvent" in event_types:
            summary = "%s agent resource command '%s(%s)' executed: %s" % (event.origin_type, event.command, event.execute_command, "OK" if event.result is None else event.result)
        elif "DeviceStatusEvent" in event_types:
            summary = "%s '%s' status change: %s" % (event.origin_type, event.sub_type, DeviceStatusType._str_map.get(event.state,"???"))
        elif "DeviceOperatorEvent" in event_types or "ResourceOperatorEvent" in event_types:
            summary = "Operator entered: %s" % event.description

        elif "OrgMembershipGrantedEvent" in event_types:
            summary = "Joined Org '%s' as member" % (event.org_name)
        elif "OrgMembershipCancelledEvent" in event_types:
            summary = "Cancelled Org '%s' membership" % (event.org_name)
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

#        if event.description and summary:
#            summary = summary + ". " + event.description
#        elif event.description:
#            summary = event.description
        return summary

    def get_user_notifications(self, user_info_id=''):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_info_id str

        @retval notifications list of NotificationRequest objects
        """

        if self.user_info.has_key(user_info_id):
            notifications = self.user_info[user_info_id]['notifications']

            log.debug("Got %s notifications, for the user: %s", len(notifications), user_info_id)

            for notif in notifications:
                # remove notifications that have expired
                if notif.temporal_bounds.end_datetime != '':
                    log.debug("removing notification: %s", notif)
                    notifications.remove(notif)

            return notifications

#            ret = IonObject(OT.ComputedListValue)
#
#            if notifications:
#                ret.value = notifications
#                ret.status = ComputedValueAvailability.PROVIDED
#            else:
#                ret.status = ComputedValueAvailability.NOTAVAILABLE
#            return ret
#        else:
#            return None

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
            process_definition_id = self.process_dispatcher.create_process_definition(process_definition=process_definition)

            # ------------------------------------------------------------------------------------
            # Process Spawning
            # ------------------------------------------------------------------------------------

            pid2 = self.process_dispatcher.create_process(process_definition_id)

            #@todo put in a configuration
            configuration = {}
            configuration['process'] = dict({
                'name': 'notification_worker_%s' % n,
                'type':'simple',
                'queue_name': 'notification_worker_queue'
            })

            pid  = self.process_dispatcher.schedule_process(
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

            notifications = value['notifications']
            notification_preferences = value['notification_preferences']

            # Ignore users who do NOT want batch notifications or who have disabled the delivery switch
            # However, if notification preferences have not been set for the user, use the default mechanism and do not bother
            if notification_preferences:
                if notification_preferences.delivery_mode != NotificationDeliveryModeEnum.BATCH \
                    or not notification_preferences.delivery_enabled:
                    continue

            events_for_message = []

            search_time = "SEARCH 'ts_created' VALUES FROM %s TO %s FROM 'events_index'" % (start_time, end_time)

            for notification in notifications:

                # If the notification request has expired, then do not use it in the search
                if notification.temporal_bounds.end_datetime:
                    continue

                if notification.origin:
                    search_origin = 'search "origin" is "%s" from "events_index"' % notification.origin
                else:
                    search_origin = 'search "origin" is "*" from "events_index"'

                if notification.origin_type:
                    search_origin_type= 'search "origin_type" is "%s" from "events_index"' % notification.origin_type
                else:
                    search_origin_type= 'search "origin_type" is "*" from "events_index"'

                if notification.event_type:
                    search_event_type = 'search "type_" is "%s" from "events_index"' % notification.event_type
                else:
                    search_event_type = 'search "type_" is "*" from "events_index"'

                search_string = search_time + ' and ' + search_origin + ' and ' + search_origin_type + ' and ' + search_event_type

                # get the list of ids corresponding to the events
                ret_vals = self.discovery.parse(search_string)

                events_for_message.extend(self.datastore.read_mult(ret_vals))

            log.debug("Found following events of interest to user, %s: %s", user_id, events_for_message)

            # send a notification email to each user using a _send_email() method
            if events_for_message:
                self.format_and_send_email(events_for_message = events_for_message,
                                            user_id = user_id,
                                            smtp_client=self.smtp_client)

        self.smtp_client.quit()


    def format_and_send_email(self, events_for_message = None, user_id = None, smtp_client = None):
        """
        Format the message for a particular user containing information about the events he is to be notified about

        @param events_for_message list
        @param user_id str
        """

        message = str(events_for_message)
        log.debug("The user, %s, will get the following events in his batch notification email: %s", user_id, message)

        msg_body = ''
        count = 1

        for event in events_for_message:

            ts_created = _convert_to_human_readable(event.ts_created)

            msg_body += string.join(("\r\n",
                                     "Event %s: %s" %  (count, event),
                                     "",
                                     "Originator: %s" %  event.origin,
                                     "",
                                     "Description: %s" % event.description or "Not provided",
                                     "",
                                     "ts_created: %s" %  ts_created,
                                     "\r\n",
                                     "------------------------"
                                     "\r\n"))
            count += 1

        msg_body += "You received this notification from ION because you asked to be " +\
                    "notified about this event from this source. " +\
                    "To modify or remove notifications about this event, " +\
                    "please access My Notifications Settings in the ION Web UI. " +\
                    "Do not reply to this email.  This email address is not monitored " +\
                    "and the emails will not be read. \r\n "


        log.debug("The email has the following message body: %s", msg_body)

        msg_subject = "(SysName: " + get_sys_name() + ") ION event "

        self.send_batch_email(  msg_body = msg_body,
            msg_subject = msg_subject,
            msg_recipient=self.user_info[user_id]['user_contact'].email,
            smtp_client=smtp_client )


    def send_batch_email(self, msg_body = None, msg_subject = None, msg_recipient = None, smtp_client = None):
        """
        Send the email

        @param msg_body str
        @param msg_subject str
        @param msg_recipient str
        @param smtp_client object
        """

        msg = MIMEText(msg_body)
        msg['Subject'] = msg_subject
        msg['From'] = self.ION_NOTIFICATION_EMAIL_ADDRESS
        msg['To'] = msg_recipient
        log.debug("UNS sending batch (digest) email from %s to %s" , self.ION_NOTIFICATION_EMAIL_ADDRESS, msg_recipient)

        smtp_sender = CFG.get_safe('server.smtp.sender')

        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())

    def update_user_info_object(self, user_id, new_notification):
        """
        Update the UserInfo object. If the passed in parameter, od_notification, is None, it does not need to remove the old notification

        @param user_id str
        @param new_notification NotificationRequest
        @param old_notification NotificationRequest
        """

        #------------------------------------------------------------------------------------
        # read the user
        #------------------------------------------------------------------------------------

        user = self.clients.resource_registry.read(user_id)

        if not user:
            raise BadRequest("No user with the provided user_id: %s" % user_id)

        for item in user.variables:
            if item['name'] == 'notifications':
                for notif in item['value']:
                    if notif._id == new_notification._id:
                        log.debug("came here for updating notification")
                        notifications = item['value']
                        notifications.remove(notif)
                        notifications.append(new_notification)

                break

        #------------------------------------------------------------------------------------
        # update the resource registry
        #------------------------------------------------------------------------------------

        log.debug("user.variables::: %s", user.variables)

        self.clients.resource_registry.update(user)

        return user


    def _get_subscriptions(self, resource_id='', include_nonactive=False):
        """
        This method is used to get the subscriptions to a data product. The method will return a list of NotificationRequest
        objects for whom the origin is set to this data product. This way all the users who were interested in listening to
        events with origin equal to this data product, will be known and all their subscriptions will be known.

        @param resource_id
        @param include_nonactive
        @return notification_requests []

        """

        search_origin = 'search "origin" is "%s" from "resources_index"' % resource_id
        ret_vals = self.discovery.parse(search_origin)

        log.debug("Using discovery with search_string: %s", search_origin)
        log.debug("_get_subscriptions() got ret_vals: %s", ret_vals )

        notifications_all = set()
        notifications_active = set()

        object_ids = []
        for item in ret_vals:
            if item['_type'] == 'NotificationRequest':
                object_ids.append(item['_id'])

        notifs = self.clients.resource_registry.read_mult(object_ids)

        log.debug("Got %s notifications here. But they include both active and past notifications", len(notifs))

        if include_nonactive:
            # Add active or retired notification
            notifications_all.update(notifs)
        else:
            for notif in notifs:
                log.debug("Got the end_datetime here: notif.temporal_bounds.end_datetime = %s", notif.temporal_bounds.end_datetime)
                if notif.temporal_bounds.end_datetime == '':
                    log.debug("removing the notification: %s", notif._id)
                    # Add the active notification
                    notifications_active.add(notif)

        if include_nonactive:
            return list(notifications_all)
        else:
            return list(notifications_active)

    def get_subscriptions(self, resource_id='', user_id = '', include_nonactive=False):
        """
        This method takes the user-id as an input parameter. The logic will first find all notification requests for this resource
        then if a user_id is present, it will filter on those that this user is associated with.
        """

        # Get the notifications whose origin field has the provided resource_id
        notifs = self._get_subscriptions(resource_id=resource_id, include_nonactive=include_nonactive)

        log.debug("For include_nonactive= %s, UNS fetched the following the notifications subscribed to the resource_id: %s --> %s. "
                      "They are %s in number", include_nonactive,resource_id, notifs, len(notifs))

        if not user_id:
            return notifs

        notifications = []

        # Now find the users who subscribed to the above notifications
        #todo Right now looking at assocs in a loop which is not efficient to find the users linked to these notifications
        # todo(contd) Need to use a more efficient way later
        for notif in notifs:
            notif_id = notif._id
            # Find if the user is associated with this notification request
            ids, _ = self.clients.resource_registry.find_subjects( subject_type = RT.UserInfo, object=notif_id, predicate=PRED.hasNotification, id_only=True)
            log.debug("Got the following users: %s, associated with the notification: %s", ids, notif_id)

            if ids and user_id in ids:
                notifications.append(notif)

        log.debug("For include_nonactive = %s, UNS fetched the following %s notifications subscribed to %s --> %s", include_nonactive,len(notifications),user_id, notifications)

        return notifications

    def get_subscriptions_attribute(self, resource_id='', user_id = '', include_nonactive=False):
        retval = self.get_subscriptions(resource_id=resource_id, user_id=user_id, include_nonactive=include_nonactive)
        container = ComputedListValue(value=retval)
        return container


#    def get_users_who_subscribed(self, resource_id='', include_nonactive=False):
#
#        # Get the notifications whose origin field has the provided resource_id
#        notifications = self.get_subscriptions(resource_id, include_nonactive)
#
#        # Now find the users who subscribed to the above notifications
#        #todo Right now looking at assocs in a loop which is not efficient to find the users linked to these notifications
#        # todo(contd) Need to use a more efficient way later
#
#        user_ids = set()
#        for notif in notifications:
#            notif_id = notif._id
#            # Find the users who are associated with this notification request
#            ids, _ = self.clients.resource_registry.find_subjects( subject_type = RT.UserInfo, object=notif_id, predicate=PRED.hasNotification, id_only=True)
#            user_ids.add(ids)
#
#        return user_ids

    def _notification_in_notifications(self, notification = None, notifications = None):

        for id, notif in notifications.iteritems():
            if notif.name == notification.name and \
            notif.origin == notification.origin and \
            notif.origin_type == notification.origin_type and \
            notif.event_type == notification.event_type:
                return id
        return None

    def _update_notification_in_notifications_dict(self, new_notification = None, notifications = None ):

        for id, notif in notifications.iteritems():
            if id == new_notification._id:
                notifications.pop(id)
                notifications[id] = new_notification
                break


    def load_user_info(self):
        '''
        Method to load the user info dictionary used by the notification workers and the UNS

        @retval user_info dict
        '''

        users, _ = self.clients.resource_registry.find_resources(restype= RT.UserInfo)

        user_info = {}

        if not users:
            return {}

        for user in users:
            notifications = []
            notification_preferences = None
            for variable in user.variables:
                if variable['name'] == 'notifications':
                    notifications = variable['value']

                if variable['name'] == 'notification_preferences':
                    notification_preferences = variable['value']

            user_info[user._id] = { 'user_contact' : user.contact, 'notifications' : notifications, 'notification_preferences' : notification_preferences}

        return user_info
