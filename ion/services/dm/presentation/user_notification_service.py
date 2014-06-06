#!/usr/bin/env python

"""Service to manage user requested notifications about events in the system"""

__author__ = 'Bill Bollenbacher, Swarbhanu Chatterjee, David Stuebe, Michael Meisinger'

from pyon.public import RT, PRED, get_sys_name, OT, IonObject, get_ion_ts, log, CFG, BadRequest, IonException, NotFound, Inconsistent, EventPublisher, EventSubscriber
from pyon.core.governance import ORG_MEMBER_ROLE, ORG_MANAGER_ROLE, INSTRUMENT_OPERATOR, DATA_OPERATOR, OBSERVATORY_OPERATOR, GovernanceHeaderValues, has_org_role

from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, convert_events_to_email_message, \
    get_event_computed_attributes, calculate_reverse_user_info
from ion.util.datastore.resources import ResourceRegistryUtil

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import BaseUserNotificationService
from interface.objects import ComputedValueAvailability, ComputedListValue
from interface.objects import ProcessDefinition, TemporalBounds

from ion.services.sa.observatory.observatory_util import ObservatoryUtil

from interface.objects import DeliveryModeEnum, NotificationFrequencyEnum, NotificationTypeEnum
from pyon.datastore.datastore import DataStore
from pyon.datastore.datastore_query import DatastoreQueryBuilder, DQ
from pyon.public import get_ion_ts_millis
from email.mime.text import MIMEText

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
    MAX_EVENT_QUERY_RESULTS = CFG.get_safe('service.user_notification.max_event_query_results', 100000)

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

        self.clients.resource_registry.update(notification)

#        raise NotImplementedError("This method needs to be worked out in terms of implementation")

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
        #-------------------------------------------------------------------------------------------------------------------
        # Generate an event that can be picked by notification workers so that they can update their user_info dictionary
        #-------------------------------------------------------------------------------------------------------------------
        log.info("(update notification) Publishing ReloadUserInfoEvent for updated notification")

        self.event_publisher.publish_event( event_type= "ReloadUserInfoEvent",
            origin="UserNotificationService",
            description= "A notification has been updated.",
            notification_id = notification._id
        )

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

        # set lcs to retired, not deleted.

        self.clients.resource_registry.retire(notification_id)

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
        #todo remove and use self.user_id_to_nr_map
        self.notifications = []
        user_notif_req_objs, _ = self.clients.resource_registry.find_objects(
            subject=user_info_id, predicate=PRED.hasNotification, object_type=RT.NotificationRequest, id_only=False)

        log.debug("Got %s notifications, for the user: %s", len(user_notif_req_objs), user_info_id)

        for notif in user_notif_req_objs:
            # do not include notifications that have expired
            if notif.temporal_bounds.end_datetime == '':
                    self.notifications.append(notif)

        return self.notifications


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
        outil_client = ObservatoryUtil(self)

        # map the user id to all NotificationRequests resouces for that user
        self.user_id_to_nr_map = {}

        if end_time <= start_time:
            return

        # flag to denote if the disabled_by_system flag was reset on any notification
        disabled_by_system_reset = False

        # retrieve all users in the system
        users, _ = self.clients.resource_registry.find_resources(restype=RT.UserInfo)
        #retrieve all the active notifications assoc to every user
        self.user_id_to_nr_map = self._get_all_user_notifications(user_objs=users)

        for user in users:

            # these are lists of all the origins and event types found in the set of Notification resources
            # used to query the events db
            self.origins = []
            self.event_types = []
            self.origin_types = []

            # the following maps help us quickly filer the set of retrieved events to see if they are of interest
            # this map has a tuple of (origin, event type) as the key and a list of NotificationRequest resources as the value
            self.origin_event_type_map = {}
            # this map has an origin id as the key and a list of NotificationRequest resources as the value
            self.origin_nr_map = {}
            #if no origin id
            #   this map has an origin type as the key and a list of NotificationRequest resources as the value
            self.origintype_nr_map = {}
            #   this map has an event type as the key and a list of NotificationRequest resources as the value
            self.eventtype_nr_map = {}
            # map user email and devlivery mode to a list of events
            self.email_mode_to_events_map = {}
            # map the event ids to the NRs that it is assoc with
            self.event_id_to_nr_map = {}


            # for each subscription created by this user
            if user._id not in self.user_id_to_nr_map:
                continue

            for notification in self.user_id_to_nr_map[user._id]:

                # reset the disabled_by_system flag if necessary
                if self._reset_disabled_by_system(notification):
                    disabled_by_system_reset = True

                #check that this NotificationRequest is active and also has at least one delivery_configuration that is batch delivery
                #todo are there still notification diasable switch at the UserInfo level?
                batch_mode = False
                for delivery_configuration in notification.delivery_configurations:
                    if delivery_configuration.frequency == NotificationFrequencyEnum.BATCH:
                        batch_mode = True

                if not batch_mode or notification.temporal_bounds.end_datetime:
                    continue

                #create a set of maps and lists for query
                self._build_reference_maps(notification.origin, notification)


                # if this is an aggregate notification then collect child ids and build map to search by origin if provided
                children_ids = []
                if notification.type != NotificationTypeEnum.SIMPLE and notification.origin:
                    children_ids = self._find_children_by_type( parent_id= notification.origin, type=notification.type, outil=outil_client)

                    #add all the children
                    for children_id in children_ids:
                        #augment the  set of maps and lists with the included child origins
                        self._build_reference_maps(children_id, notification)

            # if any up the notification resources have been updated then notify the workers to reload their cache
            if disabled_by_system_reset:
                self.event_publisher.publish_event( event_type= OT.ReloadUserInfoEvent,
                    origin="UserNotificationService",
                    description= "disabled_by_system reset in one or more notifications.")


            #log.debug('notification processing origin_event_type_map:   %s', self.origin_event_type_map)
            #log.debug('notification processing origin_nr_map:   %s', self.origin_nr_map)
            #log.debug('notification processing origintype_nr_map:   %s', self.origintype_nr_map)
            #
            #
            #log.debug('notification processing origins:   %s', self.origins)
            #log.debug('notification processing event_types:   %s', self.event_types)
            event_objects = []

            event_objects = self._get_user_batch_events(start_time=start_time, end_time=end_time,origins=self.origins, event_types=self.event_types, origin_types=self.origintype_nr_map.keys() )
            # filter out the events that have a correct origin/event_type pairing
            batch_events = []
            for event_object in event_objects:

                # determine if we are interested in this event using maps and assign to email map
                self._process_event(event_object)

            #log.debug('event processing  self.email_mode_to_events_map:   %s',  self.email_mode_to_events_map)

            # send a notification email to each user using a _send_email() method
            self._format_and_send_email(user_info=user, smtp_client=self.smtp_client)

        self.smtp_client.quit()


    def _reset_disabled_by_system(self, notification=None):
        if notification and notification.disabled_by_system is True:
            notification.disabled_by_system = False
            self.clients.resource_registry.update(notification)
            return True
        else:
            return False



    def _build_reference_maps(self, origin_id = '', notification=None):

        #add event type to list of event types to include in the events db query
        if notification.event_type:
            #add to the list of event types to pull back in eventdb query
            if not notification.event_type in self.event_types and notification.event_type != '*':
                self.event_types.append(notification.event_type)


        # the most common subscription is an origin and an event type so create a map that has that pair as a tuple
        if origin_id and origin_id != '*':

            # get the union of origins and event types to build the query
            if not origin_id in self.origins :
                self.origins.append(origin_id)

            if notification.event_type and notification.event_type != '*':
                key_tuple = (origin_id, notification.event_type)
                # create a map that pairs origins to the corresponding event types
                if not key_tuple in self.origin_event_type_map:
                    self.origin_event_type_map[key_tuple] = [notification]
                else:
                    self.origin_event_type_map[key_tuple].append(notification)

            if not notification.event_type or notification.event_type == '*':
                if origin_id in self.origin_nr_map:
                    self.origin_nr_map[origin_id].append(notification)
                else:
                    self.origin_nr_map[origin_id] = [notification]


        # if there is no origin (or wildcard) but origin_type is specified then add to origin type map
        if (not origin_id or origin_id == '*') and notification.origin_type and notification.origin_type != '*':
            # add to list of origin types in query
            if not notification.origin_type in self.origin_types:
                self.origin_types.append(notification.origin_type)
            if notification.origin_type in self.origintype_nr_map:
                self.origintype_nr_map[notification.origin_type].append(notification)
            else:
                self.origintype_nr_map[notification.origin_type] = [notification]

        # if there is no origin (or wildcard) but event_type is specified then add to event type map
        if (not origin_id or origin_id == '*') and notification.event_type and notification.event_type != '*':
            if notification.event_type in self.eventtype_nr_map:
                self.eventtype_nr_map[notification.event_type].append(notification)
            else:
                self.eventtype_nr_map[notification.event_type] = [notification]

    def _get_all_user_notifications(self, user_objs=None):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_info_id str

        @retval notifications list of NotificationRequest objects
        """

        if not user_objs:
            return {}

        user_id_list = []
        user_id_list = [ user_obj._id for user_obj in user_objs ]
        log.debug('_get_all_user_notifications user_id_list: %s', user_id_list)

        user_id_to_nrs_map = {}
        # now look for hasProcess associations to determine which Processes are TransformWorkers
        objects, associations = self.clients.resource_registry.find_objects_mult(subjects=user_id_list, id_only=False)
        for object, assoc in zip(objects, associations):
            if assoc.p == PRED.hasNotification:
                if assoc.s in user_id_to_nrs_map:
                    user_id_to_nrs_map[assoc.s].append(object)
                else:
                    user_id_to_nrs_map[assoc.s] = [object]

        #log.debug('_get_all_user_notifications user_id_to_nrs_map: %s', user_id_to_nrs_map)

        #for notif in user_notif_req_objs:
        #    # do not include notifications that have expired
        #    #todo and method is batch
        #    if notif.temporal_bounds.end_datetime == '':
        #            notifications.append(notif)

        return user_id_to_nrs_map


    def _get_user_batch_events(self, start_time='', end_time='', origins=None, event_types=None, origin_types=None ):
        """
        Retrieve all events that match the origins and event_types

        see example: https://gist.github.com/mmeisinger/9692807

        @param origins  list of all origins
        @param event_types  list of all event types

        @retval notifications list of NotificationRequest objects
        """
        dqb = DatastoreQueryBuilder(datastore=DataStore.DS_EVENTS, profile=DataStore.DS_PROFILE.EVENTS)

        filter_origins = dqb.in_(DQ.EA_ORIGIN, *origins)
        filter_types = dqb.in_(DQ.ATT_TYPE, *event_types)
        filter_origin_types = dqb.in_(DQ.EA_ORIGIN_TYPE, *origin_types)
        filter_mindate = dqb.gte(DQ.RA_TS_CREATED, start_time)

        where_list = []
        nr_condition_and = [filter_mindate]
        if origins : nr_condition_and.append(filter_origins)
        if event_types : nr_condition_and.append(filter_types)
        if origin_types : nr_condition_and.append(filter_origin_types)

        if nr_condition_and: where_list.append(dqb.and_(* nr_condition_and))
        where = dqb.or_(*where_list)

        order_by = dqb.order_by([["ts_created", "desc"]])  # Descending order by time
        dqb.build_query(where=where, order_by=order_by, limit=self.MAX_EVENT_QUERY_RESULTS, skip=0, id_only=False)
        query = dqb.get_query()

        event_objs = self.container.event_repository.event_store.find_by_query(query)

        #log.debug('_get_user_batch_events event_objs:  %s ', event_objs)


        return event_objs



    def _process_event(self, event_object=None):

        key_tuple = (event_object.origin, event_object.type_)

        # first check if this is a 'normal' origin and event type subscription
        if key_tuple in self.origin_event_type_map:
            self._define_delivery_configurations_for_event(event_object=event_object, notification_list=self.origin_event_type_map[key_tuple])
            self._add_to_event_nr_map(event_object._id, self.origin_event_type_map[key_tuple])

        # handle the origins map for origin ids that did not have a event type
        if event_object.origin in self.origin_nr_map:
            self._define_delivery_configurations_for_event(event_object=event_object, notification_list=self.origin_nr_map[event_object.origin])
            self._add_to_event_nr_map(event_object._id, self.origin_nr_map[event_object.origin])

        # next look in the list of NRs that did not specify an origin id but did specify an origin_type
        if event_object.origin_type in self.origintype_nr_map:
            # loop the list of NotificationRequests
            matches = []
            for note_req in self.origintype_nr_map[event_object.origin_type]:
                # check that the event_type spec on this NR matches what is in the event
                if not note_req.event_type or note_req.event_type == '*' or note_req.event_type == event_object.type_:
                    matches.append(note_req)
            self._define_delivery_configurations_for_event(event_object=event_object, notification_list=matches)
            self._add_to_event_nr_map(event_object._id, matches)

        # next look in the list of NRs that did not specify an origin id but did specify an event_type
        if event_object.type_ in self.eventtype_nr_map:
            # loop the list of NotificationRequests
            matches = []
            for note_req in self.eventtype_nr_map[event_object.type_]:
                # check that the origin_type spec on this NR matches (or does not conflict) with what is in the event
                if not note_req.origin_type or note_req.origin_type == '*' or note_req.origin_type == event_object.origin_type:
                    matches.append(note_req)
            self._define_delivery_configurations_for_event(event_object=event_object, notification_list=matches)
            self._add_to_event_nr_map(event_object._id, matches)

        return


    def _add_to_event_nr_map(self, event_id='', nr_obj_list=None):

        if not event_id or not nr_obj_list:
            return

        if event_id in self.event_id_to_nr_map:
            self.event_id_to_nr_map[event_id].extend(nr_obj_list)
        else:
            self.event_id_to_nr_map[event_id]= nr_obj_list




    def _define_delivery_configurations_for_event(self, event_object=None, notification_list=None):

        # check the delivery confifguration in each NR object and assign the event to an email/mode pair
        for notification in notification_list:
            #loop thru the delivery mode objects and build map of delivery addresses and modes
            for delivery_configuration in notification.delivery_configurations:
                if delivery_configuration.frequency == NotificationFrequencyEnum.BATCH:
                    email = delivery_configuration.email if delivery_configuration.email else 'default'
                    key_tuple = ( email, delivery_configuration.mode )
                    if not key_tuple in self.email_mode_to_events_map:
                        self.email_mode_to_events_map[key_tuple] = [event_object]
                    else:
                        self.email_mode_to_events_map[key_tuple].append(event_object)

        log.debug('_define_delivery_configurations_for_event  self.email_mode_to_events_map:  %s', self.email_mode_to_events_map)

        return


    def _format_and_send_email(self, user_info=None, smtp_client=None):
        """
        Format the message for a particular user containing information about the events he is to be notified about

        """

        for (user_email, batch_type), events in self.email_mode_to_events_map.iteritems():

            events = self._remove_duplicate_events(events)

            email = user_email if user_email != 'default' else user_info.contact.email
            msg = {}
            if batch_type == DeliveryModeEnum.EMAIL:
                message = str(events)
                log.debug("The user, %s, will get the following events in his batch notification email: %s", user_info.name, message)

                msg = convert_events_to_email_message(events=events, notifications_map=self.event_id_to_nr_map, rr_client=self.clients.resource_registry)
                #msg["Subject"] = "(SysName: " + get_sys_name() + ") ION event "
                msg["To"] = email
                self._send_batch_email(msg, smtp_client)

            else:
                log.warning("Invalid delivery mode for email: %s" % user_email)




    def _send_batch_email(self, msg=None, smtp_client=None):
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
        log.debug('_format_and_send_email msg_recipient %s     msg  %s', msg_recipient, msg.as_string())
        smtp_client.sendmail(smtp_sender, [msg_recipient], msg.as_string())


    def _find_children_by_type(self, parent_id= '', type='', outil=None):

        child_ids = []

        if type == NotificationTypeEnum.PLATFORM:
            device_relations = outil.get_child_devices(parent_id)
            child_ids = [did for pt,did,dt in device_relations[ parent_id] ]
        elif type == NotificationTypeEnum.SITE:
            child_site_dict, ancestors = outil.get_child_sites(parent_id)
            child_ids = child_site_dict.keys()

        elif  type == NotificationTypeEnum.FACILITY:
            resource_objs, _ = self.clients.resource_registry.find_objects(
                subject=parent_id, predicate=PRED.hasResource, id_only=False)
            for resource_obj in resource_objs:
                if resource_obj.type_ == RT.DataProduct \
                    or resource_obj.type_ == RT.InstrumentSite or resource_obj.type_ == RT.InstrumentDevice \
                    or resource_obj.type_ == RT.PlatformSite or resource_obj.type_ == RT.PlatformDevice:
                    child_ids.append(resource_obj._id)
        if parent_id in child_ids:
            child_ids.remove(parent_id)
        log.debug('_find_children_by_type  child_ids:  %s', child_ids)
        return child_ids


    def _remove_duplicate_events(self, events=None):

        #remove duplicate events using a set but preserve order
        seen = set()
        non_dup_events = []
        for event in events:
            if event._id not in seen:
                seen.add(event._id)
                non_dup_events.append(event)
        return non_dup_events

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

        rr_util = ResourceRegistryUtil(self.container)

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
