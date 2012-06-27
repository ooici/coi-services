

from interface.services.dm.idiscovery_service import DiscoveryServiceClient


def send_email(self, sender, receiver, message):
    '''
    A common method to send email with formatting
    '''

    pass

def update_user_info():
    '''
    Method to update the user info dictionary... used by notification workers and the UNS
    '''
    search_string = 'search "name" is "*" from "users_index"'
    discovery = DiscoveryServiceClient()
    results  = discovery.parse(search_string)

    for result in results:
        user_name = result['_source'].name
        user_contact = result['_source'].contact
        notifications = result['_source'].variables.values()

        user_info[user_name] = { 'user_contact' : user_contact, 'notifications' : notifications}

    return user_info

def calculate_reverse_user_info(user_info = {}):
    '''
    Calculate a reverse user info... used by the notification workers and the UNS
    '''

    event_type_user = {}
    event_subtype_user = {}
    event_origin_user = {}
    event_origin_type_user = {}

    for key, value in user_info.iteritems:

        notifications = value['notifications']

        for notification in notifications:
            if event_type_user[notification.event_type]:
                event_type_user[notification.event_type].append(key)
            else:
                event_type_user[notification.event_type] = [key]

            if event_subtype_user[notification.event_subtype]:
                event_subtype_user[notification.event_subtype].append(key)
            else:
                event_subtype_user[notification.event_subtype] = [key]

            if event_origin_user[notification.origin]:
                event_origin_user[notification.origin].append(key)
            else:
                event_origin_user[notification.origin] = [key]

            if event_origin_type_user[notification.origin_type]:
                event_origin_type_user[notification.origin_type].append(key)
            else:
                event_origin_type_user[notification.origin_type] = [key]

    return event_type_user, event_subtype_user, event_origin_user, event_origin_type_user