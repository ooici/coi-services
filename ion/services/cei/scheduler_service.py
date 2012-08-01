#!/usr/bin/env python

__author__ = 'Seman, Michael Meisinger'
__license__ = 'Apache 2.0'


from pyon.public import log, IonObject
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher
from interface.services.cei.ischeduler_service import BaseSchedulerService
from interface.objects import IntervalTimer, TimeOfDayTimer, TimerSchedulerEntry
from datetime import datetime, timedelta
from math import ceil
import time
import gevent
import uuid


class SchedulerService(BaseSchedulerService):
    schedule_entries = {}

    def __notify(self, task, id, index):
        log.debug("SchedulerService:__notify: - " + task.event_origin + " - Time: " + str(self.__now()) + " - ID: " + id + " -Index:" + str(index))
        pub = EventPublisher(event_type="ResourceEvent")
        pub.publish_event(origin=task.event_origin)

    def __now(self):
        return datetime.utcnow()

    def __now_posix(self, now):
        return time.mktime(now.timetuple())

    def _expire_callback(self, id, index):
        task = self.__get_entry(id)
        self.__notify(task, id, index)
        if not self.__reschedule(id, index):
            self.__delete(id, index)

    def __get_expire_time(self, task):
        now = self.__now()
        expires_in = []
        if type(task) == TimeOfDayTimer:
            for time_of_day in task.times_of_day:
                expire_time = datetime(now.year, now.month, now.day, time_of_day['hour'], time_of_day['minute'], time_of_day['second'])
                expires_in.append(ceil((expire_time - now).total_seconds()))
        elif type(task) == IntervalTimer and (task.number_of_intervals > 0 or task.number_of_intervals == -1):
            now_posix = self.__now_posix(now)
            if task.start_time < now_posix:
                expires_in = [ceil(task.interval)]
                task.number_of_intervals -= 1
            else:
                expires_in = [ceil((task.start_time - now_posix) + task.interval)]
                task.number_of_intervals -= 1

        return expires_in

    def __get_reschedule_expire_time(self, task, index):
        expires_in = False
        if type(task) == TimeOfDayTimer:
            now = self.__now()
            now_posix = self.__now_posix(now)
            if task.expires > now_posix:
                time_of_day = task.times_of_day[index]
                tomorrow = now + timedelta(days=1)
                expire_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, time_of_day['hour'], time_of_day['minute'], time_of_day['second'])
                expires_in = (ceil((expire_time - now).total_seconds()))
            else:
                expires_in = False
        elif type(task) == IntervalTimer and (task.number_of_intervals > 0 or task.number_of_intervals == -1):
            now_posix = self.__now_posix(self.__now())
            if task.start_time < now_posix:
                expires_in = ceil(task.interval)
                task.number_of_intervals -= 1
            else:
                expires_in = ceil((task.start_time - now_posix) + task.interval)
                task.number_of_intervals -= 1
        return expires_in

    def __schedule(self, task):
        id = str(uuid.uuid4())
        spawns = []
        expire_times = self.__get_expire_time(task)
        for index, expire_time in enumerate(expire_times):
            if expire_time > 0:
                log.debug("SchedulerService:__schedule: scheduling: - " + task.event_origin + " - Now: " + str(self.__now()) +
                          " - Expire: " + str(expire_time) + " - ID: " + id + " - Index:" + str(index))
                spawn = gevent.spawn_later(expire_time, self._expire_callback, id, index)
                spawns.append(spawn)
            else:
                log.error("SchedulerService:__schedule: scheduling: expire time is less than zero: " + str(expire_time))
                return False
        self.__create_entry(task, spawns, id)
        return id

    def __reschedule(self, id, index):
        task = self.__get_entry(id)
        if type(task) == IntervalTimer:
            if (task.number_of_intervals > 0 or task.number_of_intervals == -1):
                expire_time = self.__get_reschedule_expire_time(task, index)
                log.debug("SchedulerService:__reschedule: rescheduling: - " + task.event_origin + " - Now: " + str(self.__now()) +
                          " - Expire: " + str(expire_time) + " - ID: " + id + " -Index:" + str(index))
                spawn = gevent.spawn_later(expire_time, self._expire_callback, id, index)
                self.__update_entry(id=id, index=index, spawn=spawn)
                return True
        elif type(task) == TimeOfDayTimer:
            if task.expires > self.__now_posix(self.__now()):
                expire_time = self.__get_reschedule_expire_time(task, index)
                if expire_time and expire_time >= 1:
                    log.debug("SchedulerService:__reschedule: rescheduling: - " + task.event_origin + " - Now: " + str(self.__now()) +
                              " - Expire: " + str(expire_time) + " - ID: " + id + " -Index:" + str(index))
                    spawn = gevent.spawn_later(expire_time, self._expire_callback, id, index)
                    self.__update_entry(id=id, index=index, spawn=spawn)
                    return True
        return False

    def __create_entry(self, task, spawns, id):
        self.schedule_entries[id] = {"task": task, "spawns": spawns}

    def __update_entry(self, id, index, spawn=None, interval=None):
        if spawn is not None:
            self.schedule_entries[id]["spawns"][index] = spawn
        if interval is not None:
            self.schedule_entries[id]["task"].interval = interval

    def __get_entry_all(self, id):
        return self.schedule_entries[id]

    def __get_spawns(self, id):
        return self.schedule_entries[id]["spawns"]

    def __get_entry(self, id):
        return self.schedule_entries[id]["task"]

    def __delete(self, id, index, force=False):
        if id in self.schedule_entries:
            task = self.__get_entry(id)
            if force and type(task) == TimeOfDayTimer:
                log.debug("SchedulerService:__delete: entry deleted " + id + " -Index:" + str(index))
                del self.schedule_entries[id]
            elif type(task) == TimeOfDayTimer:
                task = self.__get_entry(id)
                task.times_of_day[index] = None
                # Delete if all the timers are set to none
                are_all_timers_expired = True
                for time_of_day in task.times_of_day:
                    if time_of_day is not None:
                        are_all_timers_expired = False
                        break
                if are_all_timers_expired:
                    log.debug("SchedulerService:__delete: entry deleted " + id + " -Index:" + str(index))
                    del self.schedule_entries[id]
            else:
                log.debug("SchedulerService:__delete: entry deleted " + id + " -Index:" + str(index))
                del self.schedule_entries[id]
            return True
        return False

    def __is_timer_valid(self, task):
        # Validate event_origin is set
        if not task.event_origin:
            log.error("SchedulerService.__is_timer_valid: event_origin is not set")
            return False
        # Validate the timer is set correctly
        if type(task) == IntervalTimer:
            if task.number_of_intervals < 0 or task.interval < 1:
                log.error("SchedulerService.__is_timer_valid: IntervalTimer is set to incorrect value")
                return False
        elif type(task) == TimeOfDayTimer:
            for time_of_day in task.times_of_day:
                time_of_day['hour'] = int(time_of_day['hour'])
                time_of_day['minute'] = int(time_of_day['minute'])
                time_of_day['second'] = int(time_of_day['second'])
                if ((time_of_day['hour'] < 0 or time_of_day['hour'] > 23) or
                    (time_of_day['minute'] < 0 or time_of_day['minute'] > 59) or
                    (time_of_day['second'] < 0 or time_of_day['second'] > 61)):
                    log.error("SchedulerService.__is_timer_valid: TimeOfDayTimer is set to incorrect value")
                    return False
        else:
            return False

        return True

    def create_timer(self, scheduler_entry=None):
        """
        Create a timer which will send TimerEvents as requested for a given schedule.
        The schedule request is expressed through a specific subtype of TimerSchedulerEntry.
        The task is delivered as a TimeEvent to which processes can subscribe. The creator
        defines the fields of the task. A GUID-based id prefixed by readable process name
        is recommended for the origin. Because the delivery of the task is via the ION Exchange
        there is potential for a small deviation in precision.
        Returns a timer_id which can be used to cancel the timer.

        @param timer__schedule    TimerSchedulerEntry
        @retval timer_id    str
        @throws BadRequest    if timer is misformed and can not be scheduled
        """
        scheduler_entry = scheduler_entry.entry
        status = self.__is_timer_valid(scheduler_entry)
        if not status:
            raise BadRequest
        id = self.__schedule(scheduler_entry)
        if not id:
            raise BadRequest
        return id

    def cancel_timer(self, timer_id=''):
        """
        Cancels an existing timer which has not reached its expire time.

        @param timer_id    str
        @throws NotFound    if timer_id doesn't exist
        """
        #try:
        try:
            spawns = self.__get_spawns(timer_id)
            for spawn in spawns:
                spawn.kill()
            log.debug("SchedulerService: cancel_timer: id: " + str(timer_id))
            self.__delete(id=timer_id, index=None, force=True)
        except:
            log.error("SchedulerService: cancel_timer: timer id doesn't exist: " + str(timer_id))
            raise BadRequest

    def create_interval_timer(self, start_time, interval, number_of_intervals, event_origin, event_subtype=""):
        if number_of_intervals < 0 or interval < 1 or not event_origin:
            log.error("SchedulerService.create_interval_timer: event_origin is not set")
            raise BadRequest
        return IonObject("IntervalTimer", {"start_time": start_time, "interval": interval, "number_of_intervals": number_of_intervals,
                                           "event_origin": event_origin, "event_subtype": event_subtype})

    def create_time_of_day_timer(self, times_of_day, expires, event_origin, event_subtype=""):
        # Validate the timer
        if not event_origin:
            log.error("SchedulerService.create_time_of_day_timer: event_origin is set to invalid value")
            raise BadRequest
        for time_of_day in times_of_day:
            time_of_day['hour'] = int(time_of_day['hour'])
            time_of_day['minute'] = int(time_of_day['minute'])
            time_of_day['second'] = int(time_of_day['second'])
            if ((time_of_day['hour'] < 0 or time_of_day['hour'] > 23) or
                (time_of_day['minute'] < 0 or time_of_day['minute'] > 59) or
                (time_of_day['second'] < 0 or time_of_day['second'] > 61)):
                log.error("SchedulerService.create_time_of_day_timer: TimeOfDayTimer is set to invalid value")
                raise BadRequest

        return IonObject("TimeOfDayTimer", {"times_of_day": times_of_day, "expires": expires,
                                           "event_origin": event_origin, "event_subtype": event_subtype})
