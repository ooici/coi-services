#!/usr/bin/env python

__author__ = 'Seman, Michael Meisinger'
__license__ = 'Apache 2.0'

from pyon.public import IonObject, RT, log
from pyon.core.exception import BadRequest
from pyon.event.event import EventPublisher
from pyon.core.bootstrap import CFG
from interface.services.cei.ischeduler_service import BaseSchedulerService
from interface.objects import IntervalTimer, TimeOfDayTimer

from datetime import datetime, timedelta
from math import ceil
import time
import gevent


class SchedulerService(BaseSchedulerService):

    def __init__(self, *args, **kwargs):
        BaseSchedulerService.__init__(self, *args, **kwargs)

        self.schedule_entries = {}
        self._no_reschedule = False

    def on_start(self):
        if CFG.get_safe("process.start_mode") == "RESTART":
            self.on_system_restart()
        self.pub = EventPublisher(event_type="TimerEvent")

    def on_quit(self):
        self.pub.close()

        # throw killswitch on future reschedules
        self._no_reschedule = True

        # terminate any pending spawns
        self._stop_pending_timers()

    def _notify(self, task, id_, index):
        log.debug("SchedulerService:_notify: - " + task.event_origin + " - Time: " + str(self._now()) + " - id_: " + id_ + " -Index:" + str(index))
        self.pub.publish_event(origin=task.event_origin)

    def _now(self):
        return datetime.utcnow()

    def _convert_to_posix_time(self, t):
        return time.mktime(t.timetuple())

    def _expire_callback(self, id_, index):
        task = self.__get_entry(id_)
        self._notify(task, id_, index)
        if not self.__reschedule(id_, index):
            self.__delete(id_, index)

    def __calculate_next_interval(self, task, current_time):
        if task.start_time < current_time:
            next_interval = task.start_time
            while next_interval < current_time:
                next_interval = next_interval + task.interval
            return next_interval - current_time
        else:
            return (task.start_time - current_time) + task.interval

    def __get_expire_time(self, task):
        now = self._now()
        now_posix = self._convert_to_posix_time(now)
        expires_in = []
        if type(task) == TimeOfDayTimer:
            for time_of_day in task.times_of_day:
                expire_time_input = datetime(year=now.year, month=now.month, day=now.day, hour=time_of_day['hour'], minute=time_of_day['minute'], second=time_of_day['second'])
                # Find out if the time is already passed for today. If it is schedule it for tomorrow
                if expire_time_input.time() > now.time():
                    expire_time = datetime(year=now.year, month=now.month, day=now.day, hour=time_of_day['hour'], minute=time_of_day['minute'], second=time_of_day['second'])
                else:
                    tomorrow_posix = self._convert_to_posix_time(now + timedelta(days=1))
                    tomorrow = now + timedelta(days=1)
                    if task.expires < tomorrow_posix:
                        # This occurs in the following case:
                        # Current time is 8am. Timer setup is 6am. Expire time is tomorrow at 5am
                        # Example current time is 8:00AM. User setups a timer for 6:00AM. Since it is 8am, it tries to
                        #   setup timer for tomorrow  6am but the expire time is set at 5AM tomorrow
                        raise BadRequest("SchedulerService: couldn't setup a timer. Please check the timer and the expiration time")
                    expire_time = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=time_of_day['hour'], minute=time_of_day['minute'], second=time_of_day['second'])

                expires_in.append(ceil((expire_time - now).total_seconds()))
        elif type(task) == IntervalTimer and (task.end_time == -1 or ((now_posix + task.interval) <= task.end_time)):
            expires_in = [(self.__calculate_next_interval(task, now_posix))]
        return expires_in

    def __get_reschedule_expire_time(self, task, index):
        expires_in = False
        now = self._now()
        now_posix = self._convert_to_posix_time(now)
        if type(task) == TimeOfDayTimer:
            tomorrow = now + timedelta(days=1)
            tomorrow_posix = self._convert_to_posix_time(tomorrow)
            if task.expires > tomorrow_posix:
                time_of_day = task.times_of_day[index]
                expire_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, time_of_day['hour'], time_of_day['minute'], time_of_day['second'])
                expires_in = (ceil((expire_time - now).total_seconds()))
            else:
                expires_in = False
        elif type(task) == IntervalTimer and (task.end_time == -1 or ((now_posix + task.interval) <= task.end_time)):
            if task.start_time <= now_posix:
                expires_in = (task.interval)
            else:
                expires_in = ((task.start_time - now_posix) + task.interval)

        return expires_in

    def __validate_expire_times(self, expire_times):
        for index, expire_time in enumerate(expire_times):
            if expire_time < 0:
                return False
        return True

    def __schedule(self, scheduler_entry, id_=False):
        # if "id_" is set, it means scheduler_entry is already in Resource Registry. This can occur during a system restart
        spawns = []
        task = scheduler_entry.entry
        expire_times = self.__get_expire_time(task)
        if not self.__validate_expire_times(expire_times):
            log.error("SchedulerService:__schedule: scheduling: expire time is less than zero: ")
            return False

        if not id_:
            id_, _ = self.clients.resource_registry.create(scheduler_entry)
        self.__create_entry(task, spawns, id_)
        for index, expire_time in enumerate(expire_times):
            log.debug("SchedulerService:__schedule: scheduling: - " + task.event_origin + " - Now: " + str(self._now()) +
                      " - Expire: " + str(expire_time) + " - ID: " + id_ + " - Index:" + str(index))
            spawn = gevent.spawn_later(expire_time, self._expire_callback, id_, index)
            spawns.append(spawn)
        return id_

    def __reschedule(self, id_, index):
        if self._no_reschedule:
            log.debug("SchedulerService:__reschedule: process quitting, refusing to reschedule %s", id_)
            return False

        task = self.__get_entry(id_)
        expire_time = self.__get_reschedule_expire_time(task, index)
        if expire_time:
            log.debug("SchedulerService:__reschedule: rescheduling: - " + task.event_origin + " - Now: " + str(self._now()) +
                      " - Expire: " + str(expire_time) + " - ID: " + id_ + " -Index:" + str(index))
            spawn = gevent.spawn_later(expire_time, self._expire_callback, id_, index)
            self.__update_entry(id_=id_, index=index, spawn=spawn)

            return True
        else:
            log.debug("SchedulerService:__reschedule: timer expired. Removed from RR  : - " + task.event_origin + " - Now: " + str(self._now()) +
                      " - Expire: " + str(expire_time) + " - ID: " + id_ + " -Index:" + str(index))
        return False

    def __create_entry(self, task, spawns, id_):
        self.schedule_entries[id_] = {"task": task, "spawns": spawns}

    def __update_entry(self, id_, index, spawn=None, interval=None):
        if spawn is not None:
            self.schedule_entries[id_]["spawns"][index] = spawn
        if interval is not None:
            self.schedule_entries[id_]["task"].interval = interval

    def __get_entry_all(self, id_):
        return self.schedule_entries[id_]

    def __get_spawns(self, id_):
        return self.schedule_entries[id_]["spawns"]

    def __get_entry(self, id_):
        return self.schedule_entries[id_]["task"]

    def __delete(self, id_, index, force=False):
        if id_ in self.schedule_entries:
            task = self.__get_entry(id_)
            if force and type(task) == TimeOfDayTimer:
                log.debug("SchedulerService:__delete: entry deleted " + id_ + " -Index:" + str(index))
                del self.schedule_entries[id_]
                self.clients.resource_registry.delete(id_)
            elif type(task) == TimeOfDayTimer:
                task = self.__get_entry(id_)
                task.times_of_day[index] = None
                # Delete if all the timers are set to none
                are_all_timers_expired = True
                for time_of_day in task.times_of_day:
                    if time_of_day is not None:
                        are_all_timers_expired = False
                        break
                if are_all_timers_expired:
                    log.debug("SchedulerService:__delete: entry deleted " + id_ + " -Index:" + str(index))
                    del self.schedule_entries[id_]
                    self.clients.resource_registry.delete(id_)
            else:
                log.debug("SchedulerService:__delete: entry deleted " + id_ + " -Index:" + str(index))
                del self.schedule_entries[id_]
                self.clients.resource_registry.delete(id_)
            return True
        return False

    def __is_timer_valid(self, task):
        # Validate event_origin is set
        if not task.event_origin:
            log.error("SchedulerService.__is_timer_valid: event_origin is not set")
            return False
            # Validate the timer is set correctly
        if type(task) == IntervalTimer:
            if task.end_time != -1 and (self._convert_to_posix_time(self._now()) >= task.end_time):
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

    def _stop_pending_timers(self):
        """
        Safely stops all pending and active timers.

        For all timers still waiting to run, calls kill on them. For active timers, let
        them exit naturally and prevent the reschedule by setting the _no_reschedule flag.
        """
        # prevent reschedules
        self._no_reschedule = True

        gls = []
        for timer_id in self.schedule_entries:
            spawns = self.__get_spawns(timer_id)

            for spawn in spawns:
                gls.append(spawn)
                # only kill spawns that haven't started yet
                if spawn._start_event is not None:
                    spawn.kill()

            log.debug("_stop_pending_timers: timer %s deleted", timer_id)

        self.schedule_entries.clear()

        # wait for running gls to finish up
        gevent.joinall(gls, timeout=10)

        # allow reschedules from here on out
        self._no_reschedule = False

    def on_system_restart(self):
        '''
        On system restart, get timer data from Resource Registry and restore the Scheduler state
        '''
        # Remove all active timers
        # When this method is called, there should not be any active timers but if it is called from test, this helps
        # to remove current active timer and restore them from Resource Regstiry
        self._stop_pending_timers()

        # Restore the timer from Resource Registry
        scheduler_entries, _ = self.clients.resource_registry.find_resources(RT.SchedulerEntry, id_only=False)
        for scheduler_entry in scheduler_entries:
            self.__schedule(scheduler_entry, scheduler_entry._id)
            log.debug("SchedulerService:on_system_restart: timer restored: " + scheduler_entry._id)

    def create_timer(self, scheduler_entry=None):
        """
        Create a timer which will send TimerEvents as requested for a given schedule.
        The schedule request is expressed through a specific subtype of TimerSchedulerEntry.
        The task is delivered as a TimeEvent to which processes can subscribe. The creator
        defines the fields of the task. A GUID-based id_ prefixed by readable process name
        is recommended for the origin. Because the delivery of the task is via the ION Exchange
        there is potential for a small deviation in precision.
        Returns a timer_id which can be used to cancel the timer.

        @param scheduler_entry    TimerSchedulerEntry
        @retval timer_id    str
        @throws BadRequest    if timer is misformed and can not be scheduled
        """
        ##scheduler_entry = scheduler_entry.entry
        status = self.__is_timer_valid(scheduler_entry.entry)
        if not status:
            raise BadRequest
        id_ = self.__schedule(scheduler_entry)
        if not id_:
            raise BadRequest
        return id_

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
            log.debug("SchedulerService: cancel_timer: id_: " + str(timer_id))
            self.__delete(id_=timer_id, index=None, force=True)
        except:
            log.error("SchedulerService: cancel_timer: timer id_ doesn't exist: " + str(timer_id))
            raise BadRequest

    def create_interval_timer(self, start_time="", interval=0, end_time="", event_origin="", event_subtype=""):
        if (end_time != -1 and (self._convert_to_posix_time(self._now()) >= end_time)) or not event_origin:
            log.error("SchedulerService.create_interval_timer: event_origin is not set")
            raise BadRequest
        if start_time == "now":
            start_time = self._convert_to_posix_time(self._now())
        log.debug("SchedulerService:create_interval_timer start_time: %s interval: %s end_time: %s event_origin: %s" %(start_time, interval, end_time, event_origin))
        interval_timer = IonObject("IntervalTimer", {"start_time": start_time, "interval": interval, "end_time": end_time,
                                                     "event_origin": event_origin, "event_subtype": event_subtype})
        se = IonObject(RT.SchedulerEntry, {"entry": interval_timer})
        return self.create_timer(se)

    def create_time_of_day_timer(self, times_of_day=None, expires='', event_origin='', event_subtype=''):
        # Validate the timer
        if not event_origin:
            log.error("SchedulerService.create_time_of_day_timer: event_origin is set to invalid value")
            raise BadRequest
        for time_of_day in times_of_day:
            time_of_day['hour'] = int(time_of_day['hour'])
            time_of_day['minute'] = int(time_of_day['minute'])
            time_of_day['second'] = int(time_of_day['second'])
            log.debug("SchedulerService:create_time_of_day_timer - hour: %d minute: %d second: %d expires: %d event_origin: %s" %(time_of_day['hour'] , time_of_day['minute'] , time_of_day['second'], time_of_day['second'], event_origin))
            if ((time_of_day['hour'] < 0 or time_of_day['hour'] > 23) or
                (time_of_day['minute'] < 0 or time_of_day['minute'] > 59) or
                (time_of_day['second'] < 0 or time_of_day['second'] > 61)):
                log.error("SchedulerService:create_time_of_day_timer: TimeOfDayTimer is set to invalid value")
                raise BadRequest

        time_of_day_timer = IonObject("TimeOfDayTimer", {"times_of_day": times_of_day, "expires": expires,
                                                         "event_origin": event_origin, "event_subtype": event_subtype})

        se = IonObject(RT.SchedulerEntry, {"entry": time_of_day_timer})
        return self.create_timer(se)
