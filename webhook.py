#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import requests
from requests_futures.sessions import FuturesSession
import threading
from utils import get_args, get_queues
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from cachetools import LFUCache
from timeit import default_timer
from queue import Empty

log = logging.getLogger(__name__)

# How low do we want the queue size to stay?
wh_warning_threshold = 100
# How long can it be over the threshold, in seconds?
# Default: 5 seconds per 100 in threshold.
wh_threshold_lifetime = int(5 * (wh_warning_threshold / 100.0))
wh_lock = threading.Lock()

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()


def send_to_webhooks(args, session, message_frame):

    if not args.webhooks:
        # What are you even doing here...
        log.warning('Called send_to_webhook() without webhooks.')
        return

    req_timeout = args.wh_timeout

    for w in args.webhooks:
        try:
            session.post(w, json=message_frame, timeout=(None, req_timeout),
                         background_callback=__wh_completed)
        except requests.exceptions.ReadTimeout:
            log.exception('Response timeout on webhook endpoint %s.', w)
        except requests.exceptions.RequestException as e:
            log.exception(repr(e))


def wh_updater():

    max_queue_size = 0
    wh_threshold_timer = default_timer()
    wh_over_threshold = False
    # WH updates queue & WH unique key LFU caches.
    key_caches = {}

    # Set up one session to use for all requests.
    # Requests to the same host will reuse the underlying TCP
    # connection, giving a performance increase.
    session = __get_requests_session(args)

    # Extract the proper identifier.
    ident_fields = {
        'pokestop': 'pokestop_id',
        'pokemon': 'encounter_id',
        'gym': 'gym_id',
        'gym_details': 'gym_id'
    }

    # Instantiate WH LFU caches for all cached types. We separate the caches
    # by ident_field types, because different ident_field (message) types can
    # use the same name for their ident field.
    for key in ident_fields:
        key_caches[key] = LFUCache(maxsize=args.wh_lfu_size)

    # Prepare to send data per timed message frames instead of per object.
    frame_interval_sec = (args.wh_frame_interval / 1000)
    frame_first_message_time_sec = default_timer()
    frame_messages = []
    first_message = True

    # How low do we want the queue size to stay?
    wh_warning_threshold = 100
    # How long can it be over the threshold, in seconds?
    # Default: 5 seconds per 100 in threshold + frame_interval_sec.
    wh_threshold_lifetime = int(5 * (wh_warning_threshold / 100.0))
    wh_threshold_timer += frame_interval_sec

    # The forever loop.
    while True:
        try:
            # Loop the queue.
            try:
                timeout = frame_interval_sec if len(
                    frame_messages) > 0 else None
                whtype, message = wh_queue.get(True, timeout)
            except Empty:
                pass
            else:
                frame_message = {'type': whtype, 'message': message}

                # Get the proper cache if this type has one.
                key_cache = None

                if whtype in key_caches:
                    key_cache = key_caches[whtype]

                # Get the unique identifier to check our cache, if it has one.
                ident = message.get(ident_fields.get(whtype), None)

                # cachetools in Python2.7 isn't thread safe, so we add a lock.
                with wh_lock:
                    # Only send if identifier isn't already in cache.
                    if ident is None or key_cache is None:
                        # We don't know what it is, or it doesn't have a cache,
                        # so let's just log and send as-is.
                        log.debug('Queued webhook item of uncached type: %s.',
                                  whtype)
                        frame_messages.append(frame_message)
                    elif ident not in key_cache:
                        key_cache[ident] = message
                        log.debug('Queued %s to webhook: %s.', whtype, ident)
                        frame_messages.append(frame_message)
                    else:
                        # Make sure to call key_cache[ident] in all branches
                        # so it updates the LFU usage count.
                        # If the object has changed in an important way, send
                        # new data to webhooks.
                        if __wh_object_changed(whtype, key_cache[ident],
                                               message):
                            key_cache[ident] = message
                            frame_messages.append(frame_message)
                            log.debug('Queued updated %s to webhook: %s.',
                                      whtype, ident)
                        else:
                            log.debug('Not queing %s to webhook: %s.',
                                      whtype, ident)
                wh_queue.task_done()
            # Store the time when we added the first message instead of the
            # time when we last cleared the messages, so we more accurately
            # measure time spent getting messages from our queue.
            now = default_timer()
            num_messages = len(frame_messages)

            if num_messages == 1 and first_message:
                frame_first_message_time_sec = now
                first_message = False

            # If enough time has passed, send the message frame.
            time_passed_sec = now - frame_first_message_time_sec

            if num_messages > 0 and (time_passed_sec >
                                     frame_interval_sec):
                log.debug('Sending %d items to %d webhook(s).',
                          len(frame_messages),
                          len(args.webhooks))
                send_to_webhooks(args, session, frame_messages)

                frame_messages = []
                first_message = True

            if wh_queue.qsize() > max_queue_size:
                max_queue_size = wh_queue.qsize()
                if args.runtime_statistics:
                    stats_queue.put(('wh_queue_max', max_queue_size))

            # Webhook queue moving too slow.
            if (not wh_over_threshold) and (
                    wh_queue.qsize() > wh_warning_threshold):
                wh_over_threshold = True
                wh_threshold_timer = default_timer()
            elif wh_over_threshold:
                if wh_queue.qsize() < wh_warning_threshold:
                    wh_over_threshold = False
                else:
                    timediff_sec = default_timer() - wh_threshold_timer

                    if timediff_sec > wh_threshold_lifetime:
                        log.warning('Webhook queue has been > %d (@%d);'
                                    + ' for over %d seconds,'
                                    + ' try increasing --wh-concurrency'
                                    + ' or --wh-threads.',
                                    wh_warning_threshold,
                                    wh_queue.qsize(),
                                    wh_threshold_lifetime)

        except Exception as e:
            log.exception('Exception in wh_updater: %s.', repr(e))


# Helpers

# Background handler for completed webhook requests.
# Currently doesn't do anything.
def __wh_completed():
    pass


def __get_requests_session(args):
    # Config / arg parser
    num_retries = args.wh_retries
    backoff_factor = args.wh_backoff_factor
    pool_size = args.wh_concurrency

    # Use requests & urllib3 to auto-retry.
    # If the backoff_factor is 0.1, then sleep() will sleep for [0.1s, 0.2s,
    # 0.4s, ...] between retries. It will also force a retry if the status
    # code returned is 500, 502, 503 or 504.
    session = FuturesSession(max_workers=pool_size)

    # If any regular response is generated, no retry is done. Without using
    # the status_forcelist, even a response with status 500 will not be
    # retried.
    retries = Retry(total=num_retries, backoff_factor=backoff_factor,
                    status_forcelist=[500, 502, 503, 504])

    # Mount handler on both HTTP & HTTPS.
    session.mount('http://', HTTPAdapter(max_retries=retries,
                                         pool_connections=pool_size,
                                         pool_maxsize=pool_size))
    session.mount('https://', HTTPAdapter(max_retries=retries,
                                          pool_connections=pool_size,
                                          pool_maxsize=pool_size))

    return session


def __get_key_fields(whtype):
    key_fields = {
        # lure_expiration is a UTC timestamp so it's good (Y).
        'pokestop': ['enabled', 'latitude',
                     'longitude', 'lure_expiration', 'active_fort_modifier'],
        'pokemon': ['spawnpoint_id', 'pokemon_id', 'latitude', 'longitude',
                    'disappear_time', 'move_1', 'move_2',
                    'individual_stamina', 'individual_defense',
                    'individual_attack', 'form', 'cp', 'pokemon_level'],
        'gym': [
            'team_id', 'guard_pokemon_id', 'enabled', 'latitude', 'longitude',
            'raid_active_until', 'occupied_since_ms', 'total_cp',
            'lowest_pokemon_motivation', 'slots_available'],
        'gym_details': ['latitude', 'longitude', 'team', 'pokemon'],
        'raid': [
            'spawn', 'start', 'end', 'pokemon_id', 'latitude', 'longitude'
        ]


    }

    return key_fields.get(whtype, [])


# Determine if a webhook object has changed in any important way (and
# requires a resend).
def __wh_object_changed(whtype, old, new):
    # Only test for important fields: don't trust last_modified fields.
    fields = __get_key_fields(whtype)

    if not fields:
        log.debug('Received an object of unknown type %s.', whtype)
        return True

    return not __dict_fields_equal(fields, old, new)


# Determine if two dicts have equal values for all keys in a list.
def __dict_fields_equal(keys, a, b):
    for k in keys:
        if a.get(k) != b.get(k):
            return False

    return True
