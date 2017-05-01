#!/usr/bin/env python
from base64 import b64encode
import string
import random
import time
import requests
from optparse import OptionParser

UNOWN = 201


def get_pokemon(options):

    lst = [random.choice(
           string.ascii_letters + string.digits) for n in xrange(16)]
    encounter = b64encode("".join(lst))
    spawnpoint = [random.choice(
        string.ascii_letters + string.digits) for n in xrange(12)]
    spawnpoint = "".join(spawnpoint)
    despawn = random.randint(15 * 60, 60 * 60)
    location = options.location.split(",")
    variance = options.variance
    pokemon = {
        'encounter_id': encounter,
        'spawnpoint_id': spawnpoint,
        'disappear_time': int(time.time()) + despawn,
        'gender': random.randint(1, 2),
        'height': random.random(),
        'individual_attack': random.randint(0, 15),
        'individual_defense': random.randint(0, 15),
        'individual_stamina': random.randint(0, 15),
        'last_modified_time': int(time.time() * 1000) + 3600,
        'latitude': float(
            location[0]) + random.uniform(0 - variance, variance),
        'longitude': float(
            location[1]) + random.uniform(0 - variance, variance),
        'move_1': random.randint(1, 137),
        'move_2': random.randint(200, 281),
        'pokemon_id': random.randint(1, 251),
        'seconds_until_despawn': despawn,
        'spawn_start': random.randint(500, 3000),
        'spawn_end': random.randint(500, 3000),
        'time_until_hidden_ms': int(time.time() * 1000) + 7200,
        'verified': True,
        'weight': random.uniform(5, 15)
    }
    if pokemon['pokemon_id'] == UNOWN:
        pokemon.update({'form': random.randint(1, 26)})

    return pokemon


def get_pokestop(options):

    lst = [random.choice(string.ascii_letters + string.digits)
           for n in xrange(16)]
    id = b64encode("".join(lst))
    location = options.location.split(",")
    variance = options.variance
    pokestop = {
        'last_modified_time':  int(time.time() +
                                   random.randint(-3600, 0)) * 1000,
        'lure_expiration': None,
        'active_fort_modifier': None,
        'latitude': float(
            location[0]) + random.uniform(0 - variance, variance),
        'longitude': float(
            location[1]) + random.uniform(0 - variance, variance),
        'enabled': True,
        'pokestop_id': id
    }
    return pokestop


def get_gymdetails(gym):
    adj = ["Decorative", "Beautiful", "Fancy", "Groovy", "Wonderful"]
    name = ["store", "bridge", "walkway", "mural", "church", "park"]

    trainer_names = ["anthonycelaya", "AnthonyTrain51", "AoifeHehewut",
                     "ApertureMik3", "Apocryphal62", "APoorLoser",
                     "APsczulkoski", "ArachnidDeat", "Archercoebhan",
                     "Arcteryx65", "ArekMarr", "arielitus", "Aristotol",
                     "Arshaa", "arunns", "AshCaughtEm42", "AsherTrasherMan"]

    gymdetails = {
        'name': random.choice(adj) + " " + random.choice(name),
        'description': "There really isn't a description",
        'id': gym['gym_id'],
        'team': gym['team_id'],
        'latitude': gym['latitude'],
        'longitude': gym['latitude'],
        'url': 'http://lh6.ggpht.com/Iiipbi8mrGZf5VFnPJa7ff5kvi40' +
               'BdBLyfvE0Vq1tdjsbOnDLm2HKP0JCAwJgs7CDahZd3KIV5M72WyImY4J',
    }

    gym_pokes = []
    for i in range(1, 10):
        gym_pokes.append(
            {'additional_cp_multiplier': random.random(),
             'cp': random.randint(500, 3000),
             'cp_multiplier': random.random(),
             'height': random.random(),
             'iv_attack': random.randint(0, 15),
             'iv_defense': random.randint(0, 15),
             'iv_stamina': random.randint(0, 15),
             'move_1': random.randint(1, 137),
             'move_2': random.randint(200, 281),
             'pokemon_id': random.randint(1, 251),
             'weight': random.uniform(5, 15),
             'num_upgrades': random.randint(1, 5),
             'stamina': random.randint(100, 281),
             'stamina_max': random.randint(1, 281),
             'trainer_level': random.randint(1, 40),
             'pokemon_uid': random.randint(10000, 20000),
             'trainer_name': random.choice(trainer_names)
             })

    gymdetails.update({'pokemon': gym_pokes})
    return gymdetails


def get_gym(options):
    lst = [random.choice(string.ascii_letters + string.digits)
           for n in xrange(16)]
    id = b64encode("".join(lst))
    location = options.location.split(",")
    variance = options.variance
    gym = {
        'gym_id': id,
        'team_id': random.randint(1, 3),
        'guard_pokemon_id': random.randint(1, 251),
        'gym_points': random.randint(1000, 50000),
        'enabled': 1,
        'latitude': float(
            location[0]) + random.uniform(0 - variance, variance),
        'longitude': float(
            location[1]) + random.uniform(0 - variance, variance),
        'last_modified': int(time.time() + random.randint(-3600, 0)) * 1000
    }
    return gym


def process_post(message, payload, options):

    data = {
        'type': message,
        'message': payload
    }

    requests.post(options.webhook_url, json=data, headers={
                  'Content-Type': 'application/json'})


if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option("-i", "--iterations", type="int",
                      dest="iterations", default=1,
                      help="Iterations to process.")

    parser.add_option("-m", "--minutes",
                      dest="minutes", type="int",
                      help="Minutes to run.")

    parser.add_option("-w", "--webhook", dest="webhook_url",
                      help="Webhook url")

    parser.add_option("-l", "--location", dest="location",
                      help="Location")

    parser.add_option("-v", "--variance", dest="variance",
                      default=.2000, type="float",
                      help="Location variance")

    parser.add_option("-d", "--delay", dest="delay",
                      default=.05, type="float",
                      help="delay between sends")

    (options, args) = parser.parse_args()

    if not options.webhook_url or not options.location:
        print "Webhook url and location are required."
        parser.print_help()
        exit(0)

    start = time.time()
    i = 0
    while i < options.iterations:
        pokemon = get_pokemon(options)
        process_post('pokemon', pokemon, options)

        # below here, these are pokestop, and gyms.
        # The intent is that when you run this for thousands
        # you will only get some of these.

        if random.randint(0, 1000) > 990:
            pokestop = get_pokestop(options)
            process_post('pokestop', pokestop, options)

        if random.randint(0, 1000) > 999:
            gym = get_gym(options)
            gymdetails = get_gymdetails(gym)
            process_post('gym', gym, options)
            process_post('gym_details', gymdetails, options)

        if options.minutes:
            # reset the iterations, we basically ignore it
            if time.time() - start > options.minutes * 60:
                break
        else:
            i += 1

        # just a gentle pause
        time.sleep(options.delay)
