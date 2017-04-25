#!/usr/bin/env python
from base64 import b64encode
import string
import random
import time
import requests

# Set a location
latitude = 39.655361
longitude = -84.225871

# generate this token with ./whserver -g <name>
TOKEN = 'TOKEN HERE'
WEBHOOK_URL = 'http://localhost:6009/' + TOKEN

# set this to the number of iterations of data to generate
# Run this in the background multiple times for load testing
ITERATIONS = 1

# You can run this for a specific amount of minutes or iterations.
# whichever comes first
MINUTES = 3

variance = .20000


def get_pokemon():

    lst = [random.choice(
           string.ascii_letters + string.digits) for n in xrange(16)]
    encounter = b64encode("".join(lst))
    spawnpoint = [random.choice(
        string.ascii_letters + string.digits) for n in xrange(12)]
    spawnpoint = "".join(spawnpoint)

    despawn = random.randint(15 * 60, 60 * 60)

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
        'latitude': latitude + random.uniform(0 - variance, variance),
        'longitude': longitude + random.uniform(0 - variance, variance),
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
    return pokemon


def get_gym_pokemon():

    lst = [random.choice(
           string.ascii_letters + string.digits) for n in xrange(16)]
    encounter = b64encode("".join(lst))
    spawnpoint = [random.choice(
        string.ascii_letters + string.digits) for n in xrange(12)]
    spawnpoint = "".join(spawnpoint)

    despawn = random.randint(15 * 60, 60 * 60)

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
        'latitude': latitude,
        'longitude': longitude,
        'move_1': random.randint(1, 281),
        'move_2': random.randint(1, 281),
        'pokemon_id': random.randint(1, 251),
        'seconds_until_despawn': despawn,
        'spawn_start': random.randint(500, 3000),
        'spawn_end': random.randint(500, 3000),
        'time_until_hidden_ms': int(time.time() * 1000) + 7200,
        'verified': True,
        'weight': random.uniform(5, 15)
    }
    return pokemon


def get_pokestop():

    lst = [random.choice(string.ascii_letters + string.digits)
           for n in xrange(16)]
    id = b64encode("".join(lst))
    pokestop = {
        'last_modified_time':  int(time.time() +
                                   random.randint(-3600, 0)) * 1000,
        'lure_expiration': None,
        'active_fort_modifier': None,
        'latitude': latitude + random.uniform(0 - variance, variance),
        'longitude': longitude + random.uniform(0 - variance, variance),
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


def get_gym():
    lst = [random.choice(string.ascii_letters + string.digits)
           for n in xrange(16)]
    id = b64encode("".join(lst))
    gym = {
        'gym_id': id,
        'team_id': random.randint(1, 3),
        'guard_pokemon_id': random.randint(1, 251),
        'gym_points': random.randint(1000, 50000),
        'enabled': 1,
        'latitude': latitude + random.uniform(0 - variance, variance),
        'longitude': longitude + random.uniform(0 - variance, variance),
        'last_modified': int(time.time() + random.randint(-3600, 0)) * 1000
    }
    return gym


def process_post(data):

    webhook_url = WEBHOOK_URL
    requests.post(webhook_url, json=data, headers={
                  'Content-Type': 'application/json'})


if __name__ == '__main__':
    start = time.time()
    for i in range(0, ITERATIONS):
        if time.time() - start > MINUTES * 60:
            break
        pokemon = get_pokemon()
        data = {
            'type': 'pokemon',
            'message': pokemon
        }
        process_post(data)
        if random.randint(0, 1000) > 990:
            pokestop = get_pokestop()
            data = {
                'type': 'pokestop',
                'message': pokestop
            }
            process_post(data)

        if random.randint(0, 1000) > 999:
            gym = get_gym()
            gymdetails = get_gymdetails(gym)
            data = {
                'type': 'gym',
                'message': gym
            }
            process_post(data)
            data = {
                'type': 'gym_details',
                'message': gymdetails
            }
            process_post(data)

        # just a gentle pause
        time.sleep(.05)
