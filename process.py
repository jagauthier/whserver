import time
import random
import logging
import yaml
from base64 import b64decode, b64encode
from models import Pokemon, Gym, Pokestop, GymDetails, \
    Trainer, GymPokemon, GymMember, Authorizations, Raid
from threading import Thread
from utils import get_args, get_queues

log = logging.getLogger(__name__)

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()

# cp_multiplier look up. PGScout is sending only the level now

cpm = ({1: 0.094, 1.5: 0.135137432, 2: 0.16639787, 2.5: 0.192650919,
        3: 0.21573247, 3.5: 0.236572661, 4: 0.25572005, 4.5: 0.273530381,
        5: 0.29024988, 5.5: 0.306057377, 6: 0.3210876, 6.5: 0.335445036,
        7: 0.34921268, 7.5: 0.362457751, 8: 0.37523559, 8.5: 0.387592406,
        9: 0.39956728, 9.5: 0.411193551, 10: 0.42250001, 10.5: 0.432926419,
        11: 0.44310755, 11.5: 0.453059958, 12: 0.46279839, 12.5: 0.472336083,
        13: 0.48168495, 13.5: 0.4908558, 14: 0.49985844, 14.5: 0.508701765,
        15: 0.51739395, 15.5: 0.525942511, 16: 0.53435433, 16.5: 0.542635767,
        17: 0.55079269, 17.5: 0.558830576, 18: 0.56675452, 18.5: 0.574569153,
        19: 0.58227891, 19.5: 0.589887917, 20: 0.59740001, 20.5: 0.604818814,
        21: 0.61215729, 21.5: 0.619399365, 22: 0.62656713, 22.5: 0.633644533,
        23: 0.64065295, 23.5: 0.647576426, 24: 0.65443563, 24.5: 0.661214806,
        25: 0.667934, 25.5: 0.674577537, 26: 0.68116492, 26.5: 0.687680648,
        27: 0.69414365, 27.5: 0.700538673, 28: 0.70688421, 28.5: 0.713164996,
        29: 0.71939909, 29.5: 0.725571552, 30: 0.7317, 30.5: 0.734741009,
        31: 0.73776948, 31.5: 0.740785574, 32: 0.74378943, 32.5: 0.746781211,
        33: 0.74976104, 33.5: 0.752729087, 34: 0.75568551, 34.5: 0.758630378,
        35: 0.76156384, 35.5: 0.764486065, 36: 0.76739717, 36.5: 0.770297266,
        37: 0.7731865, 37.5: 0.776064962, 38: 0.77893275, 38.5: 0.781790055,
        39: 0.78463697, 39.5: 0.787473578, 40: 0.79030001})


class Auth():
    authorizations = {}
    auth_stats = {}

    post_success = 0
    post_fail = 0

    def __init__(self):
        log.debug("Beginning authorization thread.")
        t = Thread(target=self.load_auth, name='load-auth')
        t.daemon = True
        t.start()

    def load_auth(self):
        # reload tokens every 30 seconds
        while (True):
            query = Authorizations.select(Authorizations.token,
                                          Authorizations.name)
            for data in query:
                self.authorizations[data.token] = data.name
                if data.token not in self.auth_stats:
                    self.auth_stats[data.token] = 0

            if args.runtime_statistics:
                stats_queue.put(('authorizations', self.authorizations))
                stats_queue.put(('auth_stats', self.auth_stats))
                posts = {'success': self.post_success,
                         'fail': self.post_fail}
                stats_queue.put(('posts', posts))

            time.sleep(30)

    def validate(self, path):
        if path[1:] not in self.authorizations:
            log.info("404 for '%s'", path[1:])
            self.post_fail += 1
            return False

        # the keys here are loaded from the database.
        # They are re-read and stored in variables
        # so we don't query the database for it upon each connection
        # This will allow us to prevent false insertions
        self.auth_stats[path[1:]] += 1
        self.post_success += 1
        return True


def process_stats():
    start_time = time.time()
    stat_time = start_time

    pokemon_total = 0
    pokestop_total = 0
    gym_total = 0
    gym_details = 0
    ignored = 0
    raid_total = 0

    max_stat_queue = 0
    max_db_queue = 0
    max_wh_queue = 0
    max_process_queue = 0

    while (True):
            # we're just going to block here until we get data
            # if the stats don't run on the exact rsi
            # I don't think it's a big deal

        stat, data = stats_queue.get()
        qsize = stats_queue.qsize()
        if qsize > max_stat_queue:
            max_stat_queue = qsize
        if stat == "stats":
            pokemon_total += data['pokemon']
            pokestop_total += data['pokestops']
            gym_total += data['gyms']
            gym_details += data['gymdetails']
            ignored += data['ignored']
            raid_total += data['raids']

        if stat == "authorizations":
            auths = data

        if stat == "auth_stats":
            auth_stats = data

        if stat == "posts":
            post_success = data['success']
            post_fails = data['fail']

        if stat == "db_queue_max":
            max_db_queue = data

        if stat == "process_queue_max":
            max_process_queue = data

        if stat == "wh_queue_max":
            max_wh_queue = data

        stats_queue.task_done()
        # run the stats
        if time.time() - stat_time > args.runtime_statistics * 60:
            stat_time = time.time()
            log.info("--- Runtime Statistics ---")
            log.info("Success/Fails: [%i,%i]", post_success,
                     post_fails)
            log.info("Pokemon: %i", pokemon_total)
            log.info("Pokestops %i", pokestop_total)
            log.info("Gyms: %i", gym_total)
            log.info("Gym details: %i", gym_details)
            log.info("Raids: %i", raid_total)
            log.info("Ignored: %i", ignored)
            log.info("Average requests per minute: %i",
                     int((post_success + post_fails) /
                         ((time.time() - start_time) / 60)))
            log.info("--- Requests by token assignment ---")

            for token in auths:
                if token in auth_stats:
                    if auth_stats[token]:
                        log.info("%s: %i", auths[token], auth_stats[token])

            log.info("--- Queue Info (Max) ---")
            log.info("Process: %i", max_process_queue)
            log.info("Stats  : %i", max_stat_queue)
            log.info("DB     : %i", max_db_queue)
            log.info("WH     : %i", max_wh_queue)


class ProcessHook():

    # used for bulk imports
    pokemon_iteration = 1
    pokemon_counter = 0

    # total stats
    pokemon_total = 0
    pokestop_total = 0
    gym_total = 0
    gym_details = 0
    ignored = 0
    raid_total = 0
    # to hold multiple pokemon for bulk insertions
    pokemon_list = {}

    def __init__(self):
        if args.runtime_statistics:
            t = Thread(target=self.stats_update, name='stats_update')
            t.daemon = True
            t.start()

    def stats_update(self):
        while (True):
            stats = {'pokemon': self.pokemon_total,
                     'pokestops': self.pokestop_total,
                     'gyms': self.gym_total,
                     'gymdetails': self.gym_details,
                     'raids': self.raid_total,
                     'ignored': self.ignored
                     }
            stats_queue.put(('stats', stats))
            self.reset_stats()
            time.sleep(random.randint(5, 8))

    def process_pokemon(self, json_data):

        # Increase the # of pokemon received, even if it's not stored
        self.pokemon_total += 1
        if args.no_pokemon:
            return
        # more items come from the webhook than we need in the database.
        # since the webhook output can change at any time to add fields,
        # we'll make a list based on database tables.  This will change with DB
        # schema changes
        to_keep = ["encounter_id", "spawnpoint_id", "pokemon_id", "latitude",
                   "longitude", "disappear_time", "individual_attack",
                   "individual_defense", "individual_stamina", "move_1",
                   "move_2", "weight", "height", "gender", "form", "cp",
                   "cp_multiplier", "last_modified"]
        pokemon = {}
        enc = json_data['encounter_id']
        pokemon[enc] = json_data
        if pokemon[enc]['pokemon_id'] in args.ignore_pokemon:
            self.ignored += 1
            return

        # copy this for webhook forwarding
        wh_poke = pokemon[enc].copy()

        # pgscout/monocle hack for level/cpm
        if "level" in pokemon[enc]:
            log.info("Got a level: %i. CPM: %f",
                     pokemon[enc]['level'], cpm[pokemon[enc]['level']])
            pokemon[enc].update({'cp_multiplier': cpm[pokemon[enc]['level']]})

        # if people are running an older DB version sending wh:
        if "form" not in pokemon[enc]:
            pokemon[enc].update({'form': None})
        if "cp" not in pokemon[enc]:
            pokemon[enc].update({'cp': None})
        if "cp_multiplier" not in pokemon[enc]:
            pokemon[enc].update({'cp_multiplier': None})

        # need to change this from an epoch style type to
        # datetime.dateime for the database insert
        pokemon[enc].update({'disappear_time':
                             time.gmtime(pokemon[enc]['disappear_time'])})
        # copies all the keys we want for the DB
        pokemon[enc] = {key: pokemon[enc][key]
                        for key in pokemon[enc] if key in to_keep}
        log.debug("%s", pokemon)
        # multiple sources at the time with the same encounter id?
        # I don't know... but just in case
        if enc not in self.pokemon_list:
            self.pokemon_list[enc] = pokemon[enc]
            self.pokemon_counter += 1
        # put it into the db queue
        if self.pokemon_counter % self.pokemon_iteration == 0:
            db_queue.put((Pokemon, self.pokemon_list))
            self.pokemon_counter = 0
            self.pokemon_list = {}
        if args.webhooks:
            wh_queue.put(('pokemon', wh_poke))

    def process_pokestop(self, json_data):
        # Increase the # of pokestops received, even if it's not stored
        self.pokestop_total += 1

        if args.no_pokestops:
            return

        to_keep = ["pokestop_id", "enabled", "latitude", "longitude",
                   "last_modified", "lure_expiration", "active_fort_modifier",
                   "last_updated"]
        pokestop = {}
        id = json_data['pokestop_id']
        pokestop[id] = json_data
        # copy this for webhook forwarding
        wh_pokestop = pokestop[id].copy()
        # last_modified is DB, last_modified_time is WH
        # and decode the id back
        if pokestop[id]['lure_expiration'] is not None:
            pokestop[id].update({'lure_expiration':
                                 time.gmtime(pokestop[id]['lure_expiration'])})
        pokestop[id].update({'last_modified':
                             time.gmtime(
                                 pokestop[id]['last_modified_time'] / 1000),
                             'pokestop_id':
                             b64decode(pokestop[id]['pokestop_id'])})
        # copies all the keys we want for the DB
        pokestop[id] = {key: pokestop[id][key]
                        for key in pokestop[id] if key in to_keep}

        log.debug("%s", pokestop)
        # put it into the db queue
        db_queue.put((Pokestop, pokestop))
        if args.webhooks:
            wh_queue.put(('pokestop', wh_pokestop))

    def process_gym(self, json_data):
        # Increase the # of gyms received, even if it's not stored
        self.gym_total += 1
        if args.no_gyms:
            return

        to_keep = ["gym_id", "team_id", "guard_pokemon_id",
                   "slots_available", "enabled", "latitude", "longitude",
                   "total_cp", "last_modified"]

        gym = {}
        id = json_data['gym_id']
        gym[id] = json_data
        # copy this for webhook forwarding
        wh_gym = gym[id].copy()
        # need to change this from an epoch style type to
        # datetime.dateime for the database insert
        # and decode the gym id
        gym[id].update({'last_modified':
                        time.gmtime(gym[id]['last_modified'] / 1000),
                        'gym_id': b64decode(gym[id]['gym_id'])})

        # copies all the keys we want for the DB
        gym[id] = {key: gym[id][key] for key in gym[id] if key in to_keep}

        log.debug("%s", gym)
        # put it into the db queue
        db_queue.put((Gym, gym))
        if args.webhooks:
            wh_queue.put(('gym', wh_gym))

    def process_gympokemon(self, id, gymdetails):
        to_keep = ["pokemon_uid", "pokemon_id", "cp", "trainer_name",
                   "num_upgrades", "move_1", "move_2", "height", "weight",
                   "stamina",  "stamina_max", "cp_multiplier",
                   "additional_cp_multiplier", "iv_defense", "iv_stamina",
                   "iv_attack"]
        gym_pokemon = {}
        gym_members = {}
        trainers = {}
        for pokemon in gymdetails[id]['pokemon']:

            trainers[pokemon['trainer_name']] = {
                'name': pokemon['trainer_name'],
                'team': gymdetails[id]['team'],
                'level': pokemon['trainer_level']}
            p_uid = pokemon['pokemon_uid']

            if 'deployment_time' not in pokemon:
                pokemon['deployment_time'] = time.gmtime(time.time() * 1000)

            gym_members[p_uid] = {'gym_id': gymdetails[id]['gym_id'],
                                  'pokemon_uid': p_uid,
                                  'cp_decayed': pokemon['cp_decayed'],
                                  'deployment_time':
                                      time.gmtime(pokemon['deployment_time'])}
            gym_pokemon[p_uid] = pokemon
            gym_pokemon[p_uid] = {key: gym_pokemon[p_uid][key]
                                  for key in gym_pokemon[p_uid]
                                  if key in to_keep}

        return gym_pokemon, gym_members, trainers

    def process_gym_details(self, json_data):
        self.gym_details += 1
        if args.no_gymdetail:
            return

        to_keep = ["gym_id", "name", "description", "url"]

        gymdetails = {}
        id = json_data['id']
        gymdetails[id] = json_data
        # copy this for webhook forwarding
        wh_gymdetails = gymdetails[id].copy()
        # the database sends "id", the but the database
        # wants gym_id
        gymdetails[id].update({'gym_id': b64decode(gymdetails[id]['id'])})
        # we need to extract trainer and pokemon information before
        # getting gymdetails ready for the database
        (gym_pokemon,
         gym_members,
         trainers) = self.process_gympokemon(id, gymdetails)

        # copies all the keys we want for the DB
        gymdetails[id] = {key: gymdetails[id][key] for key in gymdetails[id]
                          if key in to_keep}

        log.debug("%s", gymdetails)
        # put it all into the db queue
        db_queue.put((GymDetails, gymdetails))
        db_queue.put((Trainer, trainers))
        db_queue.put((GymPokemon, gym_pokemon))
        db_queue.put((GymMember, gym_members))
        if args.webhooks:
            wh_queue.put(('gym_details', wh_gymdetails))

    def process_raid(self, json_data):
        # Increase the # of pokestops received, even if it's not stored
        self.raid_total += 1

        if args.no_raids:
            return

        to_keep = ["pokemon_id", "spawn", "move_1", "move_2", "end",
                   "level", "gym_id", "start", "cp"]

        raid = {}

        # Not RM data coming in...
        if 'raid_seed' in json_data:
            # copy for wh forwarding

            id = json_data['raid_seed']
            raid[id] = json_data
            wh_raid = raid[id].copy()

            raid[id]['gym_id'] = b64encode(str(time.time()))
            raid[id]['spawn'] = raid[id]['start'] - 7200

            # if we're getting 0, they need to be set to None
            if raid[id]['cp'] == 0:
                raid[id].update({'cp': None,
                                 'pokemon_id': None,
                                 'move_1': None,
                                 'move_2': None})

            # let's see if we are getting gyms from RM by locating the gym via
            # lat and lon.
            try:
                gym_id = Gym.get(Gym.latitude == json_data['latitude'],
                                 Gym.longitude == json_data['longitude'])
            except:
                # throws an exception if the record cannot be found
                gym_id = None

            if gym_id is None:
                log.info("No gym found. Artifically adding one.")
                gym = {'gym_id': raid[id]['gym_id'], 'team_id': 0,
                       'guard_pokemon_id': 0, 'slots_available': 6,
                       'enabled': 1, 'latitude': raid[id]['latitude'],
                       'longitude': raid[id]['longitude'], 'total_cp': 0,
                       'last_modified': time.time() * 1000}
                self.process_gym(gym)
            else:
                log.info("Gym found.")
                json_data['gym_id'] = b64encode(gym_id.gym_id)
        else:
            # standard RM wh
            id = json_data['gym_id']
            raid[id] = json_data
            wh_raid = raid[id].copy()

        # decode the id back
        raid[id].update({'gym_id': b64decode(raid[id]['gym_id']),
                         'spawn': time.gmtime(raid[id]['spawn']),
                         'start': time.gmtime(raid[id]['start']),
                         'end': time.gmtime(raid[id]['end'])})
        # copies all the keys we want for the DB
        raid[id] = {key: raid[id][key]
                    for key in raid[id] if key in to_keep}

        log.debug("%s", raid)
        # put it into the db queue
        db_queue.put((Raid, raid))
        if args.webhooks:
            wh_queue.put(('raid', wh_raid))

    def reset_stats(self):
        self.pokemon_total = 0
        self.pokestop_total = 0
        self.gyms = 0
        self.gymdetail = 0
        self.ignored = 0
        self.raid_total = 0


def main_process():

    handled = ["pokemon", "pokestop", "gym", "gym_details", "raid"]

    PH = ProcessHook()
    max_queue_size = 0
    while (True):

        # if some seconds have passed, throw everything to the stats
        data_string = process_queue.get()
        json_data = yaml.safe_load(data_string)
        process_queue.task_done()

        if process_queue.qsize() > max_queue_size:
            max_queue_size = process_queue.qsize()
            if args.runtime_statistics:
                stats_queue.put(('process_queue_max', max_queue_size))
        records = len(json_data)

        for x in range(0, records):
            data_type = json_data[x]['type']
            if data_type in handled:
                log.debug("Processing: %s", data_type)
                func = getattr(PH, "process_" + data_type)
                func(json_data[x]['message'])
            else:
                log.warn("Received unhandled webhook type: %s", data_type)
            # log.debug("%s", json_data)
