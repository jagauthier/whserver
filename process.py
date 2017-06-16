import time
import random
import logging
import yaml
from base64 import b64decode
from models import Pokemon, Gym, Pokestop, GymDetails, \
    Trainer, GymPokemon, GymMember, Authorizations
from threading import Thread
from utils import get_args, get_queues

log = logging.getLogger(__name__)

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()


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
            log.info("404 for %s", path[1:])
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
        # copy this for webhook forwarding
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

        to_keep = ["gym_id", "team_id", "guard_pokemon_id", "gym_points",
                   "enabled", "latitude", "longitude", "last_modified"]

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
            gym_members[p_uid] = {'gym_id': gymdetails[id]['gym_id'],
                                  'pokemon_uid': p_uid}
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

    def process_post(self, data_string):

        handled = ["pokemon", "pokestop", "gym", "gym_details"]

        self.pokemon_iteration = args.pokemon_inserts
        json_data = yaml.safe_load(data_string)
        data_type = json_data['type']

        if data_type in handled:
            log.debug("Processing: %s", data_type)
            func = getattr(self, "process_" + data_type)
            func(json_data['message'])
        else:
            log.warn("Received unhandled webhook type: %s", data_type)

    def reset_stats(self):
        self.pokemon_total = 0
        self.pokestop_total = 0
        self.gyms = 0
        self.gymdetail = 0
        self.ignored = 0


def main_process():

    handled = ["pokemon", "pokestop", "gym", "gym_details"]

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

        data_type = json_data['type']

        if data_type in handled:
            log.debug("Processing: %s", data_type)
            func = getattr(PH, "process_" + data_type)
            func(json_data['message'])
        else:
            log.warn("Received unhandled webhook type: %s", data_type)
            # log.debug("%s", json_data)
