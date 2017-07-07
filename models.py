#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import sys
from peewee import InsertQuery, FloatField, SmallIntegerField, \
    IntegerField, CharField, DoubleField, BooleanField, \
    DateTimeField, TextField, Model
from datetime import datetime, timedelta

from timeit import default_timer
from utils import get_args, get_queues
from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError
from playhouse.migrate import migrate, MySQLMigrator

log = logging.getLogger(__name__)

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()

# Want to stay compatible with RM's schema
db_schema_version = 20


class MyRetryDB(RetryOperationalError, PooledMySQLDatabase):
    pass


db = None

# Reduction of CharField to fit max length inside 767 bytes for utf8mb4 charset


class Utf8mb4CharField(CharField):
    def __init__(self, max_length=191, *args, **kwargs):
        self.max_length = max_length
        super(CharField, self).__init__(*args, **kwargs)


def init_database():
    if args.db_type == 'mysql':
        log.info('Connecting to MySQL database on %s:%i...',
                 args.db_host, args.db_port)
        connections = args.db_max_connections
        global db
        db = MyRetryDB(
            args.db_name,
            user=args.db_user,
            password=args.db_pass,
            host=args.db_host,
            port=args.db_port,
            max_connections=connections,
            stale_timeout=300)
    return db


class BaseModel(Model):
    db = init_database()

    class Meta:
        database = db


class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api
    # because they are too big for sqlite to handle.
    encounter_id = Utf8mb4CharField(primary_key=True, max_length=50)
    spawnpoint_id = Utf8mb4CharField(index=True)
    pokemon_id = SmallIntegerField(index=True)
    latitude = DoubleField()
    longitude = DoubleField()
    disappear_time = DateTimeField(index=True)
    individual_attack = SmallIntegerField(null=True)
    individual_defense = SmallIntegerField(null=True)
    individual_stamina = SmallIntegerField(null=True)
    move_1 = SmallIntegerField(null=True)
    move_2 = SmallIntegerField(null=True)
    cp = SmallIntegerField(null=True)
    cp_multiplier = FloatField(null=True)
    weight = FloatField(null=True)
    height = FloatField(null=True)
    gender = SmallIntegerField(null=True)
    form = SmallIntegerField(null=True)
    last_modified = DateTimeField(
        null=True, index=True, default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)


class Pokestop(BaseModel):
    pokestop_id = Utf8mb4CharField(primary_key=True, max_length=50)
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    lure_expiration = DateTimeField(null=True, index=True)
    active_fort_modifier = Utf8mb4CharField(
        max_length=50, null=True, index=True)
    last_updated = DateTimeField(
        null=True, index=True, default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)


class Gym(BaseModel):
    UNCONTESTED = 0
    TEAM_MYSTIC = 1
    TEAM_VALOR = 2
    TEAM_INSTINCT = 3

    gym_id = Utf8mb4CharField(primary_key=True, max_length=50)
    team_id = SmallIntegerField()
    guard_pokemon_id = SmallIntegerField()
    slots_available = SmallIntegerField()
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    total_cp = SmallIntegerField()
    last_modified = DateTimeField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow, index=True)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)


class GymMember(BaseModel):
    gym_id = Utf8mb4CharField(index=True)
    pokemon_uid = Utf8mb4CharField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow, index=True)
    deployment_time = DateTimeField()
    cp_decayed = SmallIntegerField()

    class Meta:
        primary_key = False


class GymPokemon(BaseModel):
    pokemon_uid = Utf8mb4CharField(primary_key=True, max_length=50)
    pokemon_id = SmallIntegerField()
    cp = SmallIntegerField()
    trainer_name = Utf8mb4CharField(index=True)
    num_upgrades = SmallIntegerField(null=True)
    move_1 = SmallIntegerField(null=True)
    move_2 = SmallIntegerField(null=True)
    height = FloatField(null=True)
    weight = FloatField(null=True)
    stamina = SmallIntegerField(null=True)
    stamina_max = SmallIntegerField(null=True)
    cp_multiplier = FloatField(null=True)
    additional_cp_multiplier = FloatField(null=True)
    iv_defense = SmallIntegerField(null=True)
    iv_stamina = SmallIntegerField(null=True)
    iv_attack = SmallIntegerField(null=True)
    last_seen = DateTimeField(default=datetime.utcnow)


class Trainer(BaseModel):
    name = Utf8mb4CharField(primary_key=True, max_length=50)
    team = SmallIntegerField()
    level = SmallIntegerField()
    last_seen = DateTimeField(default=datetime.utcnow)


class GymDetails(BaseModel):
    gym_id = Utf8mb4CharField(primary_key=True, max_length=50)
    name = Utf8mb4CharField()
    description = TextField(null=True, default="")
    url = Utf8mb4CharField()
    last_scanned = DateTimeField(default=datetime.utcnow)


class Versions(BaseModel):
    key = Utf8mb4CharField()
    val = SmallIntegerField()

    class Meta:
        primary_key = False


class Authorizations(BaseModel):
    token = Utf8mb4CharField(primary_key=True, max_length=32)
    name = Utf8mb4CharField(index=True)

    class Meta:
        primary_key = False


class Raid(BaseModel):
    gym_id = Utf8mb4CharField(primary_key=True, max_length=50)
    level = IntegerField(index=True)
    spawn = DateTimeField(index=True)
    start = DateTimeField(index=True)
    end = DateTimeField(index=True)
    pokemon_id = SmallIntegerField(null=True)
    cp = IntegerField(null=True)
    move_1 = SmallIntegerField(null=True)
    move_2 = SmallIntegerField(null=True)
    last_scanned = DateTimeField(
        default=datetime.utcnow, index=True)


def db_updater():
    # The forever loop.

    max_queue_size = 0
    last_notify = time.time()
    while True:
        try:

            while True:
                try:
                    db.get_conn()
                    break
                except Exception as e:
                    log.warning('%s... Retrying...', repr(e))
                    time.sleep(5)

            # Loop the queue.
            while True:
                last_upsert = default_timer()
                model, data = db_queue.get()
                bulk_upsert(model, data, db)
                db_queue.task_done()
                log.debug('Upserted to %s, %d records (upsert queue '
                          'remaining: %d) in %.2f seconds.',
                          model.__name__,
                          len(data),
                          db_queue.qsize(),
                          default_timer() - last_upsert)
                del model
                del data

                if db_queue.qsize() > max_queue_size:
                    max_queue_size = db_queue.qsize()
                    if args.runtime_statistics:
                        stats_queue.put(('db_queue_max', max_queue_size))

                if db_queue.qsize() > 50:
                    if time.time() > last_notify + 1:
                        log.warning(
                            "DB queue is > 50 (@%d); try increasing " +
                            "--db-threads.",
                            db_queue.qsize())
                        last_notify = time.time()

        except Exception as e:
            log.exception('Exception in db_updater: %s', repr(e))
            time.sleep(5)


def clean_db_loop():
    # pause before starting so it doesn't run at the same time as
    # other interval tasks
    time.sleep(15)
    while True:
        try:
            # pokestop are received infrequently over webooks, so
            # we will leave this to unflag lures
            query = (Pokestop
                     .update(lure_expiration=None, active_fort_modifier=None)
                     .where(Pokestop.lure_expiration < datetime.utcnow()))
            query.execute()

            if args.purge_data > 0:
                log.info("Beginning purge of old Pokemon spawns.")
                start = datetime.utcnow()
                query = (Pokemon
                         .delete()
                         .where((Pokemon.disappear_time <
                                 (datetime.utcnow() -
                                  timedelta(hours=args.purge_data)))))
                rows = query.execute()
                end = datetime.utcnow()
                diff = end - start
                log.info("Completed purge of old Pokemon spawns. "
                         "%i deleted in %f seconds.",
                         rows, diff.total_seconds())

            # log.info('Regular database cleaning complete.')
            time.sleep(60)
        except Exception as e:
            log.exception('Exception in clean_db_loop: %s', repr(e))


def bulk_upsert(cls, data, db):
    num_rows = len(data.values())
    i = 0
    step = 250
    max_fails = 3
    fails = 0

    with db.atomic():
        while i < num_rows:
            log.debug('Inserting items %d to %d.', i, min(i + step, num_rows))
            try:
                # Turn off FOREIGN_KEY_CHECKS on MySQL, because apparently it's
                # unable to recognize strings to update unicode keys for
                # foreign key fields, thus giving lots of foreign key
                # constraint errors.
                db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')
                # Use peewee's own implementation of the insert_many() method.
                InsertQuery(cls, rows=data.values()
                            [i:min(i + step, num_rows)]).upsert().execute()
                db.execute_sql('SET FOREIGN_KEY_CHECKS=1;')

            except Exception as e:
                # If there is a DB table constraint error, dump the data and
                # don't retry.
                #
                # Unrecoverable error strings:
                unrecoverable = ['constraint', 'has no attribute',
                                 'peewee.IntegerField object at']
                has_unrecoverable = filter(
                    lambda x: x in str(e), unrecoverable)
                if has_unrecoverable:
                    log.warning('%s. Data is:', repr(e))
                    log.warning(data.items())
                else:
                    log.warning('%s... Retrying...', repr(e))
                    time.sleep(1)
                    fails += 1
                    if fails > max_fails:
                        return
                    continue

            i += step


def create_tables(db):
    verify_database_schema(db)
    tables = [Authorizations, Pokemon, Pokestop, Gym, GymDetails, GymMember,
              GymPokemon, Trainer, Raid, Versions]
    db.get_conn()
    for table in tables:
        if not table.table_exists():
            log.info("Creating table: %s", table.__name__)
            db.create_tables([table], safe=True)

    # fixing encoding on present and future tables
    cmd_sql = '''
        SELECT table_name FROM information_schema.tables WHERE
        table_collation != "utf8mb4_unicode_ci" AND table_schema = "%s";
        ''' % args.db_name
    change_tables = db.execute_sql(cmd_sql)

    if change_tables.rowcount > 0:
        log.info('Changing collation and charset on %s tables.',
                 change_tables.rowcount)

        if change_tables.rowcount == len(tables) + 1:
            log.info('Changing whole database, this might a take while.')

        with db.atomic():
            db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')
            for table in change_tables:
                log.debug('Changing collation and charset on table %s.',
                          table[0])
                cmd_sql = '''ALTER TABLE %s CONVERT TO CHARACTER SET utf8mb4
                            COLLATE utf8mb4_unicode_ci;''' % str(table[0])
                db.execute_sql(cmd_sql)
            db.execute_sql('SET FOREIGN_KEY_CHECKS=1;')
    db.close()


def drop_tables(db):
    tables = [Pokemon, Pokestop, Gym, GymDetails, GymMember,
              GymPokemon, Trainer, Raid, Versions]
    db.get_conn()
    db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')
    for table in tables:
        if table.table_exists():
            log.info("Dropping table: %s", table.__name__)
            db.drop_tables([table], safe=True)
    db.execute_sql('SET FOREIGN_KEY_CHECKS=1;')
    db.close()


def verify_database_schema(db):
    if not Versions.table_exists():
        db.create_tables([Versions])
        InsertQuery(Versions, {Versions.key: 'schema_version',
                               Versions.val: db_schema_version}
                    ).execute()
    else:
        db_ver = Versions.get(Versions.key == 'schema_version').val

        if db_ver < db_schema_version:
            database_migrate(db, db_ver)

        elif db_ver > db_schema_version:
            log.error('Your database version (%i) appears to be newer than '
                      'the code supports (%i).', db_ver, db_schema_version)
            sys.exit(1)


def database_migrate(db, old_ver):
    # Update database schema version.
    Versions.update(val=db_schema_version).where(
        Versions.key == 'schema_version').execute()

    log.info('Detected database version %i, updating to %i...',
             old_ver, db_schema_version)

    # Perform migrations here.
    migrator = MySQLMigrator(db)

    if old_ver < 17:
        migrate(
            migrator.add_column('pokemon', 'form',
                                SmallIntegerField(null=True)))
    if old_ver < 18:
        migrate(
            migrator.add_column('pokemon', 'cp',
                                SmallIntegerField(null=True)))
    if old_ver < 19:
        migrate(
            migrator.add_column('pokemon', 'cp_multiplier',
                                FloatField(null=True)))
    if old_ver < 20:
        migrate(
            migrator.drop_column('gym', 'gym_points'),
            migrator.add_column('gym', 'slots_available',
                                SmallIntegerField(null=False, default=0)),
            migrator.add_column('gymmember', 'cp_decayed',
                                SmallIntegerField(null=False, default=0)),
            migrator.add_column('gymmember', 'deployment_time',
                                DateTimeField(
                                    null=False, default=datetime.utcnow())),
            migrator.add_column('gym', 'total_cp',
                                SmallIntegerField(null=False, default=0)))
