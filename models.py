#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import pprint

from peewee import InsertQuery, FloatField, SmallIntegerField, \
    IntegerField, CharField, DoubleField, BooleanField, \
    DateTimeField, TextField, Model, BigIntegerField
from datetime import datetime, timedelta

from timeit import default_timer
from utils import get_args, get_queues, peewee_attr_to_col
from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError
from playhouse.migrate import migrate, MySQLMigrator

log = logging.getLogger(__name__)

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()

# Want to stay compatible with RM's schema
# Jan 04, 2018
# Changed to 21 without any changes. RM just made changes to the
# WorkerStatus and MainWorker
db_schema_version = 27


class MyRetryDB(RetryOperationalError, PooledMySQLDatabase):
    pass


db = None

# Reduction of CharField to fit max length inside 767 bytes for utf8mb4 charset


class Utf8mb4CharField(CharField):
    def __init__(self, max_length=191, *args, **kwargs):
        self.max_length = max_length
        super(CharField, self).__init__(*args, **kwargs)


def init_database():
    log.info('Connecting to MySQL database on %s:%i...',
             args.db_host, args.db_port)

    global db
    db = MyRetryDB(
        args.db_name,
        user=args.db_user,
        password=args.db_pass,
        host=args.db_host,
        port=args.db_port,
        max_connections=None,
        stale_timeout=30)
    return db


class UBigIntegerField(BigIntegerField):
    db_field = 'bigint unsigned'


class BaseModel(Model):
    db = init_database()

    class Meta:
        database = db


class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api
    # because they are too big for sqlite to handle.
    encounter_id = UBigIntegerField(primary_key=True)
    spawnpoint_id = UBigIntegerField(index=True)
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
    costume = SmallIntegerField(null=True)
    form = SmallIntegerField(null=True)
    weather_boosted_condition = SmallIntegerField(null=True)
    last_modified = DateTimeField(
        null=True, index=True, default=datetime.utcnow)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),
                   (('disappear_time', 'pokemon_id'), False)
                   )


class Pokestop(BaseModel):
    pokestop_id = Utf8mb4CharField(primary_key=True, max_length=50)
    enabled = BooleanField()
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(index=True)
    lure_expiration = DateTimeField(null=True, index=True)
    active_fort_modifier = SmallIntegerField(null=True, index=True)
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
    park = BooleanField(default=False)
    sponsor = SmallIntegerField(null=True)
    latitude = DoubleField()
    longitude = DoubleField()
    total_cp = SmallIntegerField()
    last_modified = DateTimeField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow, index=True)

    class Meta:
        indexes = ((('latitude', 'longitude'), False),)


class GymMember(BaseModel):
    gym_id = Utf8mb4CharField(index=True)
    pokemon_uid = UBigIntegerField(index=True)
    last_scanned = DateTimeField(default=datetime.utcnow, index=True)
    deployment_time = DateTimeField()
    cp_decayed = SmallIntegerField()

    class Meta:
        primary_key = False


class GymPokemon(BaseModel):
    pokemon_uid = UBigIntegerField(primary_key=True)
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
    costume = SmallIntegerField(null=True)
    form = SmallIntegerField(null=True)
    shiny = SmallIntegerField(null=True)
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


class Weather(BaseModel):
    s2_cell_id = Utf8mb4CharField(primary_key=True, max_length=50)
    latitude = DoubleField()
    longitude = DoubleField()
    cloud_level = SmallIntegerField(null=True, index=True, default=0)
    rain_level = SmallIntegerField(null=True, index=True, default=0)
    wind_level = SmallIntegerField(null=True, index=True, default=0)
    snow_level = SmallIntegerField(null=True, index=True, default=0)
    fog_level = SmallIntegerField(null=True, index=True, default=0)
    wind_direction = SmallIntegerField(null=True, index=True, default=0)
    gameplay_weather = SmallIntegerField(null=True, index=True, default=0)
    severity = SmallIntegerField(null=True, index=True, default=0)
    warn_weather = SmallIntegerField(null=True, index=True, default=0)
    world_time = SmallIntegerField(null=True, index=True, default=0)
    last_updated = DateTimeField(
        default=datetime.utcnow,
        null=True,
        index=True)

    @staticmethod
    def get_weathers():
        query = Weather.select().dicts()

        weathers = []
        for w in query:
            weathers.append(w)

        return weathers

    @staticmethod
    def get_weather_by_location(swLat, swLng, neLat, neLng, alert):
        # We can filter by the center of a cell, this deltas can expand
        # the viewport bounds
        # So cells with center outside the viewport, but close to it
        # can be rendered
        # otherwise edges of cells that intersects with viewport
        # won't be rendered
        lat_delta = 0.15
        lng_delta = 0.4
        if not alert:
            query = Weather.select().where(
                (Weather.latitude >= float(swLat) - lat_delta) &
                (Weather.longitude >= float(swLng) - lng_delta) &
                (Weather.latitude <= float(neLat) + lat_delta) &
                (Weather.longitude <= float(neLng) + lng_delta)
            ).dicts()
        else:
            query = Weather.select().where(
                (Weather.latitude >= float(swLat) - lat_delta) &
                (Weather.longitude >= float(swLng) - lng_delta) &
                (Weather.latitude <= float(neLat) + lat_delta) &
                (Weather.longitude <= float(neLng) + lng_delta) &
                (Weather.severity.is_null(False))
            ).dicts()
        weathers = []
        for w in query:
            weathers.append(w)

        return weathers


def db_updater():
    # The forever loop.

    max_queue_size = 0
    last_notify = time.time()
    while True:
        try:

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


def bulk_upsert_old(cls, data, db):
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
                    log.warning('%s', pprint.pformat(data))
                    time.sleep(1)
                    fails += 1
                    if fails > max_fails:
                        return
                    continue

            i += step


def bulk_upsert(cls, data, db):
    rows = data.values()
    num_rows = len(rows)
    i = 0

    max_fails = 3
    fails = 0

    # This shouldn't happen, ever, but anyways...
    if num_rows < 1:
        return

    # We used to support SQLite and it has a default max 999 parameters,
    # so we limited how many rows we insert for it.
    # Oracle: 64000
    # MySQL: 65535
    # PostgreSQL: 34464
    # Sqlite: 999
    step = 500

    # Prepare for our query.
    conn = db.get_conn()
    cursor = db.get_cursor()

    # We build our own INSERT INTO ... ON DUPLICATE KEY UPDATE x=VALUES(x)
    # query, making sure all data is properly escaped. We use
    # placeholders for VALUES(%s, %s, ...) so we can use executemany().
    # We use peewee's InsertQuery to retrieve the fields because it
    # takes care of peewee's internals (e.g. required default fields).
    query = InsertQuery(cls, rows=[rows[0]])
    # Take the first row. We need to call _iter_rows() for peewee internals.
    # Using next() for a single item is not considered "pythonic".
    first_row = {}
    for row in query._iter_rows():
        first_row = row
        break
    # Convert the row to its fields, sorted by peewee.
    row_fields = sorted(first_row.keys(), key=lambda x: x._sort_key)
    row_fields = map(lambda x: x.name, row_fields)
    # Translate to proper column name, e.g. foreign keys.
    db_columns = [peewee_attr_to_col(cls, f) for f in row_fields]

    # Store defaults so we can fall back to them if a value
    # isn't set.
    defaults = {}

    for f in cls._meta.fields.values():
        # Use DB column name as key.
        field_name = f.name
        field_default = cls._meta.defaults.get(f, None)
        defaults[field_name] = field_default

    # Assign fields, placeholders and assignments after defaults
    # so our lists/keys stay in order.
    table = '`' + conn.escape_string(cls._meta.db_table) + '`'
    escaped_fields = ['`' + conn.escape_string(f) + '`' for f in db_columns]
    placeholders = ['%s' for escaped_field in escaped_fields]
    assignments = ['{x} = VALUES({x})'.format(
        x=escaped_field
    ) for escaped_field in escaped_fields]

    # We build our own MySQL query because peewee only supports
    # REPLACE INTO for upserting, which deletes the old row before
    # adding the new one, giving a serious performance hit.
    query_string = ('INSERT INTO {table} ({fields}) VALUES'
                    + ' ({placeholders}) ON DUPLICATE KEY UPDATE'
                    + ' {assignments}')

    # Prepare transaction.

    with db.atomic():
        while i < num_rows:
            start = i
            end = min(i + step, num_rows)
            name = cls.__name__

            log.debug('Inserting items %d to %d for %s.', start, end, name)
            try:
                # Turn off FOREIGN_KEY_CHECKS on MySQL, because apparently it's
                # unable to recognize strings to update unicode keys for
                # foreign key fields, thus giving lots of foreign key
                # constraint errors.
                db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')

                # Time to bulk upsert our data. Convert objects to a list of
                # values for executemany(), and fall back to defaults if
                # necessary.
                batch = []
                batch_rows = rows[i:min(i + step, num_rows)]

                # We pop them off one by one so we can gradually release
                # memory as we pass each item. No duplicate memory usage.
                while len(batch_rows) > 0:
                    row = batch_rows.pop()
                    row_data = []

                    # Parse rows, build arrays of values sorted via row_fields.
                    for field in row_fields:
                        # Take a default if we need it.
                        if field not in row:
                            default = defaults.get(field, None)

                            # peewee's defaults can be callable, e.g. current
                            # time. We only call when needed to insert.
                            if callable(default):
                                default = default()

                            row[field] = default

                        # Append to keep the exact order, and only these
                        # fields.
                        row_data.append(row[field])
                    # Done preparing, add it to the batch.
                    batch.append(row_data)

                # Format query and go.
                formatted_query = query_string.format(
                    table=table,
                    fields=', '.join(escaped_fields),
                    placeholders=', '.join(placeholders),
                    assignments=', '.join(assignments)
                )

                cursor.executemany(formatted_query, batch)
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
                    log.warning('%s', pprint.pformat(data))
                    time.sleep(1)
                    fails += 1
                    if fails > max_fails:
                        return
                    continue

            i += step


def create_tables(db):
    tables = [Authorizations, Pokemon, Pokestop, Gym, GymDetails, GymMember,
              GymPokemon, Trainer, Raid, Versions, Weather]

    verify_database_schema(db)

    with db.execution_context():
        for table in tables:
            if not table.table_exists():
                log.info("Creating table: %s", table.__name__)
                db.create_tables([table], safe=True)
            else:
                log.debug('Skipping table %s, it already exists.',
                          table.__name__)

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


def drop_tables(db):
    tables = [Pokemon, Pokestop, Gym, GymDetails, GymMember,
              GymPokemon, Trainer, Raid, Versions, Weather]

    with db.execution_context():
        db.execute_sql('SET FOREIGN_KEY_CHECKS=0;')
        for table in tables:
            if table.table_exists():
                log.info("Dropping table: %s", table.__name__)
                db.drop_tables([table], safe=True)
        db.execute_sql('SET FOREIGN_KEY_CHECKS=1;')


def verify_database_schema(db):
    if not Versions.table_exists():
        db.create_tables([Versions])
        InsertQuery(Versions, {Versions.key: 'schema_version',
                               Versions.val: db_schema_version}
                    ).execute()
    else:
        db_ver = Versions.get(Versions.key == 'schema_version').val

        database_migrate(db, db_ver)

        # elif db_ver > db_schema_version:
        #     log.error('Your database version (%i) appears to be newer than '
        #               'the code supports (%i).', db_ver, db_schema_version)
        #     sys.exit(1)

# Going to try to do something a little different.
# Instead of using versioning, we're just going to check for specific
# columns, and add them as needed.

# we'll retain versions for major updates


def database_migrate(db, old_ver):
    # Update database schema version.

    if db_schema_version > old_ver:
        Versions.update(val=db_schema_version).where(
            Versions.key == 'schema_version').execute()

        log.info('Detected database version %i, updating to %i...',
                 old_ver, db_schema_version)

    # Perform migrations here.
    migrator = MySQLMigrator(db)

    if old_ver < 24:
        migrate(
            migrator.drop_index('pokemon', 'pokemon_disappear_time'),
            migrator.add_index('pokemon',
                               ('disappear_time', 'pokemon_id'), False)
        )

    table_updates = [
        # Old ver 17
        ('add_column', 'pokemon', 'form', SmallIntegerField(null=True)),
        # Old ver 18
        ('add_column', 'pokemon', 'cp', SmallIntegerField(null=True)),
        # old ver 19
        ('add_column', 'pokemon', 'cp_multiplier', FloatField(null=True)),
        # old ver 20
        ('drop_column', 'gym', 'gym_points', None),
        ('add_column', 'gym', 'slots_available',
         SmallIntegerField(null=False, default=0)),
        ('add_column', 'gymmember', 'cp_decayed',
         SmallIntegerField(null=False, default=0)),
        ('add_column', 'gymmember', 'deployment_time',
         DateTimeField(
             null=False, default=datetime.utcnow())),
        ('add_column', 'gym', 'total_cp',
         SmallIntegerField(null=False, default=0)),
        # old version 24
        ('drop_index', 'pokemon', 'disappear_time', None),
        ('add_index', 'pokemon', ('disappear_time', 'pokemon_id'), False),
        # newer stuff
        ('add_column', 'pokemon', 'costume', SmallIntegerField(null=True)),
        ('add_column', 'gympokemon', 'form', SmallIntegerField(null=True)),
        ('add_column', 'gympokemon', 'costume', SmallIntegerField(null=True)),
        ('add_column', 'gympokemon', 'shiny', SmallIntegerField(null=True)),
        ('add_column', 'pokemon', 'weather_boosted_condition',
         SmallIntegerField(null=True)),
        ('add_column', 'gym', 'park', BooleanField(default=False)),

        ('add_column', 'gym', 'sponsor', SmallIntegerField(null=True)),
    ]

    for change in table_updates:
        (action, table, data, ctype) = change
        if action == 'add_column':
            if not column_exists(db, table, data):
                log.info("Adding '%s' column to '%s'.", data, table)
                migrate(migrator.add_column(table, data, ctype))

        if action == 'drop_column':
            if column_exists(db, table, data):
                log.info("Dropping '%s' column from '%s'.", data, table)
                migrate(migrator.drop_column(table, data))

        if action == 'add_index':
            index = index_exists(db, table, data)
            if not index:
                log.info("Adding '%s' index to '%s'.", data, table)
                migrate(migrator.add_index(table, data, ctype))

        if action == 'drop_index':
            if index_name_exists(db, table, data):
                log.info("Dropping '%s' index from '%s'.", data, table)
                migrate(migrator.drop_index(table, data))


def column_exists(db, table, name):
    columns = db.get_columns(table)
    for column in columns:
        # it exists
        if column[0] == name:
            return True
    return False

# For adding an index, we need to check the index columns


def index_exists(db, table, index_cols):
    # convert to sets and compare
    idx = set(index_cols)
    indexes = db.get_indexes(table)
    for index in indexes:
        table_idx = set(index[2])
        # it exists
        if idx == table_idx:
            return index[0]
    return None

# for deleting an index, we need to check the index name


def index_name_exists(db, table, index_name):
    indexes = db.get_indexes(table)
    for index in indexes:
        # it exists
        if index_name == index[0]:
            return True
    return None
