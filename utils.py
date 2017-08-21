import sys
import os
import configargparse
from queue import Queue


def memoize(function):
    memo = {}

    def wrapper(*args):
        if args in memo:
            return memo[args]
        else:
            rv = function(*args)
            memo[args] = rv
            return rv
    return wrapper


@memoize
def get_queues():
    db_queue = Queue()
    wh_queue = Queue()
    process_queue = Queue()
    stats_queue = Queue()
    return (db_queue, wh_queue, process_queue, stats_queue)


@memoize
def get_args():

    # Pre-check to see if the -cf or --config flag is used on the command line.
    # If not, we'll use the env var or default value. This prevents layering of
    # config files as well as a missing config.ini.
    defaultconfigfiles = []
    if '-cf' not in sys.argv and '--config' not in sys.argv:
        defaultconfigfiles = [os.getenv('WHSRV_CONFIG', os.path.join(
            os.path.dirname(__file__), './config/config.ini'))]
    parser = configargparse.ArgParser(
        default_config_files=defaultconfigfiles,
        auto_env_var_prefix='WHSRV_')
    parser.add_argument('-cf', '--config',
                        is_config_file=True, help='Set configuration file')
    parser.add_argument('-H', '--host',
                        help='Set web server listening host. ' +
                        'default = 0.0.0.0',
                        default='0.0.0.0')
    parser.add_argument('-P', '--port', type=int,
                        help='Set web server listening port. ' +
                        'default=5001', default=5001)
    parser.add_argument('--db-type',
                        help='Type of database to be used (default: mysql).',
                        default='mysql')
    parser.add_argument('-cd', '--clear-db',
                        help='Clear the database tables, and recreate. ' +
                        'Does not delete the authorizations.',
                        action='store_true', default=False)
    parser.add_argument('--db-name', help='Name of the database to be used.')
    parser.add_argument('--db-user', help='Username for the database.')
    parser.add_argument('--db-pass', help='Password for the database.')
    parser.add_argument('--db-host', help='IP or hostname for the database.')
    parser.add_argument(
        '--db-port', help='Port for the database.', type=int, default=3306)
    parser.add_argument('--db-max_connections',
                        help='Max connections (per thread) for the ' +
                        ' database. default = 5', type=int, default=5)
    parser.add_argument('--db-threads',
                        help=('Number of db threads; increase if the db ' +
                              'queue falls behind.'),
                        type=int, default=2)
    parser.add_argument('-g', '--generate', help='Generate an authorization ' +
                        'token. An identifying string is required.')
    ignore_list = parser.add_mutually_exclusive_group()
    ignore_list.add_argument('-ig', '--ignore-pokemon',
                             action='append', default=[],
                             help=('List of Pokemon to ignore when ' +
                                   'received.'))
    parser.add_argument('-l', '--list', help='List authorization tokes.',
                        action='store_true', default=False)
    parser.add_argument('-np', '--no-pokemon',
                        help='Pokemon will not be stored in the database.',
                        action='store_true', default=False)
    parser.add_argument('-nk', '--no-pokestops',
                        help='Pokestops will not be stored in the database.',
                        action='store_true', default=False)
    parser.add_argument('-ng', '--no-gyms',
                        help='Gyms will not be stored in the database.',
                        action='store_true', default=False)
    parser.add_argument('-ngd', '--no-gymdetail',
                        help='Gym detail will not be stored in the database.',
                        action='store_true', default=False)
    parser.add_argument('-nr', '--no-raids',
                        help='Raids will not be stored in the database.',
                        action='store_true', default=False)
    parser.add_argument('-rs', '--runtime-statistics',
                        help='Display usage statistics. Specified in ' +
                        'minutes', type=int, default=0)
    parser.add_argument('-pd', '--purge-data',
                        help=('Clear Pokemon from database this many hours ' +
                              'after they disappear (0 to disable).'),
                        type=int, default=0)
    parser.add_argument('-pi', '--pokemon-inserts',
                        help='Number of pokemon to commit ' +
                        ' to the DB at once. Bulk inserts are faster than ' +
                        'singles.', default=1, type=int)
    parser.add_argument('--process-threads',
                        help=('Number of main workers threads; ' +
                              'increase if the queue falls behind.'),
                        type=int, default=3)
    parser.add_argument('-r', '--revoke', help='Revoke an authorization ' +
                        'token. An identifying string is required.')
    parser.add_argument('-wh', '--webhook',
                        help='Define URL(s) to POST webhook information to.',
                        default=None, dest='webhooks', action='append')
    parser.add_argument('-whr', '--wh-retries',
                        help=('Number of times to retry sending webhook ' +
                              'data on failure.'),
                        type=int, default=3)
    parser.add_argument('-whbf', '--wh-backoff-factor',
                        help=('Factor (in seconds) by which the delay ' +
                              'until next retry will increase.'),
                        type=float, default=0.25)
    parser.add_argument('--wh-threads',
                        help=('Number of webhook threads; increase if the ' +
                              'webhook queue falls behind.'),
                        type=int, default=1)
    parser.add_argument('-whc', '--wh-concurrency',
                        help=('Async requests pool size.'), type=int,
                        default=25)
    parser.add_argument('-wht', '--wh-timeout',
                        help='Timeout (in seconds) for webhook requests.',
                        type=float, default=1.0)
    parser.add_argument('-whlfu', '--wh-lfu-size',
                        help='Webhook LFU cache max size.', type=int,
                        default=2500)
    parser.add_argument('-whfi', '--wh-frame-interval',
                        help=('Minimum time (in ms) to wait before sending the'
                              + ' next webhook data frame.'), type=int,
                        default=500)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose',
                           help=('Show debug messages. ' +
                                 'Optionally specify file to log to.'),
                           nargs='?', const='nofile', default=False,
                           metavar='filename.log')
    parser.set_defaults(DEBUG=False)

    args = parser.parse_args()

    if None in (args.db_type, args.db_name, args.db_user,
                args.db_pass, args.db_host):
        parser.print_usage()
        print(sys.argv[0] + ": DB info is not set correctly.")
        exit(1)

    return args
