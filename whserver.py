#!/usr/bin/env python
from queue import Queue
import logging
from threading import Thread
from utils import get_args
from models import db, db_updater, create_tables, drop_tables, \
    clean_db_loop, Authorizations, bulk_upsert
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import random
import string
from sets import Set
from webhook import wh_updater
from process import ProcessHook
import socket

logging.basicConfig(
    format='%(asctime)s [%(threadName)12s][%(module)8s][%(levelname)7s] ' +
    '%(message)s')

log = logging.getLogger()


class Server(HTTPServer):

    def serve_forever(self, args):
        self.RequestHandlerClass.args = args
        self.RequestHandlerClass.PH = ProcessHook(args)
        HTTPServer.serve_forever(self)


class HTTPHandler(BaseHTTPRequestHandler):
    args = None
    PH = None
    # Override the default finish() because
    # http://bugs.python.org/issue14574

    def finish(self, *args, **kw):
        try:
            if not self.wfile.closed:
                self.wfile.flush()
                self.wfile.close()
        except socket.error:
            pass
        self.rfile.close()

    def handle(self):
        try:
            BaseHTTPRequestHandler.handle(self)
        except socket.error:
            pass

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        # First check if the path is an accepted value
        # TODO: Implement this
        if self.PH.auth.validate(self.path) is False:
            try:
                self.send_response(404)
                self.end_headers()
            except:
                pass
            self.PH.post_fails += 1
            return

        data_string = self.rfile.read(int(self.headers['Content-Length']))
        try:
            self.send_response(200)
            self.end_headers()
        except:
            pass
        self.PH.post_success += 1

        # Do the rest of the work in a thread
        t = Thread(target=self.PH.process_post, name='process-post',
                   args=(self.args, db_updates_queue,
                         wh_updates_queue, data_string))
        t.daemon = True
        t.start()


def validate_args(args):
    if args.clear_db:
        drop_tables(db)
        create_tables(args, db)
        log.info("Drop and create complete.")
        exit(0)

    if args.list:
        print "--- Authorization keys: ---"
        query = Authorizations.select(Authorizations.token,
                                      Authorizations.name)
        for token in query:
            print ("Token: " + token.token + "\tAssigned: " +
                   token.name)
        exit(0)
    if args.generate:
        query = Authorizations.select(Authorizations.token).where(
            Authorizations.name ==
            args.generate)
        if len(query):
            print "Token already exists for " + args.generate
            exit(0)

        print "Generating authorization token for " + args.generate
        lst = [random.choice(string.ascii_letters + string.digits)
               for n in xrange(32)]
        token = "".join(lst)
        authorization = {'token': token, 'name': args.generate}
        bulk_upsert(Authorizations, {0: authorization}, db)
        print ("Authorization token for " + args.generate + " is: " +
               token)
        exit(0)

    if args.revoke:
        query = Authorizations.delete().where(
            Authorizations.token ==
            args.revoke)
        if query.execute():
            print "Token revoked."
        else:
            print "No token found."
        exit(0)
    if args.no_gyms:
        args.no_gymdetail = True

    args.ignore_pokemon = Set([int(i) for i in args.ignore_pokemon])


if __name__ == '__main__':

    args = get_args()

    # Add file logging if enabled.
    if args.verbose and args.verbose != 'nofile':
        filelog = logging.FileHandler(args.verbose)
        filelog.setFormatter(logging.Formatter(
            '%(asctime)s [%(threadName)16s][%(module)14s][%(levelname)8s] ' +
            '%(message)s'))
        logging.getLogger('').addHandler(filelog)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    create_tables(args, db)
    # If we're doing certain things, we'll do them and Then
    # quit
    validate_args(args)

    # DB updates
    db_updates_queue = Queue()
    # Thread(s) to process database updates.
    # I won't take credit for this. This is straight from RocketMap
    # But if we're getting thrashed with multiple webhook senders
    # Then this seems important, and RM handles this so well
    for i in range(args.db_threads):
        log.debug('Starting db-updater worker thread %d', i)
        t = Thread(target=db_updater, name='db-updater-{}'.format(i),
                   args=(args, db_updates_queue, db))
        t.daemon = True
        t.start()

    # start the db cleaner - doesn't do much here
    # resets pokestops...
    t = Thread(target=clean_db_loop, name='db-cleaner', args=(args,))
    t.daemon = True
    t.start()

    # WH updates queue & WH unique key LFU caches.
    # The LFU caches will stop the server from resending the same data an
    # infinite number of times. The caches will be instantiated in the
    # webhook's startup code.
    wh_updates_queue = Queue()
    wh_key_cache = {}

    # starting web hook server threads
    for i in range(args.wh_threads):
        log.debug('Starting wh-updater worker thread %d', i)
        t = Thread(target=wh_updater, name='wh-updater-{}'.format(i),
                   args=(args, wh_updates_queue, wh_key_cache))
        t.daemon = True
        t.start()

    server = Server
    httpd = server((args.host, args.port), HTTPHandler)
    log.info("Server Starts - %s:%s", args.host, args.port)
    try:
        httpd.serve_forever(args)
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    log.info("Server Stops - %s:%s", args.host, args.port)
