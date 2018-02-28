#!/usr/bin/env python
import logging
from threading import Thread
import threading

from models import db, db_updater, create_tables, drop_tables, \
    clean_db_loop, Authorizations, bulk_upsert
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

from SocketServer import ThreadingMixIn

import random
import string
from sets import Set
from webhook import wh_updater
from process import main_process, Auth, process_stats
import socket
import time
from utils import get_args, get_queues

logging.basicConfig(
    format='%(asctime)s [%(threadName)12s][%(module)8s][%(levelname)7s] ' +
    '%(message)s')

log = logging.getLogger()

args = get_args()
(db_queue, wh_queue, process_queue, stats_queue) = get_queues()


class ThreadHTTP(threading.Thread):
    def __init__(self, i, sock, handler, auth):
        threading.Thread.__init__(self)
        self.i = i
        self.daemon = True
        self.sock = sock
        self.handler = handler
        self.auth = auth
        self.start()

    def run(self):
        httpd = HTTPServer((args.host, args.port), self.handler, False)

        # Prevent the HTTP server from re-binding every handler.
        # https://stackoverflow.com/questions/46210672/
        httpd.socket = self.sock
        httpd.server_bind = self.server_close = lambda self: None
        httpd.RequestHandlerClass.auth = self.auth
        httpd.serve_forever()


class ThreadedServer(ThreadingMixIn, HTTPServer):

    request_queue_size = 2048

    def setauth(self, auth):
        self.auth = auth

    def serve_forever(self):
        self.RequestHandlerClass.auth = self.auth
        HTTPServer.serve_forever(self)

    def server_activate(self):
        self.socket.settimeout(3.0)
        self.socket.listen(self.request_queue_size)


class HTTPHandler(BaseHTTPRequestHandler):
    # Override the default finish() because
    # http://bugs.python.org/issue14574
    post_fails = 0
    post_success = 0

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
        log.debug("%s", format % args)

    def do_POST(self):
        # First check if the path is an accepted value
        # TODO: Implement this
        if self.auth.validate(self.path) is False:
            try:
                self.send_response(404)
                self.end_headers()
            except:
                pass
            self.post_fails += 1
            return
        data_string = self.rfile.read(int(self.headers['Content-Length']))

        try:
            self.send_response(200)
            self.end_headers()
        except:
            pass
        self.post_success += 1
        # Put it in the process queue
        process_queue.put(data_string)


def validate_args():
    if args.clear_db:
        drop_tables(db)
        create_tables(db)
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


def launch_httpd(httpd, auth):

    log.info("Server Starts - %s:%s", args.host, args.port)
    httpd.setauth(auth)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        raise

    httpd.server_close()
    log.info("Server Stops - %s:%s", args.host, args.port)


def launch_threaded_httpd(handler, auth):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((args.host, args.port))
    sock.listen(5)
    log.info("Launching HTTP server - %s:%s", args.host, args.port)
    [ThreadHTTP(i, sock, handler, auth) for i in range(args.httpd_threads)]
    time.sleep(9e9)
    log.info("Server Stops - %s:%s", args.host, args.port)


if __name__ == '__main__':

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

    if not args.clear_db:
        create_tables(db)
    # If we're doing certain things, we'll do them and Then
    # quit
    validate_args()

    # Thread(s) to process database updates.
    # I won't take credit for this. This is straight from RocketMap
    # But if we're getting thrashed with multiple webhook senders
    # Then this seems important, and RM handles this so well
    for i in range(args.db_threads):
        log.debug('Starting db-updater worker thread %d', i)
        t = Thread(target=db_updater, name='db-updater-{}'.format(i))
        t.daemon = True
        t.start()

    # start the db-cleaner
    t = Thread(target=clean_db_loop, name='db-cleaner')
    t.daemon = True
    t.start()

    if args.runtime_statistics:
        log.debug("Starting thread for statistics.")
        t = Thread(target=process_stats, name='proc-stats')
        t.daemon = True
        t.start()

    # starting web hook server threads
    for i in range(args.wh_threads):
        log.debug('Starting wh-updater worker thread %d', i)
        t = Thread(target=wh_updater, name='wh-updater-{}'.format(i))
        t.daemon = True
        t.start()

    for i in range(args.process_threads):
        log.debug('Starting main process worker thread %d', i)
        t = Thread(target=main_process, name='process-{}'.format(i))
        t.daemon = True
        t.start()

    # Start authorization thread
    auth = Auth()

    # Start HTTP server
    if args.safe_httpd:
        httpd = ThreadedServer((args.host, args.port), HTTPHandler)
        launch_httpd(httpd, auth)
    else:
        launch_threaded_httpd(HTTPHandler, auth)
