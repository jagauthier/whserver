# Webhook server aggregator

This application's primary purpose is to receive webhook data from 
RocketMap, from multiple sources, and put them into a single 
database.  This can be a standalone database, or an existing 
RocketMap database.  

## Contributions ##

First and foremost, RocketMap has contributed a significant amount of
code to this project.  models.py, webhook.py, utils.py are all taken
directly from the RM source tree and retrofitted for this project.
webhook, and all the arguments that go along with is a direct copy
with very little modifications. utils.py is basically the same with 
different arguments for this projects.  Lastly, models.py
uses all of the same database models with substantial code removed 
that is not needed for this.  I also re-used the database migrator,
table creation, deletion, directly.  So big ___thanks___ to that team!

## What's the point of this ##

The project could meet some goals, and not others.  Some of these goals
could be met with database replication.  I don't know your objectives.
Maybe this project will work for them.

A ton of information is sent over webhooks.  RocketMap sends detailed 
pokemon info, gym information, gym details, pokestops (and some other
things we don't care about right now). All of the data it sends, is 
also stored in its database.  So, making a slimmed out copy of that
data, we can store the received webhook data into it.  

1) If you have multiple instances, with multiple databases you can send
them off to a third database, that aggregates them.

2) If you want to run the webfrontend from a different system and direct
mysql access is very slow over a WAN.

Quite arguably, the above two could acheived with database mirroring.
That's up to you.

3) You and several friends are all running scans in your area, or even 
disparate areas.  You can swap data to each other's databases.
Why would this not be acheived with database mirroring? Well, the
biggest point of all, is that RocketMap isn't designed to send webhooks
from database reads.  It scans the info and sends the hooks. If you are
swapping data with a DB mirror, you will never trigger additional
webhook sends. This means, you cannot use notifications.  However, this
project will also forward the receieved webhook, in tact, to the
webhook(s) of your choice. So you, and your friends, can send them to
your own PokeAlert, or whatever.



