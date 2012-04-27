CouchDB Dump Script
===================

This is a refactor of the couchdb-dump app that comes with python-couchdb.
This is meant to handle large databases and stream the output in
smaller segments.

Tested on:

  * Ubuntu 10.04
  * Python 2.6
  * CouchDB 1.1.1

Installing dependencies:

    $ sudo apt-get install libyajl-dev libyajl1
    $ sudo pip install ijson
    $ sudo pip install requests
    $ sudo pip install CouchDB

To run:

    $ ./couchdb-dump.py http://localhost:5984/some_database_name

