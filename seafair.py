# -*- coding: utf-8 -*-
# Seafair - A schemaless, persistent key-value store
# Author: Steve Krenzel
# License: MIT (See README for license details)
# TODO Implement no_guarantee cache
# TODO Implement app guarantee cache
# TODO Implement os_guarantee cache
# TODO Move to a single db file
# TODO Create data_path if not exists
# TODO Move to RESTful service
# TODO Handle out of disk space errors

import os
import sqlite3
from base64 import b64encode
from os import makedirs
from os.path import exists, join
from struct import pack, unpack, calcsize
from hashlib import md5
from simplejson import dumps, loads
from time import time


class Seafair:

    data_path = "./data/"

    def __init__(self, filename):
        if not exists(Seafair.data_path):
            makedirs(Seafair.data_path)
        filename = join(Seafair.data_path, filename)
        self.con = sqlite3.connect(filename)
        self.con.execute("pragma synchronous = off")
        self.con.execute("CREATE TABLE IF NOT EXISTS seafair (k PRIMARY KEY, v)")

    def set(self, key_names, data, cls=""):
        # We append the class incase two models use the same key(s)
        key = md5("".join(str(data[i]) for i in sorted(key_names)) + cls)
        key_bytes = b64encode(key.digest())
        val_bytes = dumps(data)
        self.con.execute("REPLACE INTO seafair VALUES (?, ?)",
                         (key_bytes, val_bytes))
        self.con.commit()
        return True


    def get(self, key, cls=""):
        # Grab the keys and hash them
        key = md5("".join(str(key[i]) for i in sorted(key.keys())) + cls)
        key_bytes = b64encode(key.digest())
        val_bytes = self.con.execute("SELECT v FROM seafair WHERE (k = ?)",
                                     (key_bytes,)).fetchone()
        if val_bytes:
            return loads(val_bytes[0])
        return None

    def close(self):
        self.con.close()

class Data:
    # TODO mulitple key groups?

    __db = None

    def __init__(self, **kwargs):
        if self.__class__.__db == None:
            self.__class__.__db = Seafair(self.__class__.__name__ + ".sea")
            self.update_keys()
        self.__dict__.update(kwargs)

    def update_keys(self):
        c = self.__class__
        c.__keys = [k for k, v in c.__dict__.items() if v == True]

    @classmethod
    def find(cls, **kwargs):
        if cls.__db == None:
            cls()
        json = cls.__db.get(kwargs, cls.__name__)
        if json:
            val = dict((str(k), v) for k, v in json.items())
            return cls(**val)
        return None

    def save(self):
        c = self.__class__
        return c.__db.set(c.__keys, self.__dict__, c.__name__)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        ret = "".encode('utf-8')
        ret += u"%s:\n"%(self.__class__.__name__)
        ret += u"\n".join(u"    %s: %s" % (k, v) for k, v in
                sorted(self.__dict__.items(), lambda a, b: cmp(a[0], b[0])))
        return unicode(ret + "\n")

class Twitter(Data):
    name = True
    tweet = None

if __name__ == "__main__":
    o = 0
    n = 3000
    t = time()
    tweet = "2" * 140
    for i in xrange(o, n):
        Twitter(name=i, tweet=tweet).save()
    print time() - t
    t = time()
    for i in xrange(o, n):
        p = Twitter.find(name=i)
        if not p or p.tweet != tweet or p.name != i:
            print "UGH ", i
    print time() - t
