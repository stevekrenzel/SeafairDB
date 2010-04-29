# -*- coding: utf-8 -*-
# Seafair - A schemaless, persistent key-value store
# Author: Steve Krenzel
# License: MIT (See README for license details)

import os
import sqlite3
from base64 import b64encode
from os import makedirs
from os.path import exists, join
from struct import pack, unpack, calcsize
from hashlib import md5
from simplejson import dumps, loads

# Consistency constants
NO_GUARANTEE = 0
APP_GUARANTEE = 1
OS_GUARANTEE = 2

class Seafair:

    data_path = "./data/"
    data_file = "seafair.sea"
    con = None
    consistency_level = APP_GUARANTEE
    no_guarantee_delay = 100

    def __init__(self):
        if Seafair.con == None:
            if not exists(Seafair.data_path):
                makedirs(Seafair.data_path)
            filename = join(Seafair.data_path, Seafair.data_file)
            Seafair.con = sqlite3.connect(filename)
            if Seafair.consistency_level in [NO_GUARANTEE, APP_GUARANTEE]:
                Seafair.con.execute("pragma synchronous = OFF")
                Seafair.no_guarantee_count = Seafair.no_guarantee_delay
            else:
                Seafair.con.execute("pragma synchronous = FULL")
            Seafair.con.execute("CREATE TABLE IF NOT EXISTS seafair (k PRIMARY KEY, v)")

    def set(self, key_names, data, cls=""):
        # TODO Handle out of disk space errors
        # We append the class incase two models use the same key(s)
        key = md5("".join(str(data[i]) for i in sorted(key_names)) + cls)
        key_bytes = b64encode(key.digest())
        val_bytes = dumps(data)
        Seafair.con.execute("REPLACE INTO seafair VALUES (?, ?)",
                            (key_bytes, val_bytes))
        if Seafair.consistency_level == NO_GUARANTEE:
            Seafair.no_guarantee_count -= 1
            if Seafair.no_guarantee_count == 0:
                Seafair.con.commit()
                Seafair.no_guarantee_count = Seafair.no_guarantee_delay
        else:
            Seafair.con.commit()
        return True

    def get(self, key, cls=""):
        # Grab the keys and hash them
        key = md5("".join(str(key[i]) for i in sorted(key.keys())) + cls)
        key_bytes = b64encode(key.digest())
        val_bytes = Seafair.con.execute("SELECT v FROM seafair WHERE (k = ?)",
                                        (key_bytes,)).fetchone()
        if val_bytes:
            return loads(val_bytes[0])
        return None

    @classmethod
    def close(self):
        Seafair.con.commit()
        Seafair.con.close()

class Data:
    # TODO mulitple key groups?

    __db = None

    def __init__(self, **kwargs):
        if self.__class__.__db == None:
            self.__class__.__db = Seafair()
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
