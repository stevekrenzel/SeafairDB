# -*- coding: utf-8 -*-
# Seafair - A schemaless, persistent key-value store
# Author: Steve Krenzel
# License: MIT (See README for license details)

from os.path import exists, join
from struct import pack, unpack, calcsize
from hashlib import md5
from simplejson import dumps, loads
from time import time
from random import shuffle

class Seafair:

    data_path = "./data/"
    fobj = None

    def __init__(self, filename):
        # Set the filename to the data path
        filename = join(Seafair.data_path, filename)

        # We use a larger sector to increase space efficiency
        self.sector = 512 * 4

        # This is the format for hash + address + length of data
        self.entry_fmt = 'QQQI'
        self.entry_sz = calcsize(self.entry_fmt)
        self.null_entry = pack(self.entry_fmt, 0, 0, 0, 0)

        # Init the file if it doesn't exist
        if not exists(filename):
            open(filename, 'w').close()
            self.fobj = open(filename, 'r+b')
            self.init_ptrs()
            self.add_table()
            self.fobj.close()

        self.fobj = open(filename, 'r+b')
        self.read_ptrs()

    def init_ptrs(self):
        # Write 64 null pointers to the start of the file
        self.write(0, pack('Q' * 64, *([0] * 64)))
        self.read_ptrs()

    def add_table(self):
        # Get the end of the file
        addr = self.get_eof()

        # Calculate the size of the new table, and allocate space for it
        size = (2**len(self.ptrs)) * self.sector
        self.write_empty_space(size)
        self.tot = (size / self.entry_sz)

        # Add our new address to the front of our ptr list and write it
        self.ptrs = [addr] + self.ptrs
        self.write(0, pack('Q' * len(self.ptrs), *self.ptrs))

        # Update sizes and range
        self.update_sizes_and_ranges()

    def write_empty_space(self, byte_cnt):
        # We write 4 megabytes at a time
        block_size = 4 * 1024 * 1024

        # We create a block of null data
        zeroed_block  = chr(0) * block_size

        # Seek to the end of the file
        self.fobj.seek(0, 2)
        # Write the blocks to disk
        for i in xrange(byte_cnt / block_size):
            self.fobj.write(zeroed_block)

        # The last block won't likely be a full block, so we create a
        # special block for the last one.
        zeroed_block = chr(0) * (byte_cnt % block_size)
        self.fobj.write(zeroed_block)

    def read_ptrs(self):
        # We read in our ptrs
        bytes = self.read(0, calcsize('Q' * 64))

        # Filter out any pointers that point to 0
        self.ptrs = [i for i in unpack('Q' * 64, bytes) if i > 0]

        # Update sizes and range
        self.update_sizes_and_ranges()

    def update_sizes_and_ranges(self):
        n = len(self.ptrs)
        s = lambda x: (2**(n - x - 1)) * self.sector
        r = lambda x: ((x - self.sector)/ self.entry_sz) + 1
        self.sizes = map(s, range(n))
        self.ranges =  map(r, self.sizes)

    def find_entry(self, entry, data):
        i = data.find(entry)
        while i != -1:
            if i % self.entry_sz == 0:
                return i
            i = data.find(entry, i + 1)
        return None

    def set(self, key_names, data, cls="", addr=None, size=None):
        # Grab the keys and hash them
        # We append the class incase two models use the same key(s)
        # TODO When you make the Data class, store an optional link to previous version?
        key  = md5("".join(str(data[i]) for i in sorted(key_names)) + cls)
        hsh  = int(key.hexdigest(), 16)
        slot = hsh % self.ranges[0]

        # If we already wrote this data, no need to write it again
        if addr == None:
            # Seek to EOF and get the address
            addr = self.get_eof()

            # Encode the data and store its size
            json = dumps(data)
            size = len(json)

            # Write the data to disk
            self.fobj.write(json)

        # Read in data to search
        start  = self.ptrs[0] + (slot * self.entry_sz)
        bytes = self.read(start, self.sector)

        # Find a place in the data to insert our key. First we see if the
        # key already exists, and if so simply overwrite it. Otherwise we
        # find the first empty spot and write our data there.
        key_bytes = key.digest()
        for b in [key_bytes, self.null_entry]:
            i = self.find_entry(b, bytes)
            if i != None:
                self.write(start + i, key_bytes + pack('QI', addr, size))
                break
        else:
            self.add_table()
            self.set(key_names, data, cls, addr, size)

    def get(self, key, cls=""):
        # Grab the keys and hash them
        key  = md5("".join(str(key[i]) for i in sorted(key.keys())) + cls)
        hsh  = int(key.hexdigest(), 16)
        key_bytes = key.digest()
        for i in xrange(len(self.ptrs)):
            slot = hsh % self.ranges[i]

            # Read in data to search
            start  = self.ptrs[i] + (slot * self.entry_sz)
            bytes = self.read(start, self.sector)

            i = self.find_entry(key_bytes, bytes)
            if i != None:
                h1, h2, addr, size = unpack(self.entry_fmt,
                                            bytes[i : i + self.entry_sz])
                return loads(self.read(addr, size))

    def write(self, addr, data):
        self.fobj.seek(addr)
        self.fobj.write(data)

    def read(self, addr, size):
        self.fobj.seek(addr)
        return self.fobj.read(size)

    def get_eof(self):
        self.fobj.seek(0, 2)
        return self.fobj.tell()

    def close(self):
        self.fobj.close()

class Data:
    # TODO mulitple key groups?

    __db = None

    def __init__(self, **kwargs):
        if self.__class__.__db == None:
            self.__class__.__db = Seafair(self.__class__.__name__ + ".sea")
        self.__dict__.update(kwargs)
        self.update_keys()

    def update_keys(self):
        c = self.__class__
        c.__keys = [k for k, v in c.__dict__.items() if v == True]

    @classmethod
    def find(cls, **kwargs):
        val = cls.__db.get(kwargs, cls.__name__)
        if val:
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

class Phonebook(Data):
    name = True
    phone = None

if __name__ == "__main__":
    o = 0
    n = 300000
    t = time()
    for i in xrange(o, n):
        Phonebook(name=i, phone=i).save()
    print time() - t
    t = time()
    for i in xrange(o, n):
        p = Phonebook.find(name=i)
        if not p:
            print "UGH ", i
    print time() - t
