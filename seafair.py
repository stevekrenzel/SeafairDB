# Seafair - A schemaless, persistent key-value store
# Author: Steve Krenzel
# License: MIT (See README for license details)

from os.path import exists
from struct import pack, unpack, calcsize
from hashlib import md5
from simplejson import dumps, loads
from time import time
from random import shuffle

class Seafair:
    def __init__(self, filename):
        # We use a larger sector to increase space efficiency
        self.sector = 512 * 4

        # This is the format for hash + address + length of data
        self.entry_fmt = 'qqqq'
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
        self.fobj.seek(0)
        self.fobj.write(pack('q' * 64, *([0] * 64)))
        self.read_ptrs()

    def add_table(self):
        # Seek to the end of the file
        self.fobj.seek(0, 2)
        addr = self.fobj.tell()

        # Calculate the size of the new table, and allocate space for it
        size = (2**len(self.ptrs)) * self.sector
        self.write_empty_space(size)
        self.tot = (size / self.entry_sz)

        # Add our new address to the front of our ptr list and write it
        self.ptrs = [addr] + self.ptrs
        self.write_ptrs()

        # Update sizes and range
        self.update_sizes_and_ranges()

    def write_empty_space(self, byte_cnt):
        # Seek to the end of the file
        self.fobj.seek(0, 2)

        # We write a megabyte at a time
        # TODO: We might want to start with larger chunks
        block_size = 1024 ** 2

        # We create a block of null data
        zeroed_block  = chr(0) * block_size

        # Write the blocks to disk
        for i in xrange(byte_cnt / block_size):
            self.fobj.write(zeroed_block)

        # The last block won't likely be a full block, so we create a
        # special block for the last one.
        zeroed_block = chr(0) * (byte_cnt % block_size)
        self.fobj.write(zeroed_block)

    def write_ptrs(self):
        # We seek to the start of the file and write out our ptrs
        self.fobj.seek(0)
        self.fobj.write(pack('q' * len(self.ptrs), *self.ptrs))

    def read_ptrs(self):
        # We seek to the start of the file and read in our ptrs
        self.fobj.seek(0)
        bytes = self.fobj.read(calcsize('q' * 64))

        # Filter out any pointers that point to 0
        self.ptrs = [i for i in unpack('q' * 64, bytes) if i > 0]

        # Update sizes and range
        self.update_sizes_and_ranges()

    def update_sizes_and_ranges(self):
        n = len(self.ptrs)
        s = lambda x: (2**(n - x - 1)) * self.sector
        r = lambda x: ((x - self.sector)/ self.entry_sz) + 1
        self.sizes = map(s, range(n))
        self.ranges =  map(r, self.sizes)

    def find_entry(self, entry, data):
        # TODO Can we safely just do "return data.find(entry)" ? I think so
        # TODO Cuckoo hashing?
        i = data.find(entry)
        while i != -1:
            if i % self.entry_sz == 0:
                return i
            i = data.find(entry, i + 1)
        return None

    def set(self, key_names, data):
        # Grab the keys and hash them
        # TODO make key a dict
        # TODO We get knocked twice for the digest stuff... what can we do about this?
        # TODO Can we minimize seeks and/or reads?
        key  = md5("".join(str(data[i]) for i in sorted(key_names)))
        hsh  = int(key.hexdigest(), 16)
        slot = hsh % self.ranges[0]

        # Seek to EOF and get the address
        self.fobj.seek(0,2)
        addr = self.fobj.tell()

        # Encode the data and store its size
        json = dumps(data)
        size = len(json)

        # Write the data to disk
        self.fobj.write(json)

        # Seek to the slot and find out where to save everything
        start  = self.ptrs[0] + (slot * self.entry_sz)
        self.fobj.seek(start)

        # Read in data to search
        bytes = self.fobj.read(self.sector)

        # Find a place in the data to insert our key. First we see if the
        # key already exists, and if so simply overwrite it. Otherwise we
        # find the first empty spot and write our data there.
        key_bytes = key.digest()
        for b in [key_bytes, self.null_entry]:
            i = self.find_entry(b, bytes)
            if i != None:
                self.fobj.seek(start + i)
                self.fobj.write(key_bytes + pack('qq', addr, size))
                break
        else:
            self.add_table()
            self.set(key_names, data)

    def get(self, key):
        # Grab the keys and hash them
        key  = md5("".join(str(key[i]) for i in sorted(key.keys())))
        hsh  = int(key.hexdigest(), 16)
        key_bytes = key.digest()
        for i in xrange(len(self.ptrs)):
            slot = hsh % self.ranges[i]

            # Seek to the slot and find out where to search
            start  = self.ptrs[i] + (slot * self.entry_sz)
            self.fobj.seek(start)

            # Read in data to search
            bytes = self.fobj.read(self.sector)

            i = self.find_entry(key_bytes, bytes)
            if i != None:
                h1, h2, addr, size = unpack(self.entry_fmt,
                                            bytes[i : i + self.entry_sz])
                self.fobj.seek(addr)
                return loads(self.fobj.read(size))

    def close(self):
        self.fobj.close()

if __name__ == "__main__":
    # TODO Try unicode
    s = Seafair("test.sea")
    r = range(50000)
    shuffle(r)
    t = time()
    for i in r:
        d = {"hello": i, "Goodbye": i}
        s.set(["hello"], d)
    print time() - t
    s.close()

    s = Seafair("test.sea")
    shuffle(r)
    t = time()
    for i in r:
        k = {"hello": i}
        g = s.get(k)
        if g["hello"] != i and g["Goodbye"] != i:
            print "FU ", i
    print time() - t
