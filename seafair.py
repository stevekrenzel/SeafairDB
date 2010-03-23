# Seafair - A schemaless, persistent key-value store
# Author: Steve Krenzel
# License: MIT (See README for license details)

from os.path import exists
from struct import pack, unpack, calcsize
from hashlib import md5
from simplejson import dumps, loads

class Seafair:
    def __init__(self, filename):
        # We assume sectors are 512 bytes
        self.sector = 512

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
        i = data.find(entry)
        while i != -1:
            if i % self.entry_sz == 0:
                return i
            i = data.find(entry, i + 1)
        return None

    def set(self, key_names, data):
        # Grab the keys and hash them
        key  = md5("".join(str(data[i]) for i in sorted(key_names)))
        hsh  = int(key.hexdigest(), 16)
        slot = hsh % self.ranges[0]

        # Seek to EOF and get the address
        self.fobj.seek(0,2)
        addr = self.fobj.tell()

        # Encode the data and store its size
        data = dumps(data)
        size = len(data)

        # Write the data to disk
        self.fobj.write(data)

        # Seek to the slot and find out where to save everything
        start  = self.ptrs[0] + (slot * self.entry_sz)
        self.fobj.seek(start)
        bytes = self.fobj.read(self.sector)

        key_bytes = key.digest() + pack('qq', addr, size)

        for b in [key_bytes, self.null_entry]:
            i = self.find_entry(b, bytes)
            if i != None:
                print "WRITING ", (start + i)
                print "KEY ", key_bytes
                self.fobj.seek(start + i)
                self.fobj.write(key_bytes)
                break
        else:
            self.add_table()
            self.set(key_names, data)
        return True

if __name__ == "__main__":
    s = Seafair("test.sea")
    d = {"hello" : 123, "Test" : "Steve"}
    s.set(["hello"], d)
