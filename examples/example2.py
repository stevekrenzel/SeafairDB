from seafair import *
from time import time

Seafair.consistency_level = NO_GUARANTEE

class CrazyList(Data):
    key = True

n = 10000
print "Printing existing values: "

t = time()
for i in xrange(n):
    g = CrazyList.find(key=i)
    if not g:
        print "AHHH", i
    #    print g.key
print time() - t

print "Adding values: "
t = time()
for i in xrange(n):
    if i % 1000 == 0:
        print "At ", i
    CrazyList(key=i).save()
print time() - t
