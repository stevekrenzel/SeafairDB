from seafair import Data
from time import time

class CrazyList(Data):
    key = True

n = 2000000
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
