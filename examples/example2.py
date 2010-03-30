from seafair import Data

class CrazyList(Data):
    key = True

print "Printing existing values: "

for i in xrange(100000):
    g = CrazyList.find(key=i)
    if g:
        print g.key

print "Adding values: "
for i in xrange(100000):
    if i % 1000 == 0:
        print "At ", i
    CrazyList(key=i).save()
