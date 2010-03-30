from seafair import Data

class Phonebook(Data):
    name = True
    phone = None

Phonebook(name="Steve", phone="215-335-2224").save()
Phonebook(name="Bob", phone="100100100").save()

print Phonebook.find(name="Steve")
print Phonebook.find(name="Bob")
