import pytest

from zephir_cluster import ZephirCluster

from mongoengine import connect, disconnect
from mongoengine import Document, StringField

class Person(Document):
    name = StringField(required=True)

class TestPerson():

    def setup_method(self):
        self.db = connect('testdb', host="mongomock://localhost", port=27017)

    def teardown_method(self):
        self.db.drop_database('testdb')
        self.db.close()

    def test_create(self):
        assert len(Person.objects) == 0
        person = Person(name="Mr. Test")
        person.save()
        assert len(Person.objects) == 1
        fresh_pers = Person.objects().first()
        assert fresh_pers.name ==  'Mr. Test'



