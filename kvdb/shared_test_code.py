
from pydantic import BaseModel
from .kvdb import DB, NotFound, TooManyValues
from .sqlite import SqliteDBContract

import uuid

def mkid():
    return str(uuid.uuid4())

import pytest

def check_engine(db):

    class Person(BaseModel):
        id: str
        first_name: str
        last_name: str

    class Address(BaseModel):
        id: str
        street: str
        number: int

    db.add_collection(Person, "id")
    db.add_collection(Address, "id")

    assert len(db.c.Person.query().fetch_all()) == 0
    assert len(db.c.Address.query().fetch_all()) == 0    

    assert db.c.Person.query().one_or_none() is None
    with pytest.raises(NotFound):
        db.c.Person.query().one()

    john_id = mkid()
    address_obj = Address(id=mkid(), street="Indian Rd", number="10")
    db.put(Person(id=john_id, first_name="John", last_name="Doe"), address_obj, Person(id=mkid(), first_name="Jane", last_name="Doe"))

    assert db.c.Person.get(john_id).first_name == "John"
    assert db.c.Person.get("invalid") is None

    assert len(db.c.Person.query().fetch_all()) == 2
    assert len(db.c.Address.query().fetch_all()) == 1    

    assert db.c.Address.query().one() == address_obj
    assert db.c.Address.query().one_or_none() == address_obj
    with pytest.raises(TooManyValues):
        db.c.Person.query().one_or_none()

    assert len(db.c.Person.query().filter("last_name", "=", "Doe").fetch_all()) == 2
    assert len(db.c.Person.query().filter("first_name", "=", "Jane").fetch_all()) == 1 
    assert len(db.c.Person.query().filter("first_name", "=", "Steve").fetch_all()) == 0

    db.delete((Person, john_id))
    remaining = db.c.Person.query().fetch_all()
    assert len(remaining) == 1
    assert remaining[0].first_name == "Jane"
