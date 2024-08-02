
from pydantic import BaseModel
from .kvdb import DB, NotFound, TooManyValues
from .sqlite import SqliteDBContract
from .shared_test_code import check_engine
import uuid

def mkid():
    return str(uuid.uuid4())

import pytest

def test_sqlite_engine(tmpdir):

    db_file = str(tmpdir.join("db.sqlite"))

    db = DB(SqliteDBContract(db_file))

    check_engine(db)
    