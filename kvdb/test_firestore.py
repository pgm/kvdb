
from .firestore import FirestoreEngine
from .shared_test_code import check_engine
from .kvdb import DB
from google.cloud.firestore import Client

def test_firestore_engine(tmpdir):
    database = 'test-kvdb'
    project = "depmap-gumbo"
    
    client = Client(database=database, project=project)

    # clean up any data from prev run. Test assumes db is empty
    client.recursive_delete(client.collection("Address"))
    client.recursive_delete(client.collection("Person"))

    db = DB(FirestoreEngine(database=database, project=project))
    check_engine(db)
