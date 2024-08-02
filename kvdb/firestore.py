from google.cloud.firestore import Client

from typing import List, Optional, Dict, Any, Iterable
from .kvdb import Predicate, DeleteOperation, PutOperation

class FirestoreEngine:
    def __init__(self, database=None, project=None):
        self.client = Client(database=database, project=project)

    def get(self, collection_name : str, key : str) -> Optional[Dict[str, Any]]:
        snapshot = self.client.collection(collection_name).document(key).get()
        return snapshot.to_dict()
    
    def fetch_all(self, collection_name : str, filters: List[Predicate]) -> Iterable[Dict[str, Any]]:
        query = self.client.collection(collection_name)
        for filter in filters:
            # sanity check operator
            assert filter.operator in ("=", "!=", "<", ">", ">=", "<=")
            operator = filter.operator
            if operator == "=":
                operator = "=="
            query = query.where(filter.field, operator, filter.value)

        for document in query.stream():
            yield document.to_dict()

    def delete(self, operations: List[DeleteOperation]):
        for operation in operations:
            self.client.collection(operation.collection_name).document(operation.id).delete()

    def put(self, operations: List[PutOperation]):
        for operation in operations:
            self.client.collection(operation.collection_name).document(operation.id).set(operation.document)
