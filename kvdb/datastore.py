from google.cloud import datastore  # noqa: I100
from typing import List, Optional, Dict, Any, Iterable
from .kvdb import Predicate, DeleteOperation, PutOperation

def _entity_to_dict(entity: datastore.Entity):
    new_dict = {}

    def coerc_value(v):
        if isinstance(v, str) or isinstance(v, int) or isinstance(v, float) or isinstance(v, bool) or v is None:
            return v
        elif isinstance(v, list):
            return [coerc_value(x) for x in v]
        elif isinstance(v, datastore.Entity):
            return _entity_to_dict(v)
        else:
            raise Exception(f"Unhandled type: {repr(v)}")

    for k, v in entity.items():
        new_dict[k] = coerc_value(v)

    return new_dict

def _dict_to_entity(collection_name : str , id : str, document : Dict[str, Any]):
    entity = datastore.Entity(id)
    for k, v in document.items():
        entity[k] = v
    return entity

class DatastoreEngine:
    def __init__(self):
        self.client = datastore.Client()

    def get(self, collection_name : str, key : str) -> Optional[Dict[str, Any]]:
        key_ = self.client.key(collection_name, key)
        entity = self.client.get(key_)
        return _entity_to_dict(entity)
    
    def fetch_all(self, collection_name : str, filters: List[Predicate]) -> Iterable[Dict[str, Any]]:
        query = self.client.query(kind=collection_name)
        for filter in filters:
            # sanity check operator
            assert filter.operator in ("=", "!=", "<", ">", ">=", "<=")
            query = query.add_filter(filter.field, filter.operator, filter.value)

        for entity in query.fetch():
            yield _entity_to_dict(entity)

    def delete(self, operations: List[DeleteOperation]):
        self.client.delete_multi([self.client.key(op.collection_name, op.id) for op in operations])

    def put(self, operations: List[PutOperation]):
        self.client.put_multi([ _dict_to_entity(op.collection_name, op.id, op.document) for op in operations])
