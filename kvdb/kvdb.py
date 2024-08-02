from dataclasses import dataclass
from typing import List, Tuple, Type, TypeVar, Iterable, Optional, Dict, Any
from pydantic import BaseModel, Field, Json

@dataclass
class PutOperation:
    collection_name: str
    id: str
    document: Dict[str, Any]

@dataclass
class DeleteOperation:
    collection_name: str
    id: str

@dataclass
class Predicate:
    field: str
    operator : str
    value : str

class TooManyValues(Exception):
    pass

class NotFound(Exception):
    pass

T = TypeVar("T")

class _InternalDBContract:
    "DB connection which provides these basic operations and operates on the level of dictionaries"
    def get(self, collection_name : str, key : str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()
    
    def fetch_all(self, collection_name : str, filters: List[Predicate]) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError()

    def delete(self, operations: List[DeleteOperation]):
        raise NotImplementedError()

    def put(self, operations: List[PutOperation]):
        raise NotImplementedError()


class Collection:
    def __init__(self, db : _InternalDBContract, collection_name : str, pydantic_class : Type, pk_field : str):
        self.db = db
        self.collection_name = collection_name
        self.pydantic_class = pydantic_class
        self.pk_field = pk_field

    def get(self, key):
        document = self.db.engine.get(self.collection_name, key)
        if document is None:
            return None
        return self.pydantic_class(**document)

    def query(self) -> "Query[T]": 
        return Query(self)

class Query[T]:
    def __init__(self, collection : Collection):
        self.collection = collection
        self.filters : List[Predicate] = []

    def filter(self, field: str, operator : str, value :str) -> "Query[T]":
        self.filters.append(Predicate(field, operator, value))
        return self
    
    def fetch_all(self) -> Iterable[T]:
        pydantic_class = self.collection.pydantic_class
        return [pydantic_class(**x) for x in self.collection.db.engine.fetch_all(self.collection.collection_name, self.filters)]
    
    def one(self) -> T:
        value = self.one_or_none()
        if value is None:
            raise NotFound(f"Could not find {self.collection.collection_name} satisfying {self.filters}")
        return value
    
    def one_or_none(self) -> Optional[T]:
        it = iter(self.fetch_all())

        try: 
            value = next(it)
        except StopIteration:
            return None
        
        # make sure there are no additional records fetched
        try: 
            next(it)
        except StopIteration:
            return value

        raise TooManyValues()


class Collections:
    pass

class DB:
    def __init__(self, engine):
        self.collections : Dict[str, Collection]= {}
        self.engine = engine
        self.c = Collections()

    def get_internal_contract(self) -> _InternalDBContract:
        "Overriden by DB specific implementation"
        return self.engine

    def add_collection(self, pydantic_class: BaseModel, pk_field : str):
        collection_name = pydantic_class.__name__

        collection = Collection(self, collection_name, pydantic_class, pk_field)
        self.collections[collection_name] = collection
        setattr(self.c, collection_name, collection)

    def put(self, *instances : List[BaseModel]):
        def to_put_operation(instance):
            pydantic_class = type(instance)
            collection_name = pydantic_class.__name__
            document = instance.model_dump()
            pk_field = self.collections[collection_name].pk_field
            id = document[pk_field]
            return PutOperation(collection_name, id, document)
        self.engine.put([to_put_operation(x) for x in instances])
    
    def delete(self, *keys : List[Tuple[Type, str]]):
        self.get_internal_contract().delete([DeleteOperation(pydantic_class.__name__, id) for pydantic_class, id in keys])


