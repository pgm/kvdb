import sqlite3
import os
import json
from dataclasses import dataclass
from typing import List, Tuple, Type, TypeVar, Iterable, Optional, Dict, Any
from pydantic import BaseModel, Field, Json
from .kvdb import Predicate, DeleteOperation, PutOperation

_comparisons = {
    "=": (lambda a, b: a == b),
    "!=": (lambda a, b: a != b),
    "<": (lambda a, b: a < b),
    ">": (lambda a, b: a > b),
    "<=": (lambda a, b: a <= b),
    ">=": (lambda a, b: a >= b),
}

def _predicate_is_satisified(document: Dict[str, Any], predicate: Predicate):
    return _comparisons[predicate.operator](document[predicate.field], predicate.value)

class SqliteDBContract:
    # not performant -- intended just for unit tests. Does all querying in memory.
    def __init__(self, filename):
        needs_schema_create = not os.path.exists(filename)

        self.connection = sqlite3.connect(filename, check_same_thread=False)

        if needs_schema_create:
            self.connection.execute("CREATE TABLE document (id varchar, collection varchar, content varchar)")

    def get(self, collection_name : str, key : str) -> Optional[Dict[str, Any]]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT content FROM document WHERE collection = ? AND id = ?", [collection_name, key])
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None
        
        assert len(rows) == 1
        document = json.loads(rows[0][0])

        return document
    
    def fetch_all(self, collection_name : str, filters: List[Predicate]) -> Iterable[Dict[str, Any]]:
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT content FROM document WHERE collection = ?", [collection_name])
            satisified_filters = []
            for row in cursor.fetchall():
                document = json.loads(row[0])
                if all([_predicate_is_satisified(document, filter) for filter in filters]):
                    satisified_filters.append(document)
            return satisified_filters
        finally:
            cursor.close()

    def delete(self, operations: List[DeleteOperation]):
        cursor = self.connection.cursor()
        try:
            for operation in operations:
                print("delete ",  [operation.collection_name, operation.id])
                cursor.execute("DELETE FROM document WHERE collection = ? and id = ?", [operation.collection_name, operation.id])
            self.connection.commit()
        finally:
            cursor.close()

    def put(self, operations: List[PutOperation]):
        cursor = self.connection.cursor()
        try:
            for operation in operations:                
                cursor.execute("DELETE FROM document WHERE collection = ? and id = ?", [operation.collection_name, operation.id])
                cursor.execute("INSERT INTO document (collection, id, content) VALUES (?, ?, ?)", [operation.collection_name, operation.id, json.dumps(operation.document)])
            self.connection.commit()
        finally:
            cursor.close()
