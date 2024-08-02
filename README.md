This exists because I find myself wanting to use hosted key-value storage
(ie: google's datastore service) in quick and dirty apps which just need 
a little backend state managed. However, testing locally or making automated
tests is more painful because one needs to run a simulator. 

This library provides an ergonomic (in my opinion) and minimal API for
storing data in a document storage backend. Two backends are also provided:
a sqlite version (which can be used for testing) and a datastore backend 
(which can be used by a real deployed app).

