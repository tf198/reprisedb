RepriseDB
=========

Experimental document database aiming to give the relationship another chance.

**Not usable at the moment!**

Aims
-------

* Strong consistency - full ACID compliance across data and indexes
* Long running transactions - ability to rectify issues and re-commit
* Asynchronous servers for maximum concurrency (twisted)
* Suitable for master/backup setup as well as large scale sharded deployment
* All servers capable of acting as co-ordinator if case of failure.
* Minimal locking on co-ordinator - just enough to serialize the commits.
* Point in time rollback
* Low overhead pluggable access controls.
* Cryptographic auditing - each commit stores a hash of the previous commit hash plus the included data.
* Indexing and authorization should be able to occur on any server in the cluster.

Datastores
==========
The following database engines are included and can be used independently if required.

RevisionDataStore
-----------------
LMDB backed datastore that implements revisioning.  A given key can be queried for most recent,
most recent within a given range or specific revision.

ArchiveDataStore
----------------
Extension of `RevisionDataStore` which stores values in a separate zip archive.  Data can be quickly
streamed from a `RevisionDataStore` to an `ArchiveDataStore` making it suitable for compaction and backup.
For example at the end of every day you can stream everything into a compressed archive and remove all but the last
three revisions of each key from the database. Each backup then represents a complete point in time recovery and the
full set can be used for integrity auditing.

MemoryDataStore
---------------
Dummy datastore that ignores revisions and behaves as a standard key => value store.

ProxyDataStore
--------------
Takes one or more `DataStore` instances and proxies the methods across them.  Implements
smart iterators.