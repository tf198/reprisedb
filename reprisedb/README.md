RepriseDB
=========

Overiew
-------
Experimental document database aiming to give the relationship another chance.

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