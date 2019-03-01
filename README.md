# nosql

NoSQL is an abstraction layer for data persistency.

Right now is used for future persistency in (step certificates)[https://github.com/smallstep/certificates]
and it is in development so the API is not stable.

# Implementation

The current version comes with a BoltDB implementation, but multiple
implementations are in the roadmap.

[ ] Memory
[x] [BoldDB](https://github.com/etcd-io/bbolt), using etcd fork
[ ] Badger
[ ] MariaDB/MySQL
[ ] PostgreSQL
[ ] ...
