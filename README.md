# Thermionic

[![CircleCI](https://circleci.com/gh/spoke-d/thermionic.svg?style=svg)](https://circleci.com/gh/spoke-d/thermionic)
[![Go Report Card](https://goreportcard.com/badge/github.com/spoke-d/thermionic)](https://goreportcard.com/report/github.com/spoke-d/thermionic)

Thermionic is a lightweight, distributed, highly available relational database,
using [SQLite](https://www.sqlite.org/index.html) for the underlying storage
engine.

SQLite doesn't naturally allow clustering out of the box, the following project
enables that by building on top of [distributed version](https://github.com/CanonicalLtd/dqlite),
of SQLite.

The configuration of Thermionic can be as a singular node or in a cluster setup.
Even when using Thermionic in singular node the global clustering database is
still used, effectively behaving as a regular SQLite database.

Since each node of the cluster also needs to be aware of other nodes in the
cluster it also uses a SQLite database (local) which can also hold state and
queried for information.

Running Thermionic as part of a larger system, as a central store for some
critical relational data, without having to run larger, more complex distributed
databases should be possible because of the minimal setup.

## Copyright Notice

DB Clustering work is derived from [lxc/lxd](https://github.com/lxc/lxd)

## Motivation

Whilst re-visiting LXD clustering in [Juju](https://github.com/juju/juju),
I wanted to understand how LXD clustering worked in terms of querying and 
configuring the cluster. Whilst doing this, it was surprising how clean the
implementation was for setting up a clustered SQLite database and so removing
the LXC parts of the LXD project created the following Thermionic project.

## Peers

 - [dqlite](https://github.com/CanonicalLtd/dqlite)
 - [lxc/lxd](https://github.com/lxc/lxd)
 - [rqlite](https://github.com/rqlite/rqlite)