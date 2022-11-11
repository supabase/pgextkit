# Postgres Extension Kit

This extension allows PostgreSQL extensions to dynamically acquire shared memory as well as start background workers.
This allows extensions to start at any time during  the lifetime of the server.
This also means can be upgraded without restarting the database server.

*This extension is in its early stages of development and is not guaranteed to be stable*

## Building from source

```shell
PG_VERSION=pg15 cargo pgx run --features extension
```

(Change `pg15` to the required version accordingly)

## Installation

This extension needs to be added to `shared_preload_libraries` setting of PostgreSQL. Extensions that depend on it,
should specify it in the list of requirements. Upon the completion of the SQL queries of those extensions it is recommended
that they call `pgexkit.load('extname', 'version')` to load themselves.