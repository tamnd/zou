# zou-postgres

The patched PostgreSQL that [zou](https://github.com/tamnd/zou) runs, as a wheel.

```bash
pip install zou zou-postgres
```

```python
from zou import create_fixture

project = create_fixture()          # a database of this test's own
supabase = project.client()
```

No `ZOU_PG_BIN`, no path to point anywhere, and nothing to build.
The binding asks for `pg_bin`, then the environment, then this package.

## What is in it

`initdb`, the postmaster, `psql`, `pg_dump` and `pg_restore`, the loadable modules including `vector`, and the share tree they read.
It is the same tree a zou release ships and the same one the curl installer downloads, so this is a way of getting it rather than a different postgres.

```python
import zou_postgres

zou_postgres.pg_bin()   # .../site-packages/zou_postgres/pg/bin
zou_postgres.postgres() # the postmaster
zou_postgres.initdb()   # the program that makes a data directory
```

The wheel is per platform, because binaries are.
On linux it is tagged with the glibc it was built against rather than an older one it was not, since a wheel tag is the only thing pip can check.

Apache-2.0. The source and the patch series are at [github.com/tamnd/zou](https://github.com/tamnd/zou).
