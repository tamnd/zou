# zou, from python

A whole Supabase compatible project inside your process: postgres, rest, auth, and storage, with no daemon to start, no port to pick, and no docker compose file.

```python
from zou import create_fixture

zou = create_fixture()
supabase = zou.client()          # a real supabase-py client, no socket under it
supabase.table("todos").select("*").eq("done", False).execute()
zou.close()
```

`create_fixture` is a database of this test's own, branched off a template the machine built once, which is tens of milliseconds rather than the seconds initdb costs.
`create_zou(dir="./data")` is a project that stays where you put it, and `create_zou()` is one that goes away when it closes.

`client()` needs supabase-py, which is an optional dependency: install `zou[client]` for it, or use `request()` or `listen()` and neither is needed.
Postgres is a child process and it has to be the patched build, so `ZOU_PG_BIN` or `pg_bin=` has to name one.

Unix only for now, and the whole story is in [docs/embedded.md](https://github.com/tamnd/zou/blob/main/docs/embedded.md).
