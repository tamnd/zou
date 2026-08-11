# zou, from Go

A whole Supabase compatible project inside your process: postgres, rest, auth, and storage, with no daemon to start, no port to pick, and no docker compose file.

```go
import zou "github.com/tamnd/zou/go"

func TestASignupLandsInAuthUsers(t *testing.T) {
	project, err := zou.Fixture()
	if err != nil {
		t.Fatal(err)
	}
	defer project.Close()

	answer, err := project.Client().Post(project.URL()+"/auth/v1/signup", "application/json", body)
}
```

`Fixture` is a database of this test's own, branched off a template the machine built once, which is tens of milliseconds rather than the seconds initdb costs.
`Open(zou.Options{Dir: "./data"})` is a project that stays where you put it, and `Open(zou.Options{})` is one that goes away when it closes.

A `*Zou` is an `http.RoundTripper`, so `project.Client()` is an ordinary `*http.Client` that answers in this process with no socket under it, and anything that takes a client can be handed one.
`project.DSN()` is the other door, for `database/sql` or psql on the database directly.

## Building it

This is cgo over `libzou`, so the library has to exist before the package will link:

```bash
cargo build -p libzou
ZOU_PG_BIN=$PWD/build/pg/bin go test ./go/...
```

`go/test.sh` does both.
The package names `../target/debug` and bakes an rpath to it, which is what makes a checkout work with nothing else set.
Anywhere else, `CGO_LDFLAGS="-L<dir> -lzou -Wl,-rpath,<dir>"` names the directory holding `libzou.so` or `libzou.dylib`.

Postgres is a child process and it has to be the patched build, so `ZOU_PG_BIN` or `Options.PgBin` has to name one.
Unix only for now, and the whole story is in [docs/embedded.md](../docs/embedded.md).
