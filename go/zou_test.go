// A project opened from Go, for real.
//
// Gated on ZOU_PG_BIN naming a patched install, same as the other
// bindings, and on libzou having been built:
//
//	cargo build -p libzou
//	ZOU_PG_BIN=$PWD/build/pg/bin go test ./go/...
//
// go/test.sh does both. These start postgres, so they are not free. Most
// of them take a fixture, which is a database branched off the machine's
// template and costs a postmaster start; the one that is about the
// ordinary path runs initdb and is most of the suite's time on its own.
package zou_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	zou "github.com/tamnd/zou/go"
)

func ready(t *testing.T) {
	t.Helper()
	if os.Getenv("ZOU_PG_BIN") == "" {
		t.Skip("needs ZOU_PG_BIN naming a patched postgres install")
	}
}

// A host process sets its own schema up over the dsn, the same way it
// would against any other postgres. psql is next to the postgres this was
// opened with, so there is nothing else to install.
func sql(t *testing.T, handle *zou.Zou, statements string) {
	t.Helper()
	psql := filepath.Join(os.Getenv("ZOU_PG_BIN"), "psql")
	out, err := exec.Command(psql, handle.DSN(), "-v", "ON_ERROR_STOP=1", "-c", statements).CombinedOutput()
	if err != nil {
		t.Fatalf("psql: %v: %s", err, out)
	}
}

func fixture(t *testing.T) *zou.Zou {
	t.Helper()
	handle, err := zou.Fixture()
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return handle
}

func TestRequestIsAnsweredInThisProcess(t *testing.T) {
	ready(t)
	handle := fixture(t)
	sql(t, handle, `create table public.todos (id int primary key, title text, done boolean default false);
		insert into public.todos values (1, 'write the binding', false), (2, 'ship it', true);
		grant select on public.todos to anon;`)

	answer, err := handle.Request("GET", "/rest/v1/todos?select=title&done=eq.false",
		[][2]string{{"apikey", handle.AnonKey()}}, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if answer.Status != 200 {
		t.Fatalf("status %d: %s", answer.Status, answer.Body)
	}
	var rows []map[string]string
	if err := json.Unmarshal(answer.Body, &rows); err != nil {
		t.Fatalf("body %q: %v", answer.Body, err)
	}
	if len(rows) != 1 || rows[0]["title"] != "write the binding" {
		t.Fatalf("rows %v", rows)
	}
}

func TestNothingIsAddedOnTheWayIn(t *testing.T) {
	ready(t)
	handle := fixture(t)
	answer, err := handle.Request("GET", "/rest/v1/", nil, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if answer.Status != 401 {
		t.Fatalf("a request with no key was answered %d", answer.Status)
	}
}

// The seam Go actually reaches for: an http.Client whose transport is
// the project, so a library that takes one is talking to a database in
// this process without knowing it.
func TestTheHandleIsAnHTTPTransport(t *testing.T) {
	ready(t)
	handle := fixture(t)

	body := strings.NewReader(`{"email":"go@example.com","password":"correct horse battery"}`)
	request, err := http.NewRequest("POST", handle.URL()+"/auth/v1/signup", body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("apikey", handle.AnonKey())
	request.Header.Set("content-type", "application/json")

	answer, err := handle.Client().Do(request)
	if err != nil {
		t.Fatalf("signup: %v", err)
	}
	defer answer.Body.Close()
	said, _ := io.ReadAll(answer.Body)
	if answer.StatusCode != 200 {
		t.Fatalf("status %d: %s", answer.StatusCode, said)
	}
	if !strings.Contains(string(said), "access_token") {
		t.Fatalf("no token in %s", said)
	}
	if got := answer.Header.Get("content-type"); !strings.Contains(got, "json") {
		t.Fatalf("content-type %q", got)
	}
}

func TestTheSameProjectGoesOnAPortAsWell(t *testing.T) {
	ready(t)
	handle := fixture(t)
	sql(t, handle, `create table public.notes (id int primary key);
		insert into public.notes values (7);
		grant select on public.notes to anon;`)

	port, err := handle.Listen(0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if port == 0 {
		t.Fatal("the kernel named no port")
	}
	if want := fmt.Sprintf("http://127.0.0.1:%d", port); handle.URL() != want {
		t.Fatalf("url %q, want %q", handle.URL(), want)
	}

	// Over a socket this time, and the same answer.
	request, err := http.NewRequest("GET", handle.URL()+"/rest/v1/notes", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("apikey", handle.AnonKey())
	answer, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("over the port: %v", err)
	}
	defer answer.Body.Close()
	said, _ := io.ReadAll(answer.Body)
	if answer.StatusCode != 200 || strings.TrimSpace(string(said)) != `[{"id":7}]` {
		t.Fatalf("status %d body %s", answer.StatusCode, said)
	}
}

func TestAFixtureIsADatabasePerTest(t *testing.T) {
	ready(t)
	first := fixture(t)
	at := time.Now()
	second := fixture(t)
	t.Logf("the second fixture took %d ms", time.Since(at).Milliseconds())

	if first.Target() != second.Target() {
		t.Fatal("one template per machine, so one target")
	}
	if first.Tenant() == second.Tenant() {
		t.Fatal("two fixtures, two databases")
	}
	if !strings.HasPrefix(first.Tenant(), "fixture-") {
		t.Fatalf("tenant %q", first.Tenant())
	}

	sql(t, first, `create table public.mine (id int primary key);
		insert into public.mine values (1);
		grant select on public.mine to anon;`)

	mine, err := first.Request("GET", "/rest/v1/mine?select=id",
		[][2]string{{"apikey", first.AnonKey()}}, nil)
	if err != nil || mine.Status != 200 {
		t.Fatalf("%v %v", err, mine)
	}
	// The other one is a database, not another view of this one.
	elsewhere, err := second.Request("GET", "/rest/v1/mine?select=id",
		[][2]string{{"apikey", second.AnonKey()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if elsewhere.Status != 404 {
		t.Fatalf("the other fixture answered %d", elsewhere.Status)
	}

	// And branchable already, because the template folded a full
	// capture down before it was published.
	branchable, err := second.Branchable()
	if err != nil || !branchable {
		t.Fatalf("branchable %v %v", branchable, err)
	}
}

func TestADatabaseTooYoungToBranchSaysSo(t *testing.T) {
	ready(t)
	// The ordinary path: a store of its own, initdb, and nothing folded
	// down yet.
	handle, err := zou.Open(zou.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer handle.Close()

	branchable, err := handle.Branchable()
	if err != nil {
		t.Fatal(err)
	}
	if branchable {
		t.Fatal("a database this young has nothing to branch from")
	}
	if _, err := handle.Branch("too-soon"); err == nil {
		t.Fatal("the branch was allowed")
	} else {
		var refused *zou.Error
		if !errors.As(err, &refused) {
			t.Fatalf("error %T", err)
		}
		if refused.Kind != "store" || !strings.Contains(refused.Message, "cannot be branched yet") {
			t.Fatalf("refusal %+v", refused)
		}
	}
}

func TestClosingTwiceIsFine(t *testing.T) {
	ready(t)
	handle, err := zou.Fixture()
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close again: %v", err)
	}
}

// The part that needs no postgres, so it runs anywhere the library is
// built.
func TestDirAndURLAreTwoWaysOfSayingTheSameThing(t *testing.T) {
	if _, err := zou.Open(zou.Options{Dir: "./data", URL: "s3://bucket/app"}); err == nil {
		t.Fatal("both were accepted")
	}
}

func TestAPostgresThatIsNotThereSaysWhichOne(t *testing.T) {
	_, err := zou.Open(zou.Options{Dir: t.TempDir(), PgBin: "/nonexistent/bin"})
	var refused *zou.Error
	if !errors.As(err, &refused) {
		t.Fatalf("error %v", err)
	}
	if refused.Kind != "options" || !strings.Contains(refused.Message, "/nonexistent/bin/postgres") {
		t.Fatalf("refusal %+v", refused)
	}
}
