// A project opened from node, for real.
//
// Gated on ZOU_PG_BIN naming a patched install, same as the rust suite
// next door, and on the addon having been built:
//
//   crates/zou-node/build.sh
//   ZOU_PG_BIN=$PWD/build/pg/bin node --test crates/zou-node/test/
//
// These start postgres, so they are not free. Most of them take a
// fixture, which is a database branched off the machine's template and
// costs a postmaster start; the one that is about the ordinary path
// runs initdb and is most of the suite's time on its own.

import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const pgBin = process.env.ZOU_PG_BIN ?? "";
const addon = path.join(here, "..", "zou.node");
const ready = pgBin !== "" && existsSync(addon);

if (!ready) {
  console.log("skipping: needs ZOU_PG_BIN and crates/zou-node/build.sh");
}

const { createZou, createFixture } = ready ? await import("../index.js") : {};

// A host process sets its own schema up over the dsn, the same way it
// would against any other postgres. psql is next to the postgres this
// was opened with, so there is nothing else to install.
function sql(zou, statements) {
  execFileSync(path.join(pgBin, "psql"), [zou.dsn, "-v", "ON_ERROR_STOP=1", "-c", statements], {
    stdio: "pipe",
  });
}

test("supabase-js talks to a database in this process", { skip: !ready }, async (t) => {
  const zou = await createFixture();
  t.after(() => zou.close());
  sql(
    zou,
    `create table public.todos (id int primary key, title text, done boolean default false);
     insert into public.todos values (1, 'write the binding', false), (2, 'ship it', true);
     grant select on public.todos to anon;`,
  );

  const supabase = zou.client();
  const { data, error } = await supabase.from("todos").select("*").eq("done", false);
  assert.equal(error, null);
  assert.deepEqual(
    data.map((row) => row.title),
    ["write the binding"],
  );

  // And the other half of the interface, on the same client.
  const signup = await supabase.auth.signUp({
    email: "node@example.com",
    password: "correct horse battery",
  });
  assert.equal(signup.error, null);
  assert.equal(signup.data.user.email, "node@example.com");
});

test("row level security is the reason there are two keys", { skip: !ready }, async (t) => {
  const zou = await createFixture();
  t.after(() => zou.close());
  sql(
    zou,
    `create table public.secrets (id int primary key, body text);
     insert into public.secrets values (1, 'nobody else');
     alter table public.secrets enable row level security;
     grant select on public.secrets to anon, authenticated, service_role;`,
  );

  const anon = await zou.client().from("secrets").select("*");
  assert.equal(anon.error, null);
  assert.deepEqual(anon.data, [], "no policy means no rows");

  const service = await zou.client(zou.serviceRoleKey).from("secrets").select("*");
  assert.equal(service.error, null);
  assert.equal(service.data.length, 1);
});

test("nothing is added on the way in", { skip: !ready }, async (t) => {
  const zou = await createFixture();
  t.after(() => zou.close());
  const refused = await zou.fetch(`${zou.url}/rest/v1/`);
  assert.equal(refused.status, 401);
});

test("the same project goes on a port as well", { skip: !ready }, async (t) => {
  const zou = await createFixture();
  t.after(() => zou.close());
  sql(
    zou,
    `create table public.notes (id int primary key);
     insert into public.notes values (7);
     grant select on public.notes to anon;`,
  );

  const port = await zou.listen(0);
  assert.notEqual(port, 0, "the kernel named one");
  assert.equal(zou.url, `http://127.0.0.1:${port}`);

  // Over a socket this time, and the same answer.
  const answer = await fetch(`${zou.url}/rest/v1/notes`, { headers: { apikey: zou.anonKey } });
  assert.equal(answer.status, 200);
  assert.deepEqual(await answer.json(), [{ id: 7 }]);
});

test("a database only just opened answers for a branch either way", { skip: !ready }, async (t) => {
  const zou = await createZou();
  t.after(() => zou.close());

  // The two read paths give different answers here, so ask rather than
  // assume. The layer path folds an image out of the base initdb wrote,
  // so a database is branchable from the start and what is worth
  // checking is that the child is a database rather than a shape. The
  // object path waits for a fold it has not had, so it refuses.
  if (await zou.branchable()) {
    const child = await zou.branch("too-soon");
    t.after(() => child.close());
    assert.equal(child.tenant, "too-soon");
    const headers = { apikey: child.anonKey };
    const answer = await child.fetch(`${child.url}/rest/v1/`, { headers });
    assert.equal(answer.status, 200);
  } else {
    await assert.rejects(() => zou.branch("too-soon"), /cannot be branched yet/);
  }
});

test("a fixture is a database per test rather than per suite", { skip: !ready }, async (t) => {
  // The first one on a cold machine builds the template, which is the
  // initdb none of the others are running.
  const first = await createFixture();
  t.after(() => first.close());
  const at = Date.now();
  const second = await createFixture();
  t.after(() => second.close());
  console.log(`the second fixture took ${Date.now() - at} ms`);

  assert.equal(first.target, second.target, "one template per machine");
  assert.notEqual(first.tenant, second.tenant);

  sql(
    first,
    `create table public.mine (id int primary key);
     insert into public.mine values (1);
     grant select on public.mine to anon;`,
  );
  const mine = await first.client().from("mine").select("*");
  assert.equal(mine.error, null);
  assert.deepEqual(mine.data, [{ id: 1 }]);

  // The other one is a database, not another view of this one.
  const elsewhere = await second.client().from("mine").select("*");
  assert.notEqual(elsewhere.error, null);

  // And the auth surface came along with the template rather than
  // being put in place per fixture, which is the part that used to
  // cost three seconds.
  const signup = await second.client().auth.signUp({
    email: "fixture@example.com",
    password: "correct horse battery",
  });
  assert.equal(signup.error, null);
});

test("closing twice is fine", { skip: !ready }, async () => {
  const zou = await createFixture();
  await zou.close();
  await zou.close();
});
