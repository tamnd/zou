// A project opened from node, for real.
//
// Gated on ZOU_PG_BIN naming a patched install, same as the rust suite
// next door, and on the addon having been built:
//
//   crates/zou-node/build.sh
//   ZOU_PG_BIN=$PWD/build/pg/bin node --test crates/zou-node/test/
//
// These start postgres, so they are seconds each.

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

const { createZou } = ready ? await import("../index.js") : {};

// A host process sets its own schema up over the dsn, the same way it
// would against any other postgres. psql is next to the postgres this
// was opened with, so there is nothing else to install.
function sql(zou, statements) {
  execFileSync(path.join(pgBin, "psql"), [zou.dsn, "-v", "ON_ERROR_STOP=1", "-c", statements], {
    stdio: "pipe",
  });
}

test("supabase-js talks to a database in this process", { skip: !ready }, async (t) => {
  const zou = await createZou();
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
  const zou = await createZou();
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
  const zou = await createZou();
  t.after(() => zou.close());
  const refused = await zou.fetch(`${zou.url}/rest/v1/`);
  assert.equal(refused.status, 401);
});

test("the same project goes on a port as well", { skip: !ready }, async (t) => {
  const zou = await createZou();
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

test("a database too young to branch says so", { skip: !ready }, async (t) => {
  const zou = await createZou();
  t.after(() => zou.close());
  assert.equal(await zou.branchable(), false);
  await assert.rejects(() => zou.branch("too-soon"), /cannot be branched yet/);
});

test("closing twice is fine", { skip: !ready }, async () => {
  const zou = await createZou();
  await zou.close();
  await zou.close();
});
