"""A project opened from python, for real.

Gated on ZOU_PG_BIN naming a patched install, same as the rust suite two
crates over, and on the extension having been built:

    crates/zou-python/build.sh
    ZOU_PG_BIN=$PWD/build/pg/bin python3 -m unittest discover -s crates/zou-python/test

These start postgres, so they are not free. Most of them take a fixture,
which is a database branched off the machine's template and costs a
postmaster start; the one that is about the ordinary path runs initdb
and is most of the suite's time on its own.
"""

import json
import os
import subprocess
import sys
import time
import unittest
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "python"))

PG_BIN = os.environ.get("ZOU_PG_BIN", "")
BUILT = (HERE.parent / "python" / "zou" / "_zou.abi3.so").exists()
READY = PG_BIN != "" and BUILT
if not READY:
    print("skipping: needs ZOU_PG_BIN and crates/zou-python/build.sh")

if BUILT:
    import zou as zoulib

try:
    import supabase  # noqa: F401

    HAVE_SUPABASE = True
except ImportError:
    HAVE_SUPABASE = False


def sql(handle, statements):
    """A host process sets its own schema up over the dsn, the same way
    it would against any other postgres. psql is next to the postgres
    this was opened with, so there is nothing else to install."""
    subprocess.run(
        [os.path.join(PG_BIN, "psql"), handle.dsn, "-v", "ON_ERROR_STOP=1", "-c", statements],
        check=True,
        capture_output=True,
    )


@unittest.skipUnless(READY, "needs ZOU_PG_BIN and the built extension")
class Embedded(unittest.TestCase):
    def test_a_request_is_answered_in_this_process(self):
        with zoulib.create_fixture() as zou:
            sql(
                zou,
                """create table public.todos (id int primary key, title text, done boolean default false);
                   insert into public.todos values (1, 'write the binding', false), (2, 'ship it', true);
                   grant select on public.todos to anon;""",
            )
            answer = zou.request(
                "GET",
                "/rest/v1/todos?select=title&done=eq.false",
                {"apikey": zou.anon_key},
            )
            self.assertEqual(answer.status, 200)
            self.assertEqual(json.loads(answer.body), [{"title": "write the binding"}])

    def test_nothing_is_added_on_the_way_in(self):
        with zoulib.create_fixture() as zou:
            self.assertEqual(zou.request("GET", "/rest/v1/").status, 401)

    @unittest.skipUnless(HAVE_SUPABASE, "needs supabase-py")
    def test_supabase_py_talks_to_a_database_in_this_process(self):
        with zoulib.create_fixture() as zou:
            sql(
                zou,
                """create table public.secrets (id int primary key, body text);
                   insert into public.secrets values (1, 'nobody else');
                   alter table public.secrets enable row level security;
                   grant select on public.secrets to anon, authenticated, service_role;""",
            )
            # No policy means no rows, which is the whole reason there
            # are two keys.
            anon = zou.client().table("secrets").select("*").execute()
            self.assertEqual(anon.data, [])
            service = (
                zou.client(zou.service_role_key).table("secrets").select("*").execute()
            )
            self.assertEqual(len(service.data), 1)

            # And the other half of the interface, on the same client.
            signed = zou.client().auth.sign_up(
                {"email": "python@example.com", "password": "correct horse battery"}
            )
            self.assertEqual(signed.user.email, "python@example.com")

    def test_the_same_project_goes_on_a_port_as_well(self):
        with zoulib.create_fixture() as zou:
            sql(
                zou,
                """create table public.notes (id int primary key);
                   insert into public.notes values (7);
                   grant select on public.notes to anon;""",
            )
            port = zou.listen(0)
            self.assertNotEqual(port, 0, "the kernel named one")
            self.assertEqual(zou.url, f"http://127.0.0.1:{port}")

            # Over a socket this time, and the same answer.
            request = urllib.request.Request(
                f"{zou.url}/rest/v1/notes", headers={"apikey": zou.anon_key}
            )
            with urllib.request.urlopen(request) as answer:
                self.assertEqual(answer.status, 200)
                self.assertEqual(json.load(answer), [{"id": 7}])

    def test_a_fixture_is_a_database_per_test_rather_than_per_suite(self):
        # The first one on a cold machine builds the template, which is
        # the initdb none of the others are running.
        with zoulib.create_fixture() as first:
            at = time.monotonic()
            with zoulib.create_fixture() as second:
                print(f"the second fixture took {(time.monotonic() - at) * 1000:.0f} ms")
                self.assertEqual(first.target, second.target, "one template per machine")
                self.assertNotEqual(first.tenant, second.tenant)
                self.assertTrue(first.tenant.startswith("fixture-"))

                sql(
                    first,
                    """create table public.mine (id int primary key);
                       insert into public.mine values (1);
                       grant select on public.mine to anon;""",
                )
                mine = first.request(
                    "GET", "/rest/v1/mine?select=id", {"apikey": first.anon_key}
                )
                self.assertEqual(json.loads(mine.body), [{"id": 1}])

                # The other one is a database, not another view of this
                # one.
                elsewhere = second.request(
                    "GET", "/rest/v1/mine?select=id", {"apikey": second.anon_key}
                )
                self.assertEqual(elsewhere.status, 404)

                # And branchable already, because the template folded a
                # full capture down before it was published.
                self.assertTrue(second.branchable())

    def test_a_database_too_young_to_branch_says_so(self):
        # The ordinary path: a store of its own, initdb, and nothing
        # folded down yet.
        with zoulib.create_zou() as zou:
            self.assertFalse(zou.branchable())
            with self.assertRaises(zoulib.ZouError) as refused:
                zou.branch("too-soon")
            self.assertEqual(refused.exception.code, "ZOU_STORE")
            self.assertIn("cannot be branched yet", str(refused.exception))

    def test_closing_twice_is_fine(self):
        zou = zoulib.create_fixture()
        zou.close()
        zou.close()


@unittest.skipUnless(BUILT, "needs crates/zou-python/build.sh")
class Offline(unittest.TestCase):
    """The part that needs no postgres, so it runs anywhere."""

    def test_dir_and_url_are_two_ways_of_saying_the_same_thing(self):
        with self.assertRaises(ValueError):
            zoulib.create_zou(dir="./data", url="s3://bucket/app")

    def test_a_postgres_that_is_not_there_says_which_one(self):
        with self.assertRaises(zoulib.ZouError) as refused:
            zoulib.create_zou(dir="/tmp/zou-python-nowhere", pg_bin="/nonexistent/bin")
        self.assertEqual(refused.exception.code, "ZOU_OPTIONS")
        self.assertIn("/nonexistent/bin/postgres", str(refused.exception))


if __name__ == "__main__":
    unittest.main()
