"""A project opened from python, for real.

Needs the extension and a patched postgres, either from a checkout:

    crates/zou-python/build.sh
    ZOU_PG_BIN=$PWD/build/pg/bin python3 -m unittest discover -s crates/zou-python/test

or from the wheels, which is what a project that never built anything
has:

    pip install zou zou-postgres
    python3 -m unittest discover -s <a copy of this directory>

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
# In a checkout the extension is built next to the source. From a wheel
# it is in site-packages and this file has been copied out of the tree
# to run against it, so the source directory goes on the path only when
# there is something in it.
SOURCE = HERE.parent / "python"
if (SOURCE / "zou" / "_zou.abi3.so").exists():
    sys.path.insert(0, str(SOURCE))

try:
    import zou as zoulib
except ImportError:
    zoulib = None

# Where psql is, since these tests set their schema up over the dsn the
# way a host process would. The environment when a checkout points at
# its own build, and the zou-postgres wheel when there is no checkout.
PG_BIN = os.environ.get("ZOU_PG_BIN", "")
if not PG_BIN:
    try:
        import zou_postgres

        PG_BIN = zou_postgres.pg_bin()
    except ImportError:
        pass

READY = zoulib is not None and PG_BIN != ""
if not READY:
    print("skipping: needs the extension and a patched postgres, see the docstring")

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


@unittest.skipUnless(READY, "needs the extension and a patched postgres")
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

    def test_a_database_only_just_opened_answers_for_a_branch_either_way(self):
        # The ordinary path: a store of its own, initdb, and nothing
        # folded down yet. The two read paths do different things with
        # that, so ask rather than assume. The layer path folds an image
        # out of the base initdb wrote, so the branch opens and what is
        # worth checking is that the child is a database of its own. The
        # object path waits for a fold it has not had, so it refuses.
        with zoulib.create_zou() as zou:
            if zou.branchable():
                with zou.branch("too-soon") as child:
                    self.assertEqual(child.tenant, "too-soon")
                    answer = child.request(
                        "GET", "/rest/v1/", {"apikey": child.anon_key}
                    )
                    self.assertEqual(answer.status, 200)
            else:
                with self.assertRaises(zoulib.ZouError) as refused:
                    zou.branch("too-soon")
                self.assertEqual(refused.exception.code, "ZOU_STORE")
                self.assertIn("cannot be branched yet", str(refused.exception))

    def test_closing_twice_is_fine(self):
        zou = zoulib.create_fixture()
        zou.close()
        zou.close()


@unittest.skipUnless(zoulib is not None, "needs the extension")
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
