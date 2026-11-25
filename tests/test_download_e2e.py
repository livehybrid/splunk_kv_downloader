"""
End-to-end: seed a real KV store collection, download it through the
kv_downloader handler, and compare what comes back with what was seeded.

The handler returns a base64-encoded tarball made by Splunk's own KV store
backup (`kvstore/backup/create`). These tests decode it with the standard
library (base64 + tarfile + json) and compare each row and field with the
Python list the test seeded from. They never consult the handler's code or a
second KV REST read to decide what "right" is: the seeded list is the truth.

Runs on 10.0 (MongoDB KV store) and 10.6 (Postgres-backed KV store, which
exports backups in batches of max_documents_per_batch_export_backup_kvservice
= 50000 documents by default; the large test crosses that boundary).
"""
from __future__ import annotations

import base64
import io
import json
import tarfile
import time

import pytest

from conftest import Splunk

HANDLER = "/services/kv_downloader"
TA = "kvdl_e2e"
NS = f"/servicesNS/nobody/{TA}/storage/collections"

SCHEMA = {"name": "string", "note": "string", "qty": "number", "price": "number", "active": "bool"}
NUMERIC = {"qty", "price"}

# Awkward values on purpose. No '/' in any _key: Splunk 10.6.0.5 404s by-id
# reads of such keys (reference_splunk_kvstore_slash_key_404), which is a
# platform defect, not something this app can change.
AWKWARD = [
    {"_key": "unicode-ключ-日本-🚀", "name": "Zoë Ångström 日本語 🚀", "note": "naïve café ✓",
     "qty": 42, "price": 3.14159, "active": True},
    {"_key": "comma,semi;colon:pipe|", "name": "a,b,c", "note": 'say "hello", then \'bye\'',
     "qty": -7, "price": 0.0, "active": False},
    {"_key": "space and.dot@at#hash", "name": "line1\nline2\r\nline3", "note": "tab\there\\back",
     "qty": 0, "price": -0.000001, "active": True},
    {"_key": "empty-fields", "name": "", "note": "", "qty": 1, "price": 1.5, "active": False},
    {"_key": "numeric-looking-strings", "name": "007", "note": "1e5", "qty": 123456789,
     "price": 2.5e10, "active": True},
    {"_key": "percent%20plus+query?q=1&x=2", "name": "<b>html</b> & entities &amp;",
     "note": "=SUM(A1)", "qty": 3, "price": 99.99, "active": False},
    {"_key": "quote'double\"backslash\\", "name": "'single'", "note": '"double"',
     "qty": 5, "price": 0.5, "active": True},
    # A field left out entirely must stay out (not come back as "" or null).
    {"_key": "missing-note", "name": "no note field", "qty": 8, "price": 8.0, "active": True},
]

LARGE_N = 50_123  # > 50000: KV REST max_rows_per_query and 10.6's export batch size


# --------------------------------------------------------------------------- helpers

def _wait_kv_ready(splunk, timeout=420):
    deadline, last = time.time() + timeout, None
    while time.time() < deadline:
        st, body = splunk.request("GET", "/services/kvstore/status", params={"output_mode": "json"})
        if st == 200:
            cur = json.loads(body)["entry"][0]["content"].get("current", {})
            last = cur.get("status")
            if str(last).lower() == "ready":
                return
        time.sleep(5)
    pytest.fail(f"KV store not ready within {timeout}s (last status {last!r})")


def _ok(st, body, what):
    assert st in (200, 201), f"{what} -> {st}: {body[:400]}"


def _create_collection(splunk, name, perms_read="*"):
    st, body = splunk.request("POST", f"{NS}/config",
                              data={"name": name, **{f"field.{k}": v for k, v in SCHEMA.items()}})
    if st == 409:  # left over from an earlier run against a live instance
        splunk.request("DELETE", f"{NS}/data/{name}")
    else:
        _ok(st, body, f"create collection {name}")
    st, body = splunk.request("POST", f"{NS}/config/{name}/acl", data={
        "sharing": "app", "owner": "nobody",
        "perms.read": perms_read, "perms.write": "admin"})
    _ok(st, body, f"acl {name}")


def _seed(splunk, name, rows):
    for i in range(0, len(rows), 1000):  # max_documents_per_batch_save = 1000
        st, body = splunk.request("POST", f"{NS}/data/{name}/batch_save",
                                  json_body=rows[i:i + 1000], timeout=180)
        _ok(st, body, f"batch_save {name} [{i}:{i + 1000}]")
        assert len(json.loads(body)) == len(rows[i:i + 1000]), f"batch_save {name} short: {body[:200]}"


def download(client, collection, app=TA):
    """Call the handler. Returns (status, body_text)."""
    return client.request("GET", HANDLER, params={"app": app, "collection": collection}, timeout=600)


def parse_backup(body, app, collection):
    """Independent decode: base64 -> gzip tar -> every JSON document under
    <app>/<collection>/. Returns (rows, member_names)."""
    raw = base64.b64decode(body, validate=True)
    tf = tarfile.open(fileobj=io.BytesIO(raw), mode="r:*")
    rows, names = [], []
    for m in tf.getmembers():
        names.append(m.name)
        if not m.isfile():
            continue
        parts = m.name.strip("./").split("/")
        assert parts[:2] == [app, collection], f"unexpected member {m.name!r} in backup of {app}/{collection}"
        text = tf.extractfile(m).read().decode("utf-8")
        stripped = text.strip()
        if not stripped:
            continue
        if stripped.startswith("["):
            rows.extend(json.loads(stripped))
        else:  # newline-delimited JSON
            rows.extend(json.loads(ln) for ln in stripped.splitlines() if ln.strip())
    return rows, names


def _strip_internal(row):
    return {k: v for k, v in row.items() if k != "_user"}


def assert_rows_equal(got_rows, seeded, members):
    """Row for row, field for field."""
    got_keys = [r.get("_key") for r in got_rows]
    assert len(got_keys) == len(set(got_keys)), f"duplicate rows in backup (members {members[:5]})"
    want = {r["_key"]: r for r in seeded}
    got = {r["_key"]: _strip_internal(r) for r in got_rows}
    missing = sorted(set(want) - set(got))
    extra = sorted(set(got) - set(want))
    assert not missing and not extra, (
        f"{len(got)} rows back, {len(want)} seeded; missing {missing[:5]} extra {extra[:5]} "
        f"(members {members[:5]})")
    for key, w in want.items():
        g = got[key]
        assert set(g) == set(w), f"{key!r}: fields {sorted(g)} != seeded {sorted(w)}"
        for field, wv in w.items():
            gv = g[field]
            if field in NUMERIC:
                assert isinstance(gv, (int, float)) and not isinstance(gv, bool), \
                    f"{key!r}.{field}: {gv!r} is not a number"
                assert float(gv) == float(wv), f"{key!r}.{field}: {gv!r} != {wv!r}"
            else:
                assert gv == wv and type(gv) is type(wv), f"{key!r}.{field}: {gv!r} != {wv!r}"


# --------------------------------------------------------------------------- fixtures

@pytest.fixture(scope="module")
def kv(splunk):
    _wait_kv_ready(splunk)
    st, body = splunk.request("POST", "/services/apps/local", data={"name": TA, "label": TA, "visible": "0"})
    assert st in (200, 201, 409), f"create app {TA} -> {st}: {body[:300]}"
    return splunk


@pytest.fixture(scope="module")
def awkward(kv):
    _create_collection(kv, "awkward")
    _seed(kv, "awkward", AWKWARD)
    return "awkward"


@pytest.fixture(scope="module")
def users(kv):
    """kvdl_plain: role `user` only. kvdl_backup: `user` + edit_kvstore, the
    capability Splunk's backup API asks for."""
    st, body = kv.request("POST", "/services/authorization/roles", data={
        "name": "kvdl_backup", "imported_roles": "user", "capabilities": "edit_kvstore"})
    assert st in (200, 201, 409), f"create role -> {st}: {body[:300]}"
    out = {}
    for name, role in (("kvdl_plain", "user"), ("kvdl_backup", "kvdl_backup")):
        pw = "Kvdl-test-pw-2026!"
        st, body = kv.request("POST", "/services/authentication/users",
                              data={"name": name, "password": pw, "roles": role})
        assert st in (200, 201, 409), f"create user {name} -> {st}: {body[:300]}"
        out[name] = Splunk(name, pw)
    return out


# --------------------------------------------------------------------------- tests

def test_awkward_values_round_trip(kv, awkward):
    st, body = download(kv, awkward)
    assert st == 200, f"download -> {st}: {body[:400]}"
    rows, members = parse_backup(body, TA, awkward)
    assert_rows_equal(rows, AWKWARD, members)


def test_download_reflects_current_data(kv, awkward):
    """A second download after a change carries the change: the handler must
    return the backup it just asked for, not an older tarball on disk."""
    _create_collection(kv, "fresh")
    first = [{"_key": "a", "name": "before", "qty": 1, "price": 1.0, "active": True}]
    _seed(kv, "fresh", first)
    st, body = download(kv, "fresh")
    assert st == 200, f"first download -> {st}: {body[:400]}"
    rows, members = parse_backup(body, TA, "fresh")
    assert_rows_equal(rows, first, members)

    second = first + [{"_key": "b", "name": "after", "qty": 2, "price": 2.0, "active": False}]
    second[0] = dict(second[0], name="changed")
    _seed(kv, "fresh", second)
    st, body = download(kv, "fresh")
    assert st == 200, f"second download -> {st}: {body[:400]}"
    rows, members = parse_backup(body, TA, "fresh")
    assert_rows_equal(rows, second, members)


def test_large_collection_complete(kv):
    """More rows than one KV REST page (max_rows_per_query 50000) and than one
    10.6 export batch (50000): every row comes back exactly once."""
    _create_collection(kv, "large")
    seeded = [{"_key": f"r{i:06d}", "name": f"row {i}", "qty": i, "price": i / 4, "active": i % 2 == 0}
              for i in range(LARGE_N)]
    _seed(kv, "large", seeded)
    st, body = download(kv, "large")
    assert st == 200, f"download -> {st}: {body[:400]}"
    rows, members = parse_backup(body, TA, "large")
    assert len(rows) == LARGE_N, f"{len(rows)} rows back, {LARGE_N} seeded (members {members[:8]})"
    assert_rows_equal(rows, seeded, members)


def test_empty_collection(kv):
    _create_collection(kv, "empty")
    st, body = download(kv, "empty")
    assert st == 200, f"download -> {st}: {body[:400]}"
    rows, _ = parse_backup(body, TA, "empty")
    assert rows == []


def _assert_refused(st, body, secret):
    assert st != 200, f"expected a refusal, got 200 ({len(body)} bytes)"
    assert secret not in body
    try:
        base64.b64decode(body, validate=True)
        decoded = True
    except Exception:
        decoded = False
    assert not decoded or not body.startswith("H4sI"), "refusal body is a gzip tarball"


def test_rbac_user_without_kvstore_capability_refused(kv, awkward, users):
    st, body = download(users["kvdl_plain"], awkward)
    _assert_refused(st, body, "Ångström")
    assert st == 403, f"expected 403, got {st}: {body[:300]}"
    assert "edit_kvstore" in body or "permission" in body.lower(), body[:300]


def test_rbac_capability_holder_can_download(kv, awkward, users):
    """Positive control for the two refusals: the same handler and collection
    succeed for a user who has edit_kvstore and can read the collection."""
    st, body = download(users["kvdl_backup"], awkward)
    assert st == 200, f"download as kvdl_backup -> {st}: {body[:400]}"
    rows, members = parse_backup(body, TA, awkward)
    assert_rows_equal(rows, AWKWARD, members)


def test_rbac_collection_not_readable_refused(kv, users):
    """edit_kvstore is not enough: a collection the user cannot read is not
    downloadable through the handler (it checks with the user's own token)."""
    _create_collection(kv, "adminonly", perms_read="admin")
    _seed(kv, "adminonly", [{"_key": "s", "name": "TOP-SECRET-ROW", "qty": 1, "price": 1.0, "active": True}])
    st, body = download(users["kvdl_backup"], "adminonly")
    _assert_refused(st, body, "TOP-SECRET-ROW")
    assert st == 404, f"expected 404, got {st}: {body[:300]}"


def test_unauthenticated_refused(kv):
    anon = Splunk("nobody-here", "wrong-password")
    st, body = download(anon, "awkward")
    assert st == 401, f"expected 401, got {st}: {body[:200]}"
