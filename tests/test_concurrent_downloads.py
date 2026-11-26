"""
Two downloads at the same time must each get their own collection.

The handler asks Splunk for a KV store backup and then waits for the tarball
to appear in $SPLUNK_DB/kvstorebackup/. Before this test it took "the first
new tarball" in that directory, so a concurrent request (or any other backup
made meanwhile) could be handed to the wrong caller.

The checks compare row contents with what each collection was seeded with,
never the tarball's file name or member paths: a crossed download is
detected by the rows it carries. Clean-up is checked by listing the backup
directory inside the container (a route the handler's HTTP API never offers),
and a sentinel tarball that belongs to nobody must survive.
"""
from __future__ import annotations

import base64
import gzip
import io
import json
import subprocess
import tarfile
import threading
import time

import pytest

from conftest import CONTAINER, Splunk, docker_exec
from test_download_e2e import TA, _create_collection, _seed, download, kv  # noqa: F401

BACKUP_DIR = "/opt/splunk/var/lib/splunk/kvstorebackup"
SENTINEL = "aios_sentinel_not_ours.tar.gz"
DECOY = "aios_decoy_backup.tar.gz"
ROUNDS = 2
# Big enough that its backup is still being written when the decoy lands
# (30,000 rows finished first on 10.0 and hid a crossed download there).
BIG_N = 100_000
DECOY_DELAY = 0.3


def _rows_any_path(body):
    """Every JSON document in the tarball, whatever its member path."""
    raw = base64.b64decode(body, validate=True)
    tf = tarfile.open(fileobj=io.BytesIO(gzip.decompress(raw)), mode="r:")
    rows = []
    for m in tf.getmembers():
        if not m.isfile():
            continue
        text = tf.extractfile(m).read().decode("utf-8").strip()
        if not text:
            continue
        if text.startswith("["):
            rows.extend(json.loads(text))
        else:
            rows.extend(json.loads(ln) for ln in text.splitlines() if ln.strip())
    return rows


def _content_fingerprint(rows):
    return sorted((r["_key"], r.get("name")) for r in rows)


def _list_backup_dir():
    rc, out, err = docker_exec("sh", "-c", f"mkdir -p {BACKUP_DIR} && ls -1A {BACKUP_DIR}")
    if rc != 0:
        pytest.skip(f"cannot list the backup directory in the container: {err[:200]}")
    return sorted(x for x in out.splitlines() if x.strip())


@pytest.fixture(scope="module")
def two_collections(kv):  # noqa: F811
    """A small and a much larger collection, so their backups finish at
    different times. Keys and names are disjoint between the two."""
    small = [{"_key": f"s{i}", "name": f"small {i}", "qty": i, "price": 1.0, "active": True}
             for i in range(5)]
    big = [{"_key": f"b{i:05d}", "name": f"big {i}", "qty": i, "price": 2.0, "active": False}
           for i in range(BIG_N)]
    _create_collection(kv, "conc_small")
    _seed(kv, "conc_small", small)
    _create_collection(kv, "conc_big")
    _seed(kv, "conc_big", big)
    return {"conc_small": small, "conc_big": big}


def _download_together(names):
    """Start one download per collection at the same instant, each with its
    own client. Returns {collection: (status, body)}."""
    barrier = threading.Barrier(len(names))
    out = {}

    def run(name):
        client = Splunk()
        barrier.wait()
        out[name] = download(client, name)

    threads = [threading.Thread(target=run, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=900)
    return out


def test_concurrent_downloads_get_their_own_rows(two_collections):
    for rnd in range(ROUNDS):
        results = _download_together(list(two_collections))
        for name, seeded in two_collections.items():
            st, body = results[name]
            assert st == 200, f"round {rnd}: download {name} -> {st}: {body[:300]}"
            got = _content_fingerprint(_rows_any_path(body))
            want = _content_fingerprint(seeded)
            assert got == want, (
                f"round {rnd}: download of {name} carried {len(got)} rows "
                f"(first {got[:2]}), expected {len(want)} (first {want[:2]})")


def test_downloads_remove_only_their_own_backup(two_collections):
    rc, _, err = docker_exec("sh", "-c",
                             f"mkdir -p {BACKUP_DIR} && printf not-a-real-backup > {BACKUP_DIR}/{SENTINEL}")
    if rc != 0:
        pytest.skip(f"cannot write into the container: {err[:200]}")
    before = _list_backup_dir()
    assert SENTINEL in before

    results = _download_together(list(two_collections))
    for name, (st, body) in results.items():
        assert st == 200, f"download {name} -> {st}: {body[:300]}"

    after = _list_backup_dir()
    assert SENTINEL in after, f"a backup that was not the handler's was deleted: {after}"
    assert after == before, f"backup directory changed: before {before}, after {after}"


def _put_file_in_backup_dir(name, data):
    """Write `data` into the backup directory atomically (temp file, then mv),
    as the splunk user, so it appears complete in one step."""
    tmp = f"/tmp/{name}.part"
    p = subprocess.run(["docker", "exec", "-i", "-u", "splunk", CONTAINER, "sh", "-c",
                        f"cat > {tmp} && mv {tmp} {BACKUP_DIR}/{name}"],
                       input=data, capture_output=True, timeout=60)
    assert p.returncode == 0, p.stderr.decode()[:300]


def test_foreign_backup_during_download_is_ignored_and_kept(two_collections):
    """The ordering that crosses downloads: another complete backup tarball
    lands in kvstorebackup/ while a download is still waiting for its own.
    The decoy is a genuine backup of conc_small; the request is for conc_big.
    The download must still carry conc_big's rows, and the decoy, which is not
    the handler's, must still be there afterwards."""
    st, small_body = download(Splunk(), "conc_small")
    assert st == 200, f"download conc_small -> {st}: {small_body[:300]}"
    decoy_bytes = base64.b64decode(small_body)
    docker_exec("sh", "-c", f"rm -f {BACKUP_DIR}/{DECOY}")
    before = _list_backup_dir()

    out = {}
    t = threading.Thread(target=lambda: out.update(big=download(Splunk(), "conc_big")))
    t.start()
    time.sleep(DECOY_DELAY)
    _put_file_in_backup_dir(DECOY, decoy_bytes)
    t.join(timeout=900)

    st, body = out["big"]
    assert st == 200, f"download conc_big -> {st}: {body[:300]}"
    got = _content_fingerprint(_rows_any_path(body))
    want = _content_fingerprint(two_collections["conc_big"])
    assert got == want, (f"download of conc_big carried {len(got)} rows (first {got[:2]}), "
                         f"expected {len(want)} (first {want[:2]})")
    after = _list_backup_dir()
    assert DECOY in after, f"the foreign backup was deleted: {after}"
    assert sorted(set(after) - {DECOY}) == before, f"backup directory changed: before {before}, after {after}"
    docker_exec("sh", "-c", f"rm -f {BACKUP_DIR}/{DECOY}")
