"""
Integration test for the kv_downloader persistent REST handler.

Proves the handler installs and *executes* on the Splunk 10 / Python 3.9 runtime
(a broken import — e.g. the old unvendored `requests`/`urllib3` — would 500 the
endpoint). No real KV backup is produced; the handler's own validation/JSON error
paths are exercised.
"""
from __future__ import annotations

import json

APP = "kv_downloader"


def test_app_installed_and_enabled(splunk):
    entries = splunk.entries(f"/services/apps/local/{APP}")
    assert entries, f"{APP} is not installed"
    assert entries[0]["content"].get("disabled") in (False, 0, "0"), f"{APP} is disabled"


def test_handler_runs_paramless(splunk):
    # No app/collection -> the handler returns its own JSON validation error.
    # Reaching that proves the persist script loaded and ran (no import crash).
    st, body = splunk.request("GET", f"/services/{APP}")
    assert st == 200, f"handler did not respond 200: {st}: {body[:300]}"
    assert "ImportError" not in body and "ModuleNotFoundError" not in body, f"import error: {body[:300]}"
    assert "app and collection parameters are required" in body, f"unexpected handler response: {body[:300]}"


def test_handler_runs_kvstore_path(splunk):
    # A real app + a non-existent collection exercises the splunklib client
    # connect + KV-store lookup path, then returns the handler's 404 JSON.
    st, body = splunk.request(
        "GET", f"/services/{APP}",
        params={"app": "search", "collection": "kv_downloader_nonexistent_xyz"},
    )
    assert "ImportError" not in body and "ModuleNotFoundError" not in body, f"import error: {body[:300]}"
    # The handler ran the KV lookup and returned its own JSON error for the
    # missing collection (splunklib raises 404, so the message is a "not found"
    # / "could not find" variant). Any of those proves the KV path executed.
    low = body.lower()
    assert any(s in low for s in ("does not exist", "not found", "could not find")), \
        f"expected a collection-not-found error from the handler: {st}: {body[:300]}"
