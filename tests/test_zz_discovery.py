"""TEMPORARY discovery, removed before merge."""


def test_discover_command(splunk):
    out = []
    for ns in ("/services", "/servicesNS/admin/kv_downloader"):
        st, b = splunk.request("POST", f"{ns}/search/jobs/oneshot", data={"search": "| kvbackup", "output_mode": "json"})
        out.append(f"{ns} kvbackup {st} {b[:600]}")
    raise AssertionError("\n".join(out))
