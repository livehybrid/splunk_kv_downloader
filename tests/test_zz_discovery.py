"""TEMPORARY discovery, removed before merge."""
import base64, io, json, tarfile
from conftest import Splunk

TA = "kvdl_disc"


def test_discover(splunk):
    out = []
    st, b = splunk.request("POST", "/services/apps/local", data={"name": TA, "visible": "0", "label": TA})
    out.append(f"create app {st} {b[:200]}")
    ns = f"/servicesNS/nobody/{TA}/storage/collections"
    st, b = splunk.request("POST", f"{ns}/config", data={"name": "disc", "field.s": "string", "field.n": "number", "field.b": "bool"})
    out.append(f"create coll {st} {b[:200]}")
    rows = [{"_key": "k:1 a,b", "s": 'quo"te, comma\nnl Zoë 🚀', "n": 3.5, "b": True}, {"_key": "k2", "s": "", "n": -7}]
    st, b = splunk.request("POST", f"{ns}/data/disc/batch_save", json_body=rows)
    out.append(f"batch_save {st} {b[:200]}")
    st, b = splunk.request("GET", "/services/kv_downloader", params={"app": TA, "collection": "disc"}, timeout=400)
    out.append(f"handler {st} len={len(b)} head={b[:120]!r}")
    try:
        raw = base64.b64decode(b)
        tf = tarfile.open(fileobj=io.BytesIO(raw))
        for m in tf.getmembers():
            out.append(f"MEMBER {m.name} {m.type} {m.size}")
            if m.isfile():
                out.append(repr(tf.extractfile(m).read()[:1500]))
    except Exception as e:
        out.append(f"decode err {e!r}")
    st, b = splunk.request("POST", "/services/search/jobs/oneshot", data={"search": "| kvbackup", "output_mode": "json"})
    out.append(f"kvbackup search {st} {b[:600]}")
    st, b = splunk.request("GET", "/services/configs/conf-limits/kvstore", params={"output_mode": "json"})
    try:
        c = json.loads(b)["entry"][0]["content"]
        out.append("limits " + json.dumps({k: v for k, v in c.items() if "max" in k}))
    except Exception as e:
        out.append(f"limits err {e!r} {b[:200]}")
    st, b = splunk.request("POST", "/services/authentication/users", data={"name": "disc_user", "password": "Disc-pass-1234", "roles": "user"})
    out.append(f"user {st}")
    u = Splunk("disc_user", "Disc-pass-1234")
    st, b = u.request("GET", "/services/kv_downloader", params={"app": TA, "collection": "disc"}, timeout=400)
    out.append(f"user handler {st} len={len(b)} head={b[:300]!r}")
    st, b = u.request("GET", f"{ns}/data/disc", params={"output_mode": "json"})
    out.append(f"user data read {st} {b[:200]}")
    st, b = splunk.request("GET", "/services/kvstore/status", params={"output_mode": "json"})
    out.append(f"kvstatus {st} {b[:900]}")
    raise AssertionError("\n".join(out))
