# KV Store Backup Downloader

This Splunk app provides a REST endpoint for creating and downloading KVStore backups.

## Compatibility

| Attribute | Value |
|-----------|-------|
| **Python runtime** | 3.13 on Splunk 10.2 and later, 3.9 on Splunk 10.1 and earlier |
| **Expected compatible** | Splunk Enterprise and Cloud 9.3+ and 10.x |
| **Tested in CI** | On Splunk 10.0 (MongoDB KV store) and 10.6 (Postgres-backed KV store): seeds real collections (awkward values, 50,123 rows, empty), downloads them through the handler and compares every row and field; RBAC refusals. AppInspect CLI `cloud`, `future`, `private_victoria` and the hosted AppInspect API on every push |
| **Deployment roles** | Standalone, Distributed, Search Head Clustering |

The persistent REST handler uses only the Splunk-provided `splunklib` and
`http.client` (the unused `requests`/`urllib3` imports were removed), so
nothing extra is vendored. There is no search command: the old `kvbackup`
stanza in `commands.conf` pointed at the REST handler script, was never
exported, and failed with "External search command exited unexpectedly"
wherever it was visible, so it was removed in 1.0.3.

Splunk 10.2 and later ship Python 3.9 and 3.13 side by side; the handler sets
`python.required = 3.13`, so it runs on 3.13 there. Splunk 10.1 and earlier
ignore `python.required` and run them on 3.9 (`python.version = python3`), so
the code stays 3.9-compatible. CI runs the integration tests on Splunk 10.0
(Python 3.9) and 10.6 (Python 3.13) to hold both.

## Endpoint

### Create and Download Backup
**GET** `/services/kv_downloader`

Creates a backup for the specified app and collection, then immediately downloads it as a base64-encoded tarball.

**Parameters:**
- `app` (required): App name
- `collection` (required): Collection name

**Example:**
```bash
curl -k -u admin:password \
  "https://localhost:8089/services/kv_downloader?app=search&collection=big_lookup_1" \
  | base64 -d > backup.tgz
```

Or with authorization header:
```bash
curl 'https://splunkrest.cloud.livehybrid.com/services/kv_downloader?app=search&collection=big_lookup_1' \
  -H 'authorization: Basic YWRtaW46UGFzc3dvcmQ=' \
  | base64 -d > backup.tgz
```

## Features

- Creates KVStore backups using Splunk's native backup API
- Validates collection existence before creating backups
- Automatically waits for backup completion
- Downloads backup file as base64-encoded tarball
- Uses user session authentication for all operations
- Automatic filename generation with timestamps
- Comprehensive error handling and logging

## Requirements

- Splunk Enterprise
- Python 3
- Proper authentication with appropriate KVStore permissions
- User must have read access to the specified collections

## Installation

1. Place this app in `$SPLUNK_HOME/etc/apps/`
2. Restart Splunk or reload the app
3. The endpoint will be available at `/services/kv_downloader`

## Notes

- Backup files are made in `$SPLUNK_DB/kvstorebackup/` under a name unique to each request
  (`kvdl_<app>_<collection>_<random>.tar.gz`). The handler returns exactly that file and then
  deletes it; other files in the directory (your own backups, other downloads) are left alone.
- Splunk runs one KV store backup at a time. A download that arrives while another backup is
  running waits its turn (up to 5 minutes in all) instead of failing with 503.
- The app uses Splunk's built-in KVStore backup functionality
- **Important:** All operations use the authenticated user's session token, not a system token. This means users can only backup and download collections they have permission to access
- Collections are validated for existence before backup creation
- Both `app` and `collection` parameters are required
- The caller needs the `edit_kvstore` (or `edit_server`) capability, which
  Splunk's backup API requires, and read access to the collection. Refusals
  pass Splunk's status through: 403 for a missing capability, 404 for a
  collection the user cannot see (they were all 500 before 1.0.3)
- The tarball holds JSON files under `<app>/<collection>/` with the
  documents (each with `_key` and `_user`)
- The backup is returned as a base64-encoded tarball
- Backup creation may take some time depending on collection size
- The app waits for backup completion before returning the download
