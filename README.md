# KV Store Backup Downloader

This Splunk app provides a REST endpoint for creating and downloading KVStore backups.

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

- Backup files are stored in `$SPLUNK_DB/kvstorebackup/`
- The app uses Splunk's built-in KVStore backup functionality
- **Important:** All operations use the authenticated user's session token, not a system token. This means users can only backup and download collections they have permission to access
- Collections are validated for existence before backup creation
- Both `app` and `collection` parameters are required
- The backup is returned as a base64-encoded tarball
- Backup creation may take some time depending on collection size
- The app waits for backup completion before returning the download
