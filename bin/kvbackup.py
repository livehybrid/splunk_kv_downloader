import os
import sys
import re
from os.path import dirname
import base64

ta_name = 'kv_downloader'
pattern = re.compile(r'[\\/]etc[\\/]apps[\\/][^\\/]+[\\/]bin[\\/]?$')
new_paths = [path for path in sys.path if not pattern.search(path) or ta_name in path]
new_paths.insert(0, os.path.join(dirname(dirname(__file__)), "lib"))
new_paths.insert(0, os.path.sep.join([os.path.dirname(__file__), ta_name]))
sys.path = new_paths

import importlib.util
import http.client
import json
import logging
import re
import time
import uuid
from splunk.clilib.bundle_paths import make_splunkhome_path
from splunk.persistconn.application import PersistentServerConnectionApplication
import splunk.Intersplunk as si
from splunklib import client as client

BACKUP_BUSY_RETRY_SECONDS = 2


def _safe_name_part(value, limit=60):
    """Collection and app names reach the archive file name; keep only
    characters that cannot step out of the backup directory."""
    return re.sub(r'[^A-Za-z0-9_-]', '_', str(value))[:limit] or 'x'


def unique_archive_name(app_name, collection_name):
    """A backup name no other request can produce. Splunk appends .tar.gz."""
    return 'kvdl_{}_{}_{}'.format(_safe_name_part(app_name), _safe_name_part(collection_name),
                                  uuid.uuid4().hex)


def _backup_busy(exc):
    """Splunk runs one KV store backup at a time and answers a second one with
    503 "KVStore backup or restore is already running or not available!"."""
    return getattr(exc, 'status', None) == 503


def _http_status(exc):
    """Pass splunkd's refusal through (403 for a missing capability, 404 for a
    collection the user cannot see) instead of reporting every error as 500."""
    status = getattr(exc, 'status', None)
    return status if isinstance(status, int) and 400 <= status < 600 else 500


class RequestInfo(object):
    def __init__(self, user, session_key, method, path, query, raw_args):
        self.user = user
        self.session_key = session_key
        self.method = method
        self.path = path
        self.query = query
        self.raw_args = raw_args

class KVBackup(PersistentServerConnectionApplication):

    def __init__(self, command_line, command_arg, logger=None):
        self.appName = "kv_downloader"
        self.restPath = "kv_downloader"
        logger = logger if logger else logging.getLogger()
        log_file_path = make_splunkhome_path(['var', 'log', 'splunk', 'kv_downloader.log'])
        file_handler = logging.handlers.RotatingFileHandler(log_file_path, maxBytes=25000000,
                                                        backupCount=5)

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %z %Z')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        self.logger = logger
        self.logger.setLevel(logging.DEBUG)

        PersistentServerConnectionApplication.__init__(self)

    def get_backup_and_download(self, request_info, **kwargs):
        """
        Create a backup for dummyApp/DummyCollection and immediately download it.
        """
        splunkd_uri = request_info.raw_args['server']['rest_uri']
        splunkd_token = request_info.session_key
        self.session_key = splunkd_token

        # Use default values
        app_name = request_info.query.get("app", None)
        collection_name = request_info.query.get("collection", None)
        if not app_name or not collection_name:
            return {
                'payload': json.dumps({
                    'success': False,
                    'message': 'app and collection parameters are required'
                }),
            }
        # Each request names its own backup, waits for exactly that file and
        # removes only that file. It never takes "a new tarball" from the
        # directory, which another request or an admin's backup may also be
        # writing to.
        archive_name = unique_archive_name(app_name, collection_name)

        try:
            # Get backup directory path
            splunk_db = os.environ.get('SPLUNK_DB', '/opt/splunk/var/lib/splunk')
            backup_dir = os.path.join(splunk_db, 'kvstorebackup')
            
            backup_file = os.path.join(backup_dir, archive_name + '.tar.gz')

            # Create Splunk client connection
            service = client.connect(
                host=splunkd_uri.split('://')[1].split(':')[0],
                port=int(splunkd_uri.split(':')[-1].split('/')[0]) if ':' in splunkd_uri else 8089,
                token=splunkd_token,
                scheme='https' if 'https' in splunkd_uri else 'http'
            )
            
            # Check if the collection exists before creating backup
            try:
                collection_path = f'/servicesNS/nobody/{app_name}/storage/collections/config/{collection_name}'
                collection_response = service.get(collection_path)
                if collection_response.status != 200:
                    self.logger.error(f"Collection {collection_name} does not exist in app {app_name}")
                    return {
                        'payload': json.dumps({
                            'success': False,
                            'message': f'Collection "{collection_name}" does not exist in app "{app_name}"'
                        }),
                        'status': 404,
                        'headers': {
                            'Content-Type': 'application/json'
                        },
                    }
                self.logger.info(f"Verified collection {collection_name} exists in app {app_name}")
            except Exception as e:
                self.logger.error(f"Error checking collection existence: {str(e)}")
                return {
                    'payload': json.dumps({
                        'success': False,
                        'message': f'Error checking if collection exists: {str(e)}'
                    }),
                    'status': _http_status(e),
                    'headers': {
                        'Content-Type': 'application/json'
                    },
                }
 
            # Prepare backup request parameters
            backup_params = {
                'archiveName': archive_name,
                'appName': app_name,
                'collectionName': collection_name,
                'pointInTime': 'false',
                'parallelCollections': '1'
            }

            max_wait_time = 300  # 5 minutes maximum wait, including time queued behind other backups
            wait_interval = 2
            deadline = time.time() + max_wait_time

            # Splunk refuses a backup while another is running (503). Wait our
            # turn rather than fail a concurrent download.
            while True:
                try:
                    backup_response = service.post('/services/kvstore/backup/create', **backup_params)
                    break
                except Exception as e:
                    if _backup_busy(e) and time.time() < deadline:
                        self.logger.info(f"Another KV store backup is running; retrying {archive_name}")
                        time.sleep(BACKUP_BUSY_RETRY_SECONDS)
                        continue
                    raise

            if backup_response.status != 200:
                return {
                    'payload': json.dumps({
                        'success': False,
                        'message': f'Backup creation failed with status: {backup_response.status}'
                    }),
                    'status': backup_response.status,
                    'headers': {
                        'Content-Type': 'application/json'
                    },
                }

            self.logger.info(f"Backup creation initiated for: {archive_name}")
            # Wait for exactly our tarball, and for its size to settle.
            while True:
                if os.path.isfile(backup_file):
                    if self._is_file_size_stable(backup_file, stable_duration=10):
                        self.logger.info(f"Backup file is complete: {os.path.basename(backup_file)}")
                        break
                    self.logger.info(f"Backup file still being written: {os.path.basename(backup_file)}")
                if time.time() >= deadline:
                    return {
                        'payload': json.dumps({
                            'success': False,
                            'message': f'Backup creation timed out after {max_wait_time} seconds'
                        }),
                        'status': 408,
                        'headers': {
                            'Content-Type': 'application/json'
                        },
                    }
                time.sleep(wait_interval)

            try:
                # Read the file, then remove it: it was made for this request only.
                try:
                    with open(backup_file, 'rb') as f:
                        file_content = f.read()
                finally:
                    self._remove_own_backup(backup_file)
                
                # Get file name for download
                filename = os.path.basename(backup_file)
                
                self.logger.info(f"Successfully downloaded backup file: {filename}")
                
                return {
                    'payload': base64.b64encode(file_content).decode('utf-8'),
                    'status': 200,
                    'headers': {
                        'Content-Type': 'text/plain; charset=us-ascii',
                        'Content-Disposition': f'attachment; filename="{filename}"',
                        'X-Content-Encoding': 'base64'
                    }
                }
            except Exception as e:
                self.logger.error(f"Error getting backup file: {str(e)}")
                return {
                    'payload': json.dumps({
                        'success': False,
                        'message': f'Error getting backup file: {str(e)}'
                    }),
                    'status': 500,
                    'headers': {
                        'Content-Type': 'application/json'
                    },
                }
        except Exception as e:
            self.logger.error(f"Error creating and downloading backup: {str(e)}")
            return {
                'payload': json.dumps({
                    'success': False,
                    'message': f'Error creating and downloading backup: {str(e)}'
                }),
                'status': _http_status(e),
                'headers': {
                    'Content-Type': 'application/json'
                },
            }

    def get_forms_args_as_dict(self, form_args):

        post_arg_dict = {}

        for arg in form_args:
            name = arg[0]
            value = arg[1]

            post_arg_dict[name] = value

        return post_arg_dict

    def handle(self, in_bytes):
        in_string = in_bytes.decode('utf-8')
        self.logger.error("Handling WS request")
        # self.logger.exception(in_bytes)
        try:

            self.logger.info("Handling WS request 2")
            # Parse the arguments
            args = self.parse_in_string(in_string)
            self.logger.error(args)
            # return {
            #     'payload': args,
            #     'status': 200
            # }

            # Get the user information
            session_key = args['session']['authtoken']
            user = args['session']['user']

            # Get the method
            method = args['method']

            # Get the path and the args
            if 'rest_path' in args:
                path = args['rest_path']
            else:
                return {
                    'payload': 'No path was provided',
                    'status': 403
                }

            if method.lower() == 'post':
                query = self.get_forms_args_as_dict(args["form"])
            else:
                query = args['query_parameters']

            # Make the request info object
            request_info = RequestInfo(user, session_key, method, path, query, args)

            return self.get_backup_and_download(request_info, **query)

        except Exception as exception:
            if self.logger is not None:
                self.logger.exception("Failed to handle request due to an unhandled exception")

            return {
                'payload': str(exception),
                'status': 500
            }

    def _remove_own_backup(self, backup_file):
        try:
            os.remove(backup_file)
            self.logger.info(f"Removed backup file: {os.path.basename(backup_file)}")
        except OSError as e:
            self.logger.warning(f"Could not remove backup file {backup_file}: {e}")

    def _is_file_size_stable(self, filepath, stable_duration=10):
        """
        Check if a file's size is stable (not being written to) for the specified duration.
        Returns True if the file size hasn't changed for the specified duration.
        """
        try:
            if not os.path.exists(filepath):
                return False
            
            initial_size = os.path.getsize(filepath)
            self.logger.info(f"Initial file size: {initial_size} bytes for {filepath}")
            
            # Wait for the specified duration, checking size periodically
            check_interval = 2  # Check every 2 seconds
            checks_made = 0
            required_checks = stable_duration // check_interval
            
            while checks_made < required_checks:
                time.sleep(check_interval)
                current_size = os.path.getsize(filepath)
                
                if current_size != initial_size:
                    self.logger.info(f"File size changed from {initial_size} to {current_size} bytes, not stable yet")
                    return False
                
                checks_made += 1
                self.logger.info(f"File size stable check {checks_made}/{required_checks}: {current_size} bytes")
            
            self.logger.info(f"File size is stable at {initial_size} bytes for {stable_duration} seconds")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking file stability: {str(e)}")
            return False

    def convert_to_dict(self, query):
        """
        Create a dictionary containing the parameters.
        """
        parameters = {}

        for key, val in query:

            # If the key is already in the list, but the existing entry isn't a list then make the
            # existing entry a list and add thi one
            if key in parameters and not isinstance(parameters[key], list):
                parameters[key] = [parameters[key], val]

            # If the entry is already included as a list, then just add the entry
            elif key in parameters:
                parameters[key].append(val)

            # Otherwise, just add the entry
            else:
                parameters[key] = val

        return parameters

    def parse_in_string(self, in_string):
        """
        Parse the in_string
        """

        params = json.loads(in_string)

        params['method'] = params['method'].lower()

        params['form_parameters'] = self.convert_to_dict(params.get('form', []))
        params['query_parameters'] = self.convert_to_dict(params.get('query', []))
        return params
