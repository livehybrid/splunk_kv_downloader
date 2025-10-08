import os
import sys
import re
from os.path import dirname
import requests
import urllib3
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
import glob
from splunk.clilib.bundle_paths import make_splunkhome_path
from splunk.persistconn.application import PersistentServerConnectionApplication
import splunk.Intersplunk as si
from splunklib import client as client

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
        archive_name = f"{app_name}_{collection_name}_{int(time.time())}"

        try:
            # Get backup directory path
            splunk_db = os.environ.get('SPLUNK_DB', '/opt/splunk/var/lib/splunk')
            backup_dir = os.path.join(splunk_db, 'kvstorebackup')
            
            # List existing backups before creating new one
            existing_backups = []
            if os.path.exists(backup_dir):
                existing_backups = [f for f in os.listdir(backup_dir) if os.path.isfile(os.path.join(backup_dir, f))]
            
            self.logger.info(f"Existing backup files before creating new one: {existing_backups}")

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
                    'status': 500,
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

            # Make the backup request
            backup_response = service.post('/services/kvstore/backup/create', **backup_params)
            
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
            # self.logger.info(f"Backup creation response: {backup_response.body.read()}")
            # Wait for the backup to complete and the tarball to be created
            max_wait_time = 300  # 5 minutes maximum wait
            wait_interval = 2    # Check every 2 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Check for new backup files that weren't in the original list
                current_backups = []
                if os.path.exists(backup_dir):
                    # Get both files and directories
                    current_items = os.listdir(backup_dir)
                    current_backups = [f for f in current_items if os.path.isfile(os.path.join(backup_dir, f))]
                    current_dirs = [f for f in current_items if os.path.isdir(os.path.join(backup_dir, f))]
                
                # Find new backup files (not in the original list)
                new_backups = [f for f in current_backups if f not in existing_backups]
                
                # Look for tarball files specifically (usually .tar.gz)
                new_tarballs = [f for f in new_backups if f.endswith('.tar.gz') or f.endswith('.tar')]
                
                if new_tarballs:
                    # Found new tarball, check if file size is stable
                    backup_file = os.path.join(backup_dir, new_tarballs[0])
                    self.logger.info(f"Found new backup tarball: {new_tarballs[0]}")
                    if self._is_file_size_stable(backup_file, stable_duration=10):
                        self.logger.info(f"Tarball file size is stable, proceeding with download: {new_tarballs[0]}")
                        break
                    else:
                        self.logger.info(f"Tarball file size still changing, continuing to wait: {new_tarballs[0]}")
                
                # Check if there's a directory with our archive name (backup in progress)
                backup_dir_name = os.path.join(backup_dir, archive_name)
                if os.path.exists(backup_dir_name) and os.path.isdir(backup_dir_name):
                    self.logger.info(f"Backup directory exists, waiting for tarball creation: {archive_name}")
                
                # Also check for backup files matching our archive name pattern
                backup_pattern = os.path.join(backup_dir, f"{archive_name}*")
                backup_items = glob.glob(backup_pattern)
                
                # Filter out directories and only keep files
                backup_files = [item for item in backup_items if os.path.isfile(item)]
                
                if backup_files:
                    # Get the most recent file
                    backup_file = max(backup_files, key=os.path.getctime)
                    filename = os.path.basename(backup_file)
                    
                    # Verify this is a new file (not in existing backups)
                    if filename not in existing_backups:
                        self.logger.info(f"Found new backup file: {filename}")
                        # Check if file size is stable (not being written to)
                        if self._is_file_size_stable(backup_file, stable_duration=10):
                            self.logger.info(f"File size is stable, proceeding with download: {filename}")
                            break
                        else:
                            self.logger.info(f"File size still changing, continuing to wait: {filename}")
                
                # Wait before checking again
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                self.logger.info(f"Waiting for backup completion... ({elapsed_time}s elapsed)")
            
            else:
                # Timeout reached
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

            try:
                # Read and stream the file
                with open(backup_file, 'rb') as f:
                    file_content = f.read()
                
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
                'status': 500,
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
