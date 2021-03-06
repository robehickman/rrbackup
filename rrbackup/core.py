import functools, time, datetime, fnmatch, os, json, fcntl
import collections
from termcolor import colored

#---
import rrbackup.pipeline as pipeline
import rrbackup.crypto   as crypto
from . import fsutil as sfs


###################################################################################
def default_config(interface):
    """ The default configuration structure. """
    conf = { 'base_path'                      : None,             # The root from where the backup is performed
             'remote_manifest_diff_file'      : 'manifest_diffs', # Location of the remote manifest diffs
             'remote_gc_log_file'             : 'gc_log',         # Location of the remote garbage collection log
             'remote_garbage_object_log_file' : 'garbage_objects',# Accumulating log of garbage objects
             'remote_base_path'               : 'files',          # The directory used to store files on S3
             'local_manifest_file'            : 'manifest',       # Path and name of the local manifest file
             'local_lock_file'                : 'rrbackup_lock',  # Path and name of the local lock file
             'chunk_size'                     : 1048576 * 5,      # minimum chunk size is 5MB on s3, 1mb = 1048576
             'read_only'                      : False,            # Disable writing operations
             'write_only'                     : False,            # Disable reading operations
             'allow_delete_versions'          : True,             # Should remote versions be deletable (used by GC)
             'meta_pipeline'                  : [],               # pipeline applied to meta files like manifest diffs
             'file_pipeline'                  : [[ '*', []]],     # pipeline applied to backed up files, list as sort order is important
             'ignore_files'                   : [],               # files to ignore
             'skip_delete'                    : [],               # files which should never be deleted from manifest
             'visit_mountpoints'              : True,             # Should files in a unix mount point be included in backup?
             'split_chunk_size'               : 0}                # The manifest can be split into smaller chunks to
                                                                  # allow large updates to recover more easily in case
                                                                  # of connection loss. As this system is inherently designed
                                                                  # to have atomic commits, a connection fail fails the
                                                                  # whole action, which could be hours of time with a big
                                                                  # commit

    conf = interface.add_default_config(conf)
    return crypto.add_default_config(conf)

###################################################################################
def validate_config(parsed_config):
    if 'meta_pipeline' in parsed_config and not isinstance(parsed_config['meta_pipeline'], list): raise SystemExit('meta_pipeline in conf file mist be a list')
    if 'file_pipeline' in parsed_config and not isinstance(parsed_config['file_pipeline'], list): raise SystemExit('file_pipeline in conf file mist be a list')
    if 'ignore_files'  in parsed_config and not isinstance(parsed_config['ignore_files'], list):  raise SystemExit('ignore_files in conf file mist be a list')
    if 'skip_delete'   in parsed_config and not isinstance(parsed_config['skip_delete'], list):   raise SystemExit('skip_delete in conf file mist be a list')

###################################################################################
def merge_config(config, parsed_config):
    def dict_merge(dct, merge_dct): # recursive dict merge from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
        for k in merge_dct.keys():
            if (k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], collections.Mapping)):
                dict_merge(dct[k], merge_dct[k])
            else: dct[k] = merge_dct[k]
    dict_merge(config, parsed_config)
    return config

###################################################################################
def new_manifest():
    """ The structure of the locally stored manifest, and manifest data
     within this program.
    """

    return { 'latest_remote_diff' : {},
             'files'              : []}

###################################################################################
meta_pl_format = pl_in = pl_out = None
def init(interface, conn, config):
    """ Set up format of the pipeline used for storing meta-data like manifest diffs """
    global meta_pl_format, pl_in, pl_out
    meta_pl_format = pipeline.get_default_pipeline_format()
    meta_pl_format['format'].update({i : None for i in config['meta_pipeline']})
    if 'encrypt' in meta_pl_format['format']: meta_pl_format['format']['encrypt'] = config['crypto']['encrypt_opts']

    # ----
    pl_in  = pipeline.build_pipeline(functools.partial(interface.read_file, conn), 'in')
    pl_out = pipeline.build_pipeline(functools.partial(interface.write_file, conn), 'out')

    # Check for previous failed uploads and delete them
    if 'read_only' in config and not config['read_only']:
        interface.delete_failed_uploads(conn)
        garbage_collect(interface, conn, config, 'simple')


###################################################################################
def write_json_to_remote(config, path : str, data_to_write):
    meta = {'path' : path, 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
    return pl_out(json.dumps(data_to_write).encode('utf-8'), meta, config)

###################################################################################
def read_json_from_remote(config, path : str, version_id = None):

    meta = {'path'       : path,
            'version_id' : version_id,
            'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
    try: data, object_meta = pl_in(meta, config)
    except ValueError: return None, None

    return json.loads(data), object_meta



###################################################################################
def streaming_file_upload(interface, conn, config, local_file_path, system_path):

    #Determine the correct pipeline format to use for this file from the configuration
    try: pipeline_format = next((plf for wildcard, plf in config['file_pipeline']
                                 if fnmatch.fnmatch(system_path, wildcard)))
    except StopIteration: raise SystemExit('No pipeline format matches ')

    # Get remote file path
    remote_file_path = sfs.cpjoin(config['remote_base_path'], system_path)

    #----
    pipeline_configuration = pipeline.get_default_pipeline_format()
    pipeline_configuration['chunk_size'] = config['chunk_size']
    pipeline_configuration['format'] = {i : None for i in pipeline_format}
    if 'encrypt' in pipeline_configuration['format']:
        pipeline_configuration['format']['encrypt'] = config['crypto']['encrypt_opts']

    #-----
    upload = interface.streaming_upload()
    pl     = pipeline.build_pipeline_streaming(upload, 'out')
    pl.pass_config(config, pipeline.serialise_pipeline_format(pipeline_configuration))

    upload.begin(conn, remote_file_path)

    try:
        with open(local_file_path, 'rb') as fle:
            while True:
                print('.', end =" ")

                chunk = fle.read(config['chunk_size'])
                if chunk == b'': break
                pl.next_chunk(chunk)
            print()
        return upload.finish()

    # If file no longer exists at this stage assume it has been deleted and ignore it
    except IOError:
        upload.abort()
        raise


###################################################################################
def streaming_file_download(interface, conn, config, remote_file_path, version_id, local_file_path):
    download_stream  = interface.streaming_download()
    header = download_stream.begin(conn, remote_file_path, version_id)[0]
    pl                = pipeline.build_pipeline_streaming(download_stream, 'in')
    pl.pass_config(config, header)

    sfs.make_dirs_if_dont_exist(local_file_path)
    with open(local_file_path, 'wb') as fle:
        while True:
            res = pl.next_chunk()
            if res is None: break
            fle.write(res)


###################################################################################
def get_remote_manifest_versions(interface, conn, config):
    return list(interface.list_versions(conn, config['remote_manifest_diff_file']))


###################################################################################
def get_remote_manifest_diff(config, version_id = None):
    meta = {'path'       : config['remote_manifest_diff_file'],
            'version_id' : version_id,
            'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
    data, meta2 = pl_in(meta, config)
    return { 'version_id'    : version_id,
             'last_modified' : meta2['last_modified'],
             'body'          : json.loads(data)}


###################################################################################
def get_remote_manifest_diffs(interface, conn, config):
    """ Get and sort the progression of change differences from the remote """

    diffs = []
    for v in get_remote_manifest_versions(interface, conn, config):
        meta = {'path'       : config['remote_manifest_diff_file'],
                'version_id' : v['VersionId'],
                'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
        data, meta2 = pl_in(meta, config)
        diffs.append({ 'version_id' : v['VersionId'],
                       'body' : data,
                       'meta' : meta2})

    return list(diffs)


###################################################################################
def rebuild_manifest_from_diffs(versions, version_id = None):
    """ Rebuild manifest from a series of diffs, passed as an array of
    boot key objects """

    # filter these to find the diffs up until the desired version
    if version_id is not None:
        filtered = []
        for vers in versions:
            filtered.append(vers)
            if vers['version_id'] == version_id:
                break
        else:
            raise SystemExit('The given version ID ' + version_id + ' does not exist')

        versions = filtered

    # merge the diffs
    diffs = [json.loads(vers['body']) for vers in versions]

    file_manifest = new_manifest()
    file_manifest['files'] = sfs.apply_diffs(diffs, file_manifest['files'])
    file_manifest['latest_remote_diff'] = {'version_id' : versions[-1]['version_id'],
                                           'last_modified' : ''}#versions[-1]['last_modified']}
    return file_manifest

###################################################################################
def get_manifest(interface, conn, config):
    """ Get the manifest. If a locally cached manifest exists this is used,
    otherwise the manifest is rebuilt from the diff sequence on the remote.
    """

    try:
        file_manifest = json.loads(sfs.file_get_contents(config['local_manifest_file']))

        try: latest = get_remote_manifest_diff(config)
        except ValueError: raise SystemExit('Local manifest exists but remote missing, suspect tampering')

        if file_manifest['latest_remote_diff']['last_modified'] != latest['last_modified'].isoformat():
            # If the client where to crash between writing the remote diff and local manifest the remote manifest
            # will be one version ahead of the local. Handle this transparently by rebuilding the local manifest.
            # Under normal circumstances the remote should never be more than one diff ahead.
            local_manifest_time = datetime.datetime.strptime(file_manifest['latest_remote_diff']['last_modified'].replace('T', ' ').split('+')[0], '%Y-%m-%d %H:%M:%S')
            remote_diff_time    = latest['last_modified'].replace(tzinfo=None)
            if remote_diff_time > local_manifest_time:
                diffs = get_remote_manifest_diffs(interface, conn, config)
                if local_manifest_time == diffs[-2]['meta']['last_modified'].replace(tzinfo=None):
                    print('Remote is one diff ahead of local, updating local manifest')
                    new_diff = json.loads(diffs[-1]['body'])
                    file_manifest['files'] = sfs.apply_diffs([new_diff], file_manifest['files'])

                    file_manifest['latest_remote_diff'] = {'version_id' : diffs[-1]['meta']['version_id'], 'last_modified' : diffs[-1]['meta']['last_modified'].isoformat()}

                    #============================================
                    write_local_manifest(config, file_manifest)
                    return file_manifest

                else: raise SystemExit('Latest remote manifest does not align with local manifest')
            else: raise SystemExit('Latest remote manifest does not align with local manifest')

        return file_manifest

    except IOError:
        versions = get_remote_manifest_diffs(interface, conn, config)
        if versions != []: return rebuild_manifest_from_diffs(versions)
        else: return new_manifest() # No manifest exists on s3


###################################################################################
def write_local_manifest(config, file_manifest):
    """ Write the local manifest, done using write and move for atomicity """

    sfs.file_put_contents(config['local_manifest_file']+'.tmp', json.dumps(file_manifest))
    os.rename(config['local_manifest_file']+'.tmp', config['local_manifest_file'])


###################################################################################
def split_files_changes_into_chunks(config, localy_changed_files):
    # Allow changes to be split into chunks to handle a large number of changes
    # made to a filesystem mostly consisting of large files, where the upload
    # may fail mid-process. Useful for initial uploads
    changed_files_chunked = []

    if localy_changed_files !={}:
        localy_changed_files_list = [changed for p, changed in localy_changed_files.items()]

        # Allow diff to be split into chunks to handle large uploads
        chunk_size = config['split_chunk_size'] if 'split_chunk_size' in config else 0

        if chunk_size > 0:

            chunk = []
            for item in localy_changed_files_list:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    changed_files_chunked.append(chunk)
                    chunk = []

            if len(chunk) != 0:
                changed_files_chunked.append(chunk)

        else:
            changed_files_chunked = [localy_changed_files_list]

    return changed_files_chunked


###################################################################################
def referance_duplicate_to_master(master_file, duplicate_file):
    """ Referances a duplicate file back to a master file """

    duplicate_file['real_path']   = master_file['real_path']
    duplicate_file['version_id']  = master_file['version_id']

    return duplicate_file


###################################################################################
def deduplicate_changes_and_create_diff(config, changed_files, file_manifest):
    """ Performs file de-duplication against the previous manifest and works out
    which files need to be uploaded, creating a new diff """

    # For the detection of duplicates we need to hash any newly added files.
    # Also, we sort the file list so it's more logical for the user
    changed_files = sfs.hash_new_files(changed_files, config['base_path'])
    changed_files = sorted(changed_files,key=lambda fle:(os.path.dirname(fle['path']), os.path.basename(fle['path'])))

    # for de-duplication we create an index of the hashes in the previous manifest
    file_hashes_in_previous_manifest = {f['hash'] : f for f in file_manifest['files']}
    file_hashes_in_this_revision = {}


    # Note that we cannot simplify this by multiple assignment to one list
    # as python handles lists by referance not by value
    new_diff       = []
    need_to_upload = []
    new_duplicates = []

    for change in changed_files:
        if change['status'] in ['new', 'changed']:
            msg = colored('Adding: ' + change['path'], 'green')

            # Get the size of a file to check if it is empty. If we cannot get this, the
            # file has probably been deleted since directory contents was listed, so skip it
            local_file_path = sfs.cpjoin(config['base_path'], change['path'])
            try: local_file_size = os.stat(local_file_path).st_size
            except OSError: continue

            # -------------------------------------------------------------
            # If a file is empty it cannot possibly be a duplicate,
            # as empty files cannot be stored in s3
            if local_file_size == 0:
                change['empty'] = True
                new_diff.append(change)

            # If the hash already exists in the previous manifest or has been seen already in the
            # current run, the file has been moved or is a duplicate, don't need to upload it again
            elif change['hash'] in file_hashes_in_previous_manifest:
                msg += colored(' (De-duplicated)', 'yellow')

                duplicate_from_previous_manifest = file_hashes_in_previous_manifest[change['hash']]
                new_diff.append(referance_duplicate_to_master(duplicate_from_previous_manifest, change))

            # new duplicates need to be handled specially as the metadata
            # they need to be referanced to does not exist yet
            elif change['hash'] in file_hashes_in_this_revision:
                msg += colored(' (De-duplicated)', 'yellow')
                new_duplicates.append(change)

            # If the file has not been seen before, it isn't a duplicate and needs uploading
            else:
                need_to_upload.append(change)
                file_hashes_in_this_revision[change['hash']] = change

            print(msg)

        elif change['status'] == 'deleted':
            # skip delete feature
            if sfs.filter_helper(change['path'], config['skip_delete']): continue

            # Delete only removes the file from the manifest, the object needs to remain as it
            # is referenced by prior versions
            print(colored('Deleting: ' + change['path'], 'red'))
            new_diff.append(change)

    return new_diff, need_to_upload, new_duplicates


###################################################################################
def upload_changed_files(interface, conn, config, file_manifest, new_diff, need_to_upload, new_duplicates):
    # Before we actually upload anything, we store the list of what we are about
    # to upload on the remote in order to garbage collect failed uploads without
    # checking every version of the manifest against all existing objects
    if need_to_upload != []:
        gc_changes = [file_to_upload for file_to_upload in need_to_upload
                      if file_to_upload['status'] in ['new', 'changed']]
        write_json_to_remote(config, config['remote_gc_log_file'], gc_changes)

    #--
    new_uploads = {}
    for file_to_upload in need_to_upload:
        local_file_path = sfs.cpjoin(config['base_path'], file_to_upload['path'])

        # Attempt to get the file size to see if the file is empty as s3 does
        # not allow empty objects, and they need special handling. If we
        # cannot obtain this the file has probably been deleted so skip it
        try: local_file_size = os.stat(local_file_path).st_size
        except OSError: continue

        # handle empty files
        if local_file_size == 0:
            print(colored('Warning, empty file: ' + file_to_upload['path'], 'red'))
            file_to_upload['empty'] = True
            new_diff.append(file_to_upload)

            # also log to new uploads so duplicates of these files can be referenced correctly below
            new_uploads[file_to_upload['hash']] = file_to_upload
            continue

        # =========================================================
        print(colored('Uploading: ' + file_to_upload['path'], 'green'))

        upload_metadata = streaming_file_upload(interface, conn, config,
                                                local_file_path, file_to_upload['path'])

        # =========================================================
        # in case name obfuscation will be used, real path stores the obfuscated name
        file_to_upload['real_path']   = file_to_upload['path']
        file_to_upload['version_id']  = upload_metadata['VersionId']
        new_diff.append(file_to_upload)

        # also log to new uploads so duplicates of these files can be referenced correctly below
        new_uploads[file_to_upload['hash']] = file_to_upload

    # process duplicates of new files
    for duplicate_file in new_duplicates:
        master_file = new_uploads[duplicate_file['hash']]
        new_diff.append(referance_duplicate_to_master(master_file, duplicate_file))

    # upload the diff
    upload_metadata = write_json_to_remote(config, config['remote_manifest_diff_file'], new_diff)

    # for some reason have to get the key again to obtain it's time stamp
    last_uploaded_diff = interface.get_object(conn, config['remote_manifest_diff_file'],
                                              version_id = upload_metadata['version_id'])

    # apply the diff to the local manifest
    file_manifest['files'] = sfs.apply_diffs([new_diff], file_manifest['files'])
    file_manifest['latest_remote_diff'] = {
        'version_id' : last_uploaded_diff['version_id'],
        'last_modified' : last_uploaded_diff['last_modified'].isoformat()
    }

    return file_manifest


###################################################################################
def backup(interface, conn, config):
    """ Compares the current state of the local filesystem with a historic state
    stored in a manifest, and uploads the differances to the remote store """

    if 'read_only' in config and config['read_only']: raise SystemExit('read only')

    #Local lock for sanity checking
    lockfile_path = config['local_lock_file']
    lockfile = open(lockfile_path, 'a')
    try: fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError: raise SystemExit('Locked by another process')

    # Scan the local filesystem to obtain its current state
    visit_mountpoints = 'visit_mountpoints' in config and config['visit_mountpoints']

    file_manifest = get_manifest(interface, conn, config)
    current_state, errors = sfs.get_file_list(config['base_path'], config['ignore_files'],
                                              visit_mountpoints = visit_mountpoints)

    # filter ignore files
    #current_state = sfs.filter_file_list(current_state, config['ignore_files'])
    #errors        = sfs.filter_file_list([{'path' : e} for e in errors], config['ignore_files'])

    if errors != []:
        for e in errors: print(colored('Could not read ' + e, 'red'))
        print('--------------')

    #Find changed files
    localy_changed_files = sfs.find_manifest_changes(current_state, file_manifest['files'])

    changed_files_chunked = split_files_changes_into_chunks(config, localy_changed_files)

    # =============================================================================
    for changed_files in changed_files_chunked:
        print('--------------')

        new_diff, need_to_upload, new_duplicates = deduplicate_changes_and_create_diff(config, changed_files, file_manifest)

        file_manifest = upload_changed_files(interface, conn, config, file_manifest, new_diff, need_to_upload, new_duplicates)

        write_local_manifest(config, file_manifest)

        # minimum resolution on s3 timestamps is 1 second, make sure delete marker comes last
        time.sleep(1)

        # delete the garbage collection log, done last in case of crash
        # to avoid leaving garbage objects on the remote
        if need_to_upload != []:
            interface.delete_object(conn, config['remote_gc_log_file'])

        print('--------------')

    # unlock
    fcntl.flock(lockfile, fcntl.LOCK_UN)
    os.remove(lockfile_path)

###################################################################################
def download(interface, conn, config, version_id, target_directory, ignore_filters = None):
    """ Download files from a specified version """

    if 'write_only' in config and config['write_only']: raise SystemExit('write only')

    versions = get_remote_manifest_diffs(interface, conn, config)
    file_manifest = rebuild_manifest_from_diffs(versions, version_id)

    file_manifest['files'] = sorted(file_manifest['files'],key=lambda fle:
        (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

    if ignore_filters is not None:
        for fil in ignore_filters:
            file_manifest['files'] = sfs.filter_f_list(file_manifest['files'], fil)

    # download the objects in the manifest
    for fle in file_manifest['files']:
        print('Downloading: ' + fle['path'])

        local_file_path = sfs.cpjoin(target_directory, fle['path'])

        if 'empty' in fle:
            sfs.make_dirs_if_dont_exist(local_file_path)
            open(local_file_path, 'w').close()
        else:
            remote_file_path = sfs.cpjoin(config['remote_base_path'], fle['real_path'])
            sfs.make_dirs_if_dont_exist(local_file_path)
            streaming_file_download(interface, conn, config, remote_file_path, fle['version_id'], local_file_path)


############################################################################################
def garbage_collect(interface, conn, config, mode='simple'):
    """
    As uploads are made atomic through being referenced from a manifest if an upload fails
    before writing the manifest, such as due to a power failure, unreferenced object versions
    may be left on the remote. This implements garbage collection for these.

    Two modes are available, simple mode uses the gc log uploaded before the objects to detect
    and remove garbage objects. Full performs a complete check of every item in the manifest
    against every object on the remote. Under normal circumstances simple mode is adequate,
    full mode can be very slow and would only be needed under exceptional circumstances probably
    caused by misuse of the application.
    """

    # If the client is in read only mode we cannot perform garbage collection
    if 'read_only' in config and config['read_only']: return

    # If the client is in write only mode, we can perform garbage collection
    # but not in full, garbage objects which are found are appended to
    # a garbage objects list, instead of being deleted.
    is_write_only = False
    if 'read_only' in config and config['read_only']:
        is_write_only = True

    if 'allow_delete_versions' in config and config['allow_delete_versions']:
        is_write_only = True

    # ----------------------------------------------------------------------
    # Perform GC
    # ----------------------------------------------------------------------
    missing_objects = []
    garbage_objects = []

    #---------------
    if mode == 'simple':
        garbage_objects = verify_manifest_with_gc_log(interface, conn, config)

    #---------------
    elif mode == 'full':
        missing_objects, garbage_objects = varify_manifest(interface, conn, config)
        if missing_objects != []: raise SystemExit('Missing objects found')

    #---------------
    else: raise SystemExit('Invalid GC mode')


    #---------------
    delete_garbage_objects(interface, conn, config, garbage_objects, is_write_only)

    # Finally delete the GC log
    interface.delete_object(conn, config['remote_gc_log_file'])


############################################################################################
def verify_manifest_with_gc_log(interface, conn, config):
    gc_log, gc_log_meta = read_json_from_remote(config, config['remote_gc_log_file'])
    if gc_log is None: return []

    #----
    manifest = get_manifest(interface, conn, config)
    manifest_index = {fle['path'] : fle for fle in manifest['files']}

    garbage_objects = []
    for item in gc_log:
        remote_path     = sfs.cpjoin(config['remote_base_path'], item['path'])
        object_versions = interface.list_versions(conn, remote_path)

        latest_version  = None
        if len(object_versions) > 0:
            latest_version = object_versions[-1]

        # Check if the version of the object stored on the remote is newer than the
        # one in the local manifest. If so, the latest remote version is garbage
        if item['path'] in manifest_index:

            # As empty files don't actually get stored on the remote, we don't need to do anything
            # to clean them up if we find one in the GC log.
            if 'empty' in manifest_index[item['path']] and manifest_index[item['path']]['empty']:
                pass

            # If there is a previous version committed, but the upload of a revision failed,
            # the file will be missing on the remote but still exist in the local manifest
            elif latest_version is None:
                pass

            # if it exists, is remote version newer?
            elif(latest_version['VersionId'] != manifest_index[item['path']]['version_id']
                 and latest_version['LastModified'] >= gc_log_meta['last_modified']):
                garbage_objects.append((latest_version['Key'], latest_version['VersionId']))

        # if it does not exist in the prior manifest it's a new addition so the latest version is garbage
        # the latest version was uploaded equal to or later than the timestamp of the GC log. Note that an existing
        # object won't always exist in the prior manifest as it may have been deleted in an earlier version.
        else:
            # not in manifest and no prior versions so upload failed, don't need to do anything.
            if latest_version is None:
                pass

            # else upload of that file succeeded, leaving a garbage object on the remote
            elif latest_version['LastModified'] >= gc_log_meta['last_modified']:
                garbage_objects.append((latest_version['Key'], latest_version['VersionId']))

    return garbage_objects

############################################################################################
def varify_manifest(interface, conn, config):
    """ Check that every item in the manifest actually exists on the remote. """

    # Get every version of every object
    all_objects = {(i['Key'], i['VersionId']) : None for i in interface.list_versions(conn)}

    # get every object and version in every version of the manifest
    manifest_referanced_objects = {}
    for diff in get_remote_manifest_diffs(interface, conn, config):
        for change in json.loads(diff['body']):
            if 'empty' in change and change['empty']: continue

            real_path = sfs.cpjoin(config['remote_base_path'], change['real_path'])
            version_id = change['version_id']

            if (real_path, version_id) not in manifest_referanced_objects:
                manifest_referanced_objects[(real_path, version_id)] = None

    #Add the remote manifest diffs themselves, gc log and salt file as they are not garbage
    for k in all_objects.keys():
        if(k[0] in [config['remote_gc_log_file'], config['remote_manifest_diff_file'],
                    config['remote_garbage_object_log_file'], 'salt_file']):
            manifest_referanced_objects[k] = None

    # Remove objects referenced in the manifest
    missing_objects = []
    for k in manifest_referanced_objects.keys():
        if k not in all_objects: missing_objects.append(k)
        else: del all_objects[k]

    garbage_objects = list(all_objects.keys())
    return missing_objects, garbage_objects


############################################################################################
def delete_garbage_objects(interface, conn, config, garbage_objects, is_write_only):
    if garbage_objects == []: return

    # If have delete permissions, delete the garbage versions of the objects,
    # else append them onto the garbage object log.
    if not is_write_only:
        for item in garbage_objects:
            print(colored('Deleting garbage object: ' + str(item) , 'red'))
            try: interface.delete_object(conn, item[0], version_id = item[1])
            except: print(colored('(Warning) The garbage object has already been deleted.', 'yellow'))
    else:
        for item in garbage_objects:
            print(colored('Appending to garbage object log: ' + str(item) , 'red'))

        log = read_json_from_remote(config, config['remote_garbage_object_log_file'])[0]
        if log is None:
            log = []

        log.append(garbage_objects)

        write_json_to_remote(config, config['remote_garbage_object_log_file'], log)


############################################################################################
def clean_gc_log(interface, conn, config):
    is_write_only = False
    if 'read_only' in config and config['read_only']:
        is_write_only = True

    if 'allow_delete_versions' in config and not config['allow_delete_versions']:
        is_write_only = True

    if is_write_only:
        raise SystemExit("We cannot delete garbage objects from the remote in write only mode, " +
                          "ensure 'allow_delete_versions' is enabled in configuration file")

    gc_log = read_json_from_remote(config, config['remote_garbage_object_log_file'])[0]

    if gc_log is None:
        raise SystemExit("There is no gc log on the remote (nothing to do).")

    flattened_gc_log = [item for a in gc_log for item in a]

    delete_garbage_objects(interface, conn, config, flattened_gc_log, False)

    # Delete the gc log file
    interface.delete_object(conn, config['remote_garbage_object_log_file'])
