import functools, hashlib, time, datetime, fnmatch, os, json, fcntl
import collections
from copy import deepcopy
from termcolor import colored
from pprint import pprint
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
    if 'meta_pipeline' in parsed_config and type(parsed_config['meta_pipeline']) != list: raise ValueError('meta_pipeline in conf file mist be a list')
    if 'file_pipeline' in parsed_config and type(parsed_config['file_pipeline']) != list: raise ValueError('file_pipeline in conf file mist be a list')
    if 'ignore_files' in parsed_config and type(parsed_config['ignore_files']) != list: raise ValueError('ignore_files in conf file mist be a list')
    if 'skip_delete' in parsed_config and type(parsed_config['skip_delete']) != list: raise ValueError('skip_delete in conf file mist be a list')

###################################################################################
def merge_config(config, parsed_config):
    def dict_merge(dct, merge_dct): # recursive dict merge from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
        for k, v in merge_dct.items():
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
    if 'read_only' in config and config['read_only'] == False:
        interface.delete_failed_uploads(conn)
        garbage_collect(interface, conn, config, 'simple')

###################################################################################
def get_remote_manifest_versions(interface, conn, config):
    return list(interface.list_versions(conn, config['remote_manifest_diff_file']))

###################################################################################
def get_remote_manifest_diff(interface, conn, config, version_id = None):
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
    if version_id != None:
        filtered = []; last = None
        for vers in versions:
            filtered.append(vers)
            if vers['version_id'] == version_id:
                last = vers; break
        else:
            raise KeyError('The given version ID does not exist')

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

        try: latest = get_remote_manifest_diff(interface, conn, config)
        except ValueError: raise ValueError('Local manifest exists but remote missing, suspect tampering')

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

                    # Write and move for atomicity
                    sfs.file_put_contents(config['local_manifest_file']+'.tmp', json.dumps(file_manifest))
                    os.rename(config['local_manifest_file']+'.tmp', config['local_manifest_file'])

                    return file_manifest

                else: raise SystemExit('Latest remote manifest does not align with local manifest')
            else: raise SystemExit('Latest remote manifest does not align with local manifest')

        return file_manifest

    except IOError:
        versions = get_remote_manifest_diffs(interface, conn, config)
        if versions != []: return rebuild_manifest_from_diffs(versions)
        else: return new_manifest() # No manifest exists on s3

###################################################################################
from itertools import chain, repeat
def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return zip(*[chain(iterable, repeat(padvalue, n-1))]*n)

def backup(interface, conn, config):
    """ To store data, diff file changes, upload changes and store the diff """

    if 'read_only' in config and config['read_only'] == True: raise Exception('read only')

    #Local lock for sanity checking
    lockfile_path = config['local_lock_file']
    lockfile = open(lockfile_path, 'a')
    try: fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError: raise SystemExit('Locked by another process')

    #----------
    
    visit_mountpoints = 'visit_mountpoints' in config and config['visit_mountpoints'] == True

    file_manifest = get_manifest(interface, conn, config)
    current_state, errors = sfs.get_file_list(config['base_path'], config['ignore_files'],
                                              visit_mountpoints = visit_mountpoints)

    # filter ignore files
    #current_state = sfs.filter_file_list(current_state, config['ignore_files'])
    #errors        = sfs.filter_file_list([{'path' : e} for e in errors], config['ignore_files'])

    if errors != []:
        for e in errors: print(colored('Could not read ' + e['path'], 'red')) 
        print('--------------')

    #Find and process changes
    diff = sfs.find_manifest_changes(current_state, file_manifest['files'])

    if diff !={}:
        diff2 = [change for p, change in diff.items()]

        # Allow diff to be split into chunks to handle large uploads
        chunk_size = config['split_chunk_size'] if 'split_chunk_size' in config else 0

        if chunk_size > 0:
            diff_chunks = grouper(chunk_size, diff2)
        else:
            diff_chunks = [diff2]

        # ==============
        for diff3 in diff_chunks:

            diff = [x for x in diff3 if x is not None] #grouper inserts none if there are insufficient elements to make a full group, need to strip

            diff = sfs.hash_new_files(diff, config['base_path'])
            #-------
            # Move detection disabled for now, it was added for de-duplication, new system does that a lot better.
            # Be careful if this is re-enabled to handle the 'empty' flag correctly
            #-------
            #diff = sfs.detect_moved_files(file_manifest, diff)
            #-------
            diff = sorted(diff,key=lambda fle:(os.path.dirname(fle['path']), os.path.basename(fle['path'])))

            # for de-duplication index hashes from the previous manifest
            hash_index = {f['hash'] : f for f in file_manifest['files']}

            need_to_upload = []; new_diff = []; new_duplicates = [] # these must be assigned to different lists!
            for change in diff:
                if change['status'] in ['new', 'changed']:
                    msg = colored('Adding: ' + change['path'], 'green')

                    # If the hash already exists in the previous manifest or has been seen already in the current run
                    # file been moved or is a duplicate, don't need to upload again
                    if change['hash'] in hash_index :
                        msg += colored(' (De-duplicated)', 'yellow')

                        it = hash_index[change['hash']]
                        if 'empty' in it:
                            change['empty'] = True
                            new_diff.append(change)
                        elif 'name_hashed' in it:
                            change['name_hashed'] = it['name_hashed']
                            change['real_path']   = it['real_path']
                            change['version_id']  = it['version_id']
                            new_diff.append(change)
                        else: # new duplicates need to be handled specially as the above metadata does not exist yet
                            new_duplicates.append(change)
                    else:
                        need_to_upload.append(change)
                        hash_index[change['hash']] = change
                    print(msg)

                elif change['status'] == 'moved':
                    # Moves store the name of the new file but to save space store a pointer to the old file
                    # on the remote. Store as is as details handled by 'detect_moved_files()'.
                    print(colored('Moving: ' + change['path'], 'yellow')) 
                    new_diff.append(change)

                elif change['status'] == 'deleted':
                    # skip delete feature
                    if sfs.filter_helper(change['path'], config['skip_delete']): continue

                    # Delete only removes the file from the manifest, the object needs to remain as it
                    # is referenced by prior versions
                    print(colored('Deleting: ' + change['path'], 'red')) 
                    new_diff.append(change)

            print('--------------')

            # For garbage collection of failed uploads, log new and changed items to s3
            if need_to_upload != []:
                gc_changes = [change for change in need_to_upload if change['status'] == 'new' or change['status'] == 'changed']
                meta = {'path' : config['remote_gc_log_file'], 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
                gc_log = pl_out(json.dumps(gc_changes).encode('utf-8'), meta, config)

            #--
            new_uploads = {}
            for change in need_to_upload:
                fspath = sfs.cpjoin(config['base_path'], change['path'])

                try:
                    stat_result = os.stat(fspath)
                except OSError:
                    # If file no longer exists at this stage assume it has been deleted and ignore it
                    continue 

                # handle empty files
                if stat_result.st_size == 0:
                    print(colored('Warning, empty file: ' + change['path'], 'red'))
                    change['empty'] = True
                    new_diff.append(change);

                     # also log to new uploads so duplicates of these files can be referenced correctly below
                    new_uploads[change['hash']] = change

                # normal files
                else:
                    print(colored('Uploading: ' + change['path'], 'green'))

                    #Determine the correct pipeline format to use for this file from the configuration
                    try: matched_plf = next((plf for wildcard, plf in config['file_pipeline'] if fnmatch.fnmatch(change['path'], wildcard)))
                    except StopIteration: raise 'No pipeline format matches '

                    # Get remote file name and implementation of hash path
                    path_hash = hashlib.sha256(change['path'].encode('utf8')).hexdigest() if 'hash_names' in matched_plf else change['path']
                    remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)

                    #----
                    pl_format = pipeline.get_default_pipeline_format()
                    pl_format['chunk_size'] = config['chunk_size']
                    pl_format['format'] = {i : None for i in matched_plf}
                    if 'encrypt' in pl_format['format']: pl_format['format']['encrypt'] = config['crypto']['encrypt_opts']

                    #-----
                    upload = interface.streaming_upload()
                    pl     = pipeline.build_pipeline_streaming(upload, 'out')

                    pl.pass_config(config, pipeline.serialise_pipeline_format(pl_format))

                    upload.begin(conn, remote_path, )

                    try:
                        with open(fspath, 'rb') as fle:
                            while True:
                                print('here')

                                chunk = fle.read(config['chunk_size'])
                                if chunk == b'': break
                                pl.next_chunk(chunk)
                        res = upload.finish()

                    except IOError: 
                        # If file no longer exists at this stage assume it has been deleted and ignore it
                        upload.abort()
                        continue


                    change['name_hashed'] = 'hash_names' in matched_plf
                    change['real_path']   = change['path']
                    change['version_id']  = res['VersionId']
                    new_diff.append(change);

                     # also log to new uploads so duplicates of these files can be referenced correctly below
                    new_uploads[change['hash']] = change

            print('------------------')

            # process duplicates of new files
            for change in new_duplicates:
                it = new_uploads[change['hash']]
                if 'empty' in it:
                    change['empty'] = True
                    new_diff.append(change)
                else:
                    change['name_hashed'] = it['name_hashed']
                    change['real_path']   = it['real_path']
                    change['version_id']  = it['version_id']
                    new_diff.append(change)

            # upload the diff
            meta = {'path' : config['remote_manifest_diff_file'], 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
            meta2 = pl_out(json.dumps(new_diff).encode('utf-8'), meta, config)

            # for some reason have to get the key again to obtain it's time stamp
            k = interface.get_object(conn, config['remote_manifest_diff_file'], version_id = meta2['version_id'])

            # apply the diff to the local manifest and update it
            file_manifest['files'] = sfs.apply_diffs([new_diff], file_manifest['files'])
            file_manifest['latest_remote_diff'] = {'version_id' : k['version_id'], 'last_modified' : k['last_modified'].isoformat()}

            # Write and move for atomicity
            sfs.file_put_contents(config['local_manifest_file']+'.tmp', json.dumps(file_manifest))
            os.rename(config['local_manifest_file']+'.tmp', config['local_manifest_file'])

            # delete the garbage collection log
            #time.sleep(1) # minimum resolution on s3 timestamps is 1 second, make sure delete marker comes last

            if need_to_upload != []:
                interface.delete_object(conn, config['remote_gc_log_file'])

        # unlock
        fcntl.flock(lockfile, fcntl.LOCK_UN)
        os.remove(lockfile_path)

###################################################################################
def download(interface, conn, config, version_id, target_directory, ignore_filters = None):
    """ Download files from a specified version """

    if 'write_only' in config and config['write_only'] == True: raise Exception('write only')

    versions = get_remote_manifest_diffs(interface, conn, config)
    file_manifest = rebuild_manifest_from_diffs(versions, version_id)

    file_manifest['files'] = sorted(file_manifest['files'],key=lambda fle:
        (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

    if ignore_filters != None:
        for fil in ignore_filters:
            file_manifest['files'] = sfs.filter_f_list(file_manifest['files'], fil)

    # download the objects in the manifest
    for fle in file_manifest['files']:
        print('Downloading: ' + fle['path'])

        dest = sfs.cpjoin(target_directory, fle['path'])

        if 'empty' in fle:
            open(dest, 'a').close()
        else:
            path_hash = hashlib.sha256(fle['real_path'].encode('utf8')).hexdigest() if fle['name_hashed'] == True else fle['real_path']
            remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)

            download          = interface.streaming_download()
            header, pl_format = download.begin(conn, remote_path, fle['version_id'])
            pl                = pipeline.build_pipeline_streaming(download, 'in')
            pl.pass_config(config, header)

            sfs.make_dirs_if_dont_exist(dest)
            with open(dest, 'wb') as fle:
                while True:
                    res = pl.next_chunk()
                    if res == None: break
                    fle.write(res)

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

    if 'read_only' in config and config['read_only'] == True: return

    missing_objects = garbage_objects = []

    if mode == 'simple':
        meta = {'path'       : config['remote_gc_log_file'],
                'version_id' : None,
                'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
        try: data, gc_log_meta = pl_in(meta, config)
        except ValueError: return

        gc_log = json.loads(data)

        #----
        manifest = get_manifest(interface, conn, config)
        index = {fle['path'] : fle for fle in manifest['files']}

        garbage_objects = []
        for item in gc_log:
            path_hash = hashlib.sha256(item['path'].encode('utf8')).hexdigest()
            remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)
            vers = interface.list_versions(conn, remote_path)



            #if this exists in the previous manifest, see if a newer version exists, if so it is garbage
            if item['path'] in index:

                # As empty files don't actually get stored on the remote, we don't need to do anything
                # to clean them up if we find one in the GC log.
                if 'empty' in index[item['path']] and index[item['path']]['empty'] == True:
                    pass

                elif vers[-1]['VersionId'] != index[item['path']]['version_id'] and vers[-1]['LastModified'] >= gc_log_meta['last_modified']:
                    garbage_objects.append((vers[-1]['Key'], vers[-1]['VersionId']))

            # if it does not exist in the prior manifest it's a new addition so the latest version is garbage
            # the latest version was uploaded equal to or later than the timestamp of the GC log. Note that an existing
            # object won't always exist in the prior manifest as it may have been deleted in an earlier version.
            else:
                if vers == []: pass # not in manifest and no prior versions so upload failed, don't need to do anything.
                elif vers[-1]['LastModified'] >= gc_log_meta['last_modified']: garbage_objects.append((vers[-1]['Key'], vers[-1]['VersionId']))

    #---------------
    elif mode == 'full':
        missing_objects, garbage_objects = varify_manifest(interface, conn, config)
        if missing_objects != []: raise ValueError('Missing objects found')

    #---------------
    else: raise ValueError('Invalid GC mode')

    # If have delete permissions, delete the garbage versions of the objects,
    # else append them onto the garbage object log.
    if 'allow_delete_versions' in config and config['allow_delete_versions'] == True:
        for item in garbage_objects:
            print(colored('Deleting garbage object: ' + str(item) , 'red'))
            interface.delete_object(conn, item[0], version_id = item[1])
    else:
        for item in garbage_objects:
            print(colored('Appending to garbage object log: ' + str(item) , 'red'))

        meta = {'path'       : config['remote_garbage_object_log_file'],
                'version_id' : None,
                'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
        try:
            data, gc_log_meta = pl_in(meta, config)
            log = json.loads(data)
        except ValueError:
            log = []

        log.append(garbage_objects)
        meta = {'path' : config['remote_garbage_object_log_file'], 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
        meta2 = pl_out(json.dumps(log), meta, config)

    # Finally delete the GC log
    interface.delete_object(conn, config['remote_gc_log_file'])


############################################################################################
def varify_manifest(interface, conn, config):
    """ Check that every item in the manifest actually exists on the remote. """

    # Get every version of every object
    all_objects = {(i['Key'], i['VersionId']) : None for i in interface.list_versions(conn)}

    # get every object and version in every version of the manifest
    manifest_referanced_objects = {}
    for diff in get_remote_manifest_diffs(interface, conn, config):
        for fle in json.loads(diff['body']):
            path_hash = hashlib.sha256(fle['real_path'].encode('utf8')).hexdigest()
            real_path = sfs.cpjoin(config['remote_base_path'], path_hash)
            version_id = fle['version_id']
            if (real_path, version_id) not in manifest_referanced_objects:
                manifest_referanced_objects[(real_path, version_id)] = None

    #Add the remote manifest diffs themselves, gc log and salt file as they are not garbage
    for k in all_objects.keys():
        if k[0] in ['salt_file', config['remote_gc_log_file'], config['remote_manifest_diff_file']]:
            manifest_referanced_objects[k] = None

    # Remove objects referenced in the manifest
    missing_objects = []
    for k in manifest_referanced_objects.keys():
        if k not in all_objects: missing_objects.append(k)
        else: del all_objects[k]

    garbage_objects = [o for o in all_objects.keys()]
    return missing_objects, garbage_objects

