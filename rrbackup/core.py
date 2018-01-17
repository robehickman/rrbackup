from boto.s3.connection import S3Connection
from copy import deepcopy
from boto.s3.key import Key
from termcolor import colored
import shttpfs.common as sfs
import boto.utils, os, json
from pprint import pprint
import rrbackup.pipeline as pipeline
import functools, hashlib, time, datetime

###################################################################################
def default_config():
    """ The default configuration structure. """
    return { 'base_path'                 : None,             # The root from where the backup is performed
             'remote_manifest_diff_file' : 'manifest_diffs', # Location of the remote manifest diffs
             'remote_gc_log_file'        : 'gc_log',         # Location of the remote garbage collection log
             'remote_base_path'          : 'files',          # The directory used to store files on S3
             'local_manifest_file'       : 'manifest',       # Name of the local manifest file
             'chunk_size'                : 1048576 * 5}      # minimum chunk size is 5MB on s3, 1mb = 1048576

###################################################################################
def new_manifest():
    """ The structure of the locally stored manifest, and manifest data
     within this program.
    """

    return { 'latest_remote_diff' : {},
             'files'              : []}

###################################################################################
meta_pl_format = None
def init(interface, conn, config):
    """ Set up format of the pipeline used for storing meta-data like manifest diffs """
    global meta_pl_format
    meta_pl_format = pipeline.get_default_pipeline_format()
    meta_pl_format['format'].update({'compress'   : None,
                                     'encrypt'    : config['encrypt_opts']})

    # Check for previous failed uploads and delete them
    interface.delete_failed_uploads(conn)
    garbage_collect(interface, conn, config, 'simple')

###################################################################################
def get_remote_manifest_versions(interface, conn, config):
    return list(interface.list_versions(conn, config['remote_manifest_diff_file']))

###################################################################################
def get_remote_manifest_diff(interface, conn, config, version_id = None):
    i_in = functools.partial(interface.read_file, conn)
    pl_in = pipeline.build_pipeline(i_in, 'in', meta_pl_format['format'])

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

    versions = get_remote_manifest_versions(interface, conn, config)

    i_in = functools.partial(interface.read_file, conn)
    pl_in = pipeline.build_pipeline(i_in, 'in', meta_pl_format['format'])
    # TODO instead of building pipeline every time, always have the whole pipeline loaded and have if blocks in each section of the pipeline itself to see if it's needs to be applied

    diffs = []
    for v in versions:
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
        except Exception: raise ValueError('Local manifest exists but remote missing, suspect tampering')

        if file_manifest['latest_remote_diff']['last_modified'] != latest['last_modified'].isoformat():
            # If the client where to crash between writing the remote diff and local manifest the remote manifest
            # will be one version ahead of the local. Handle this transparently by rebuilding the local manifest.
            # Under normal circumstances the remote should never be more than one diff ahead.
            local_manifest_time = datetime.datetime.strptime(file_manifest['latest_remote_diff']['last_modified'].replace('T', ' ').split('+')[0], '%Y-%m-%d %H:%M:%S')
            remote_diff_time    = latest['last_modified'].replace(tzinfo=None)
            if remote_diff_time > local_manifest_time:
                diffs = get_remote_manifest_diffs(interface, conn, config)
                if local_manifest_time == diffs[-2]['meta']['last_modified'].replace(tzinfo=None):
                    print 'Remote is one diff ahead of local, updating local manifest'
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
def backup(interface, conn, config):
    """ To store data, diff file changes, upload changes and store the diff """

    if 'read_only' in config: raise Exception('read only')

    file_manifest = get_manifest(interface, conn, config)
    current_state = sfs.get_file_list(config['base_path'])
    diff = sfs.find_manifest_changes(current_state, file_manifest['files'])

    if diff !={}:
        diff = [change for p, change in diff.iteritems()]
        diff = sfs.hash_new_files(diff, config['base_path'])
        diff = sfs.detect_moved_files(file_manifest, diff)
        diff = sorted(diff,key=lambda fle:(os.path.dirname(fle['path']), os.path.basename(fle['path'])))

        # For garbage collection of failed uploads, log new and changed items to s3
        gc_changes = [change for change in diff if change['status'] == 'new' or change['status'] == 'changed']

        i_out = functools.partial(interface.write_file, conn)
        pl_out = pipeline.build_pipeline(i_out, 'out', meta_pl_format['format'])
        meta = {'path' : config['remote_gc_log_file'], 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
        gc_log = pl_out(json.dumps(gc_changes), meta, config)

        #--
        new_diff = []
        for change in diff:
            if change['status'] == 'new' or change['status'] == 'changed':
                print colored('Uploading: ' + change['path'], 'green') 

                fspath = sfs.cpjoin(config['base_path'], change['path'])

                path_hash = hashlib.sha256(change['path']).hexdigest()
                remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)
                if os.stat(fspath).st_size == 0: print colored('Warning, skipping empty file: ' + change['path'], 'red'); continue

                upload = interface.streaming_upload(conn, remote_path, config['chunk_size'])
                pl     = pipeline.build_pipeline_streaming(upload, 'out', ['encrypt'], config)
                upload.begin()
                with open(fspath, 'rb') as fle:
                    while True:
                        chunk = fle.read(config['chunk_size'])
                        if chunk == "": break
                        pl.next_chunk(chunk)

                res = upload.finish()

                change['real_path'] = change['path']
                change['version_id'] = res['VersionId']
                new_diff.append(change)

            elif change['status'] == 'moved':
                # Moves store the name of the new file but to save space store a pointer to the old file
                # on the remote. Store as is as details handled by 'detect_moved_files()'.
                print colored('Moving: ' + change['path'], 'yellow') 
                new_diff.append(change)

            elif change['status'] == 'deleted':
                # Delete only removes the file from the manifest, the object needs to remain as it
                # is referenced by prior versions
                print colored('Deleting: ' + change['path'], 'red') 
                new_diff.append(change)

        # upload the diff
        i_out = functools.partial(interface.write_file, conn)
        pl_out = pipeline.build_pipeline(i_out, 'out', meta_pl_format['format'])
        meta = {'path' : config['remote_manifest_diff_file'], 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
        meta2 = pl_out(json.dumps(new_diff), meta, config)

        # for some reason have to get the key again to obtain it's time stamp
        k = interface.get_object(conn, config['remote_manifest_diff_file'], version_id = meta2['version_id'])

        # apply the diff to the local manifest and update it
        file_manifest['files'] = sfs.apply_diffs([new_diff], file_manifest['files'])
        file_manifest['latest_remote_diff'] = {'version_id' : k['version_id'], 'last_modified' : k['last_modified'].isoformat()}

        # Write and move for atomicity
        sfs.file_put_contents(config['local_manifest_file']+'.tmp', json.dumps(file_manifest))
        os.rename(config['local_manifest_file']+'.tmp', config['local_manifest_file'])

        # delete the garbage collection log
        time.sleep(1) # minimum resolution on s3 timestamps is 1 second, make sure delete marker comes last

        interface.delete_object(conn, config['remote_gc_log_file'])

###################################################################################
def download(interface, conn, config, version_id, target_directory, ignore_filters = None):
    """ Download files from a specified version """

    versions = get_remote_manifest_diffs(interface, conn, config)
    file_manifest = rebuild_manifest_from_diffs(versions, version_id)

    file_manifest['files'] = sorted(file_manifest['files'],key=lambda fle:
        (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

    if ignore_filters != None:
        for fil in ignore_filters:
            file_manifest['files'] = sfs.filter_f_list(file_manifest['files'], fil)

    # download the objects in the manifest
    for fle in file_manifest['files']:
        print 'Downloading: ' + fle['path']

        path_hash = hashlib.sha256(fle['real_path']).hexdigest()
        remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)

        download = interface.streaming_download(conn, remote_path2, fle['version_id'], config['chunk_size'])
        pl     = pipeline.build_pipeline_streaming(download, 'in', ['encrypt'], config)

        dest = sfs.cpjoin(target_directory, fle['path'])
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

    missing_objects = garbage_objects = []

    if mode == 'simple':
        i_in = functools.partial(interface.read_file, conn)
        pl_in = pipeline.build_pipeline(i_in, 'in', meta_pl_format['format'])

        meta = {'path'       : config['remote_gc_log_file'],
                'version_id' : None,
                'header'     : pipeline.serialise_pipeline_format(meta_pl_format)}
        try: data, gc_log_meta = pl_in(meta, config)
        except ValueError: return

        gc_log = json.loads(data)

        #----
        manifest = rebuild_manifest_from_diffs(get_remote_manifest_diffs(interface, conn, config))
        index = {fle['path'] : fle for fle in manifest['files']}

        garbage_objects = []
        for item in gc_log:
            path_hash = hashlib.sha256(item['path']).hexdigest()
            remote_path = sfs.cpjoin(config['remote_base_path'], path_hash)
            vers = interface.list_versions(conn, remote_path)

            #if this exists in the previous manifest, see if a newer version exists, if so it is garbage
            if item['path'] in index:
                if vers[-1]['VersionId'] != index[item['path']]['version_id'] and vers[-1]['LastModified'] >= gc_log_meta['last_modified']:
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

    #---------------
    else: raise ValueError('Invalid GC mode')

    # TODO, if have delete permissions, delete the garbage versions of the objects,
    # else concat tham onto the 'to delete' log.
    for item in garbage_objects:
        print 'deleting garbage object: ' + str(item)
        interface.delete_object(conn, item[0], item[1])

    # Finally delete the GC log
    interface.delete_object(conn, config['remote_gc_log_file'])

    if missing_objects != []: raise ValueError('Missing objects found')

############################################################################################
def varify_manifest(interface, conn, config):
    """ Check that every item in the manifest actually exists on the remote. """

    # Get every version of every object
    all_objects = {(i['Key'], i['VersionId']) : None for i in interface.list_versions(conn)}

    # get every object and version in every version of the manifest
    manifest_referanced_objects = {}
    for diff in get_remote_manifest_diffs(interface, conn, config):
        for fle in json.loads(diff['body']):
            path_hash = hashlib.sha256(fle['real_path']).hexdigest()
            real_path = sfs.cpjoin(config['remote_base_path'], path_hash)
            version_id = fle['version_id']
            if (real_path, version_id) not in manifest_referanced_objects:
                manifest_referanced_objects[(real_path, version_id)] = None

    #Add the remote manifest diffs themselves, gc log and salt file as they are not garbage
    for k in all_objects.iterkeys():
        if k[0] in ['salt_file', config['remote_gc_log_file'], config['remote_manifest_diff_file']]:
            manifest_referanced_objects[k] = None

    # Remove objects referenced in the manifest
    missing_objects = []
    for k in manifest_referanced_objects.iterkeys():
        if k not in all_objects: missing_objects.append(k)
        else: del all_objects[k]

    garbage_objects = [o for o in all_objects.iterkeys()]
    return missing_objects, garbage_objects

