# This defines a system for storing a file manifest on amazon S3. This is
# needed when the remote objects and there names are encrypted. The manifest
# stores a copy of the original file names. Additionally the manifest works
# in harmony with the S3 versioning system. File updates create new versions,
# at the end of a 'commit' the manifest is updated to point to these latest
# versions. Consequently a failed update will not leave the data in an
# inconsistent state. The manifest will always point to the last good version.
#
# The primary use-case for this is a backup system. Files are added or updated
# infrequently and only a small number are changed at once. The manifest
# could store thousands of files. Because s3 does not allow delta update of
# objects storing the manifest as a single blob would be inefficient.  The
# whole thing would have to be downloaded and # reuploaded even if only a
# single file is changed.
#
# In my use case file changes tend to happen in one directory at a time,
# in order to minimize the number of manifests this changes files are sorted
# by directory. Changing files within a single directory will only require
# updating a one, or a small number of the remote manifest files.
#
# The maximum size of any single manifest file is configurable, and the manifest
# is split when this size is exceeded. Splits attempt to maintain the order
# by inserting the additional manifest before or after the one it would have
# filled, had the maximum size not been exceeded.
#
# All files are encrypted under the same key. Should the manifest be lost
# data will still be accessible, though structure and file names will
# be lost
#
# Manifest files will be numbered linearly. As files are added to the manifest,
# and it becomes full, a new manifest will be created as 1 plus the highest
# manifest number, but will be inserted in the middle. This ordering is maintained
# by the master manifest.

# All files are encrypted under the same key. Should the manifest be lost
# data will still be accessible, though structure and file names will
# be lost
#


# Store manifest on remote as a succession of diffs
# Create diff relative to current state
# Current state can be rebuilt by applying diffs forward/backward
# only changes are stored so minimal additional upload overhead



from pprint import pprint
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from collections import defaultdict
from shttpfs.common import *
from copy import deepcopy
from sets import Set
import manifest_data_structure as mds

import boto.utils
import hashlib
import math


############################################################################################
def js_keys(from_json):
    """ Filters data returned from json handler and converts numeric keys into numbers """
    def get_value(value):
        if hasattr(value, '__iter__'): return js_keys(value)
        else: return value
    def convert_key(key):
        try: return int(key)
        except: return key
    if type(from_json) == dict:
        return {convert_key(k) : get_value(v) for k, v in from_json.iteritems()}
    elif type(from_json) == list:
        return [get_value(value) for value in from_json] 


############################################################################################
def connect(access_key, secret_key, bucket):
    conn   = S3Connection(access_key, secret_key)
    bucket = conn.get_bucket(bucket)

    # Make sure that versioning is enabled
    status = bucket.get_versioning_status(headers=None)
    if status == [] or status['Versioning'] != 'Enabled':
        print 'Bucket versioning must be enabled, attempting to enable, please restart application'
        bucket.configure_versioning(True)
        raise SystemExit(0)

    return conn, bucket

############################################################################################
def s3_get_object(bucket, name, error='object not found', version_id=None):
    k = bucket.get_key(name, version_id = version_id)
    if k == None: raise Exception(error)
    return k

############################################################################################
def s3_put_object(bucket, name, contents):
    k = Key(bucket)
    k.key = name
    k.set_contents_from_string(contents)
    return k

############################################################################################
def s3_put_existing_object(bucket, name, contents, error='object not found'):
    k = bucket.get_key(name)
    if k == None: raise Exception(error)
    k.set_contents_from_string(contents)
    return k

############################################################################################
def get_remote_manifest(config, bucket, version_id = None, debug=False):
    """ Get the manifest as it is on s3 """

    master_manifest_name = config['master_manifest'] + config['manifest_ext']
    k = bucket.get_key(master_manifest_name, version_id = version_id)

    # Attempt to get manifest
    if k != None:
        pass
        """
        print 'Getting manifest'

        # decode main manifest
        manifest = js_keys(json.loads(k.get_contents_as_string()))
 
        #decode the sub manifests and join them
        for i in range(0, manifest['sub_manifest_count']):
            manifest_name = config['subman_prefix'] + str(i) + config['manifest_ext']
            key = s3_get_object(bucket, manifest_name,
                version_id = manifest['sub_manifests'][i]['version_id'],
                error      = 'Manifest part not found')

            submanifest_files = js_keys(json.loads(key.get_contents_as_string()))

            manifest['sub_manifests'][i]['files'] = submanifest_files['files']

        return manifest
        """
 
    # If no manifest, create one
    else:
        print 'Creating manifest'

        manifest = mds.new_manifest()
        """
        # create sub manifests on the remote and store there version ids
        for i in range (0, manifest['sub_manifest_count']):
            object_name = config['subman_prefix'] + str(i) + config['manifest_ext']
            k = s3_put_object(bucket, object_name, json.dumps({'files' : []}))
            manifest['sub_manifests'][i] = {
                'version_id' : k.version_id,
                'files'      : []}

        # create master manifest on the remote
        s3_put_object(bucket, master_manifest_name, json.dumps(manifest))
        """

        return force_unicode(manifest)


############################################################################################
def update_remote_manifest(config, manifest, new_manifest):
    """ Update the contents of the remote manifest """

    # As pythons data structures are not immutable ALWAYS work on a copy of manifest.
    # A failed update could leave the local manifest in an inconsistent state.
    local_manifest = deepcopy(manifest)

    # Find the parts of the manifest that have changed so only things that need updating are uploaded
    #TODO need some way of identifying which manifest sections are related

    # Perform this update
    """
    # Add the new files to the relevant manifests, this only updates things which have changed
    # TODO this should replace rather than duplicate items which have the same path as items already in the manifest
    manifest_version_ids = defaultdict(dict)
    for f_key, f_value in bins.iteritems():
        local_manifest['sub_manifests'][f_key]['files'] += f_value

        s3_key = s3_put_existing_object(bucket,
            name     = config['subman_prefix'] + str(f_key) + config['manifest_ext'],
            contents = json.dumps(local_manifest['sub_manifests'][f_key]),
            error    = 'object not found')

        local_manifest['sub_manifests'][f_key]['version_id'] \
            = manifest_version_ids[f_key]['version_id'] \
            = s3_key.version_id

    # Always write manifest master file last. If anything above fails the master on s3 will point
    # to the prior versions of the sub manifests. Failures will not corrupt the manifest
    # data structure on s3.
    master_manifest_name = config['master_manifest'] + config['manifest_ext']
    s3_put_object(bucket, master_manifest_name, json.dumps({
        'sub_manifest_count' : manifest['sub_manifest_count'],
        'sub_manifests'      : dict(manifest_version_ids)}))
    """

    return local_manifest



############################################################################################
def upload_files(config, bucket, manifest, files):
    #upload files, keeping track of there remote version_id

    file_versions = []
    for fle in files:
        print fle['path']
        k = Key(bucket); k.key = fle['path']
        k.set_contents_from_string(file_get_contents(cpjoin('./bk_dir/', fle['path'])))
        fle['version_id'] = k.version_id
        file_versions.append(fle)

    #update the manifest
    print file_versions

    new_manifest = mds.add_to_manifest_2(config, manifest, files)

    print new_manifest 

    quit()

    return update_remote_manifest(config, manifest, new_manifest)


############################################################################################
def download_files(config, manifest, destination):
    """ Download everything from remote """

    for subman in manifest['sub_manifests'].values():
        for fle in subman['files']:
            k = s3_get_object(bucket, fle['path'], error='object not found')

            contents = k.get_contents_as_string()

            print fle['path']

            path = cpjoin('./down_dir/', fle['path'])
            make_dirs_if_dont_exist(path)
            file_put_contents(path, contents)

############################################################################################
def rebuild_manifest(config, bucket, manifest):
    """
    Perform automatic optimisation of manifest file sizes
    rebuild automatically when the size of the manifests exceeds a certain size.
    """

    #determine the sizes of largest sub manifests
    sizes = []
    for f_key, man in manifest['sub_manifests'].iteritems():
        name = config['subman_prefix'] + str(f_key) + config['manifest_ext']
        k = s3_get_object(bucket, name, version_id = man['version_id'])

        if(k.size > config['max_manifest_size']):
            sizes.append(k.size)

    # if there are manifests over the maximum size do a rebuild
    if(sizes != []):
        print 'optimising manifests'

        # determine how many manifest files to use
        no_manifests = math.ceil(float(max(sizes)) / float(config['max_manifest_size'] / 2))

        #merge all items listed in current sub manifests
        files = []
        for subman in manifest['sub_manifests'].values():
            for fle in subman['files']:
                files.append(fle)


        #rebuild manifest bins
        bins = defaultdict(list)
        for val in files:
            idx = int(int(hashlib.sha1(val['path']).hexdigest(), 16) % no_manifests)
            bins[idx].append(val)

        pprint(dict(bins))

        # replace manifests with new manifests

        # if less than previous, create delete markers on excess manifests

        # update master manifest



############################################################################################
def remove_files(config, manifest, files):
    """ Remove files from the current version on the remote """

    # search for file in manifest

    # remove from manifest

    # remove from s3

    pass


############################################################################################
def garbage_collect(config, bucket):
    """
    Implement garbage collection for failed uploads, make sure every object on
    the remote has a pointer from a manifest
    """

    # Get all objects
    all_objects = Set()
    for version in bucket.list_versions():
        all_objects.add((version.key, version.version_id))

    # Get all manifests and add the objects they reference to 'not garbage' list
    master_manifest_name = config['master_manifest'] + config['manifest_ext']
    manifest_versions = bucket.list_versions(prefix=master_manifest_name)

    manifests = []
    all_manifest_objects = Set()
    for result in manifest_versions:
        manifest = get_manifest(config, bucket, version_id = result.version_id, debug=True)
        manifests.append((result, manifest))
        for subman in manifest['sub_manifests'].values():
            for fle in subman['files']:
                all_manifest_objects.add((fle['path'][1:], fle['version_id']))

    # add the manifests themselves to the 'not garbage' list
    for manifest in manifests:
        all_manifest_objects.add((manifest[0].key, manifest[0].version_id))
        for f_key, subman in manifest[1]['sub_manifests'].iteritems():
            name = force_unicode(config['subman_prefix'] + str(f_key) + config['manifest_ext'])
            all_manifest_objects.add((name, subman['version_id']))

    # TODO unicode and non unicode 'files' key getting into manifest somehow


    print '--------------'
    print all_objects
    print '--------------'
    print all_manifest_objects

    # Find any objects which are on s3 but not in the manifest
    diff = Set(all_objects) - Set(all_manifest_objects) 

    print '--------------'

    print diff

    # anything left over is garbage from failed upload
    # offer option to remove it or add to a new version of the manifest
