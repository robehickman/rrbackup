from shttpfs.common import *
from copy import deepcopy
from pprint import pprint
from operator import itemgetter
from itertools import groupby

"""
This is part of a system for backing up files to AWS S3. S3 has object versioning but
it does not have any means of linking a set of objects as an atomic commit. In order
to achieve this a file manifest is stored which works sympathetically with S3 object
versioning. Details of which are documented in s3test_split_manifest.py. This file
implements the data structure of this manifest and is implemented as a collection of
referentially transparent functions.

As s3 does not permit partial updates storing this manifest as a single remote object
would entail uploading the whole manifest on every backup. This manifest could be
arbitrarily large depending on the number of files stored, so doing this entails
considerable overhead. Because of this the manifest is split into smaller chunks.
The size of these chunks in KB is defined in the below structure.
"""

def get_default_config():
    return { 'master_manifest'   : 'manifest_master',
             'subman_prefix'     : 'manifests/manifest_',
             'manifest_ext'      : '.json',
             'max_manifest_size' : 10000  }

"""
The manifest is stored as two parts, a collection of chunks stored under 'manifests/'
and an index, 'manifest_master'. The chunks only contain file paths, stored as a
list of dictionary's. This dict must contain a key 'path', the path of the file, and
may contain arbitrary data as other keys.

The file paths are sorted in alphabetical order within the manifest chunks. At least
in my own use case file additions and changes tend to happen within a single directory.
Storing the paths sorted reduces the number of manifest chunks which need to be updated
on the remote. However it will require more chunks than optimal.

In order to maintain this ordering, when a chunk overflows an additional chunk is
inserted immediately after it. Starting with chunks 1 and 2, if 1 overflows an
new chunk is inserted following 1 which becomes chunk 2, and chunk 2 becomes
chunk 3.

For the sake of simplicity these chunks are stored on S3 as a linear sequence, 
manifest_1, 2, 3 etc. The inserted chunk '2' above would actually be backed by
file 3 on the remote, allocated linearly after file 2. An index in master_manifest
is used to keep the chunks in order and map them to the files they are stored in.

Part count is the number of parts in the manifest. New chunks are always allocated
after this. Chunks may be deleted if they become empty.
"""

def new_manifest(submanifests = []):
    """ Create a new manifest structure """
    return { 'part_count'     : 0,
             'manifest_parts' : [],
             'index'          : []}

"""
Below is the structure of index items, chunk file is the file which stores the
chunk. Manifest_order is where it appears in the chunk sequence. Version_id
is the version ID of the S3 object which stores the chunk.
"""

def new_manifest_index_item(chunk_file, manifest_order, version_id) {
    return { 'manifest__chunk_file' : chunk_file,
             'manifest_order'       : manifest_order,
             'version_id'           : version_id}
}

############################################################################################
def build_manifest(config, new_files):
    """ Build a new split manifest from a collection of files """

    new_files = sorted(new_files, key=lambda fle: (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

    manifest = []
    manifests = 0
    manifest_part = []


    for fle in new_files:
        manifest_part.append(fle)
        if get_serialised_size(manifest_part) > config['max_manifest_size']:
            manifests += 1;
            manifest.append(manifest_part)
            manifest_part = []

            #TODO need to update index
            new_manifest_index_item()

    if manifest_part != []:
        manifest.append(manifest_part)

    return manifest


############################################################################################
def add_to_manifest(config, manifest, new_files):
    """ Add multiple items to the manifest """
    if manifest == []: return build_manifest(config, new_files)

    manifest = deepcopy(manifest)

    # flatten the manifest but keep track of what section each path was in
    items = [(item['path'], i) for i, chunk in enumerate(manifest) for item in chunk]

    # add the new files and sort it, the index of the new item is the manifest file it
    # needs to go in
    items = items + [(fle['path'], 'new', fle) for fle in new_files]
    items = sorted(items, key=lambda fle: (os.path.dirname(fle[0]), os.path.basename(fle[0])))

    # group items with sequential indices
    filtered = [(i, item) for i, item in enumerate(items) if item[1] == 'new']
    groups = [map(itemgetter(1), g) for k, g in groupby(enumerate(filtered), lambda (a,b):a-b[0])]

    # The below code mutates the manifest, adding the files in each section one at a time, inserting
    # additional splits in the manifest if the maximum size is exceeded. Because of this we need
    # to create a mapping between the original and mutated state of the manifest.
    mapper = {i : i for i, sect in enumerate(manifest)}

    for group in groups:
        group_files = [i[1][2] for i in group]

        # Get the index of the previous and following manifest
        prev_m = get_if_set(items, group[0][0]  - 1, (0, None))[1]
        next_m = get_if_set(items, group[-1][0] + 1, (0, None))[1]
        options  = [x for x in [prev_m, next_m] if x is not None]

        # if the previous and next manifest are the same it was inserted in the
        # middle of a manifest, else it was inserted at the start / end of one
        if prev_m == next_m and not get_serialised_size(manifest[options[0]] + group_files) > config['max_manifest_size']:
            avalible = [options[0]]
        else:
            avalible = [opt for opt in options if not get_serialised_size(manifest[opt] + group_files) > config['max_manifest_size']]

        # Decide which target manifest to use, if one with space is available use it
        if avalible != []:
            target = avalible[0]
        # Start of list
        elif prev_m == None and next_m != None:
            target = -1
        # Middle or end of list
        else: 
            target = prev_m

        # insert the items. If the is space in the manifest, add the file  if it is full, add another manifest file.
        # new files are allocated sequentially and is kept in sequence by the index. TODO Need to keep track of this
        if target == -1:
            manifest = [] + manifest; target = 0
        else:
            target = mapper[target]

        #TODO need to update index

        incremented = target
        for item in group_files:
            if get_serialised_size(manifest[incremented] + [item]) > config['max_manifest_size']:
                manifest.insert(incremented + 1, [])
                incremented += 1 
            manifest[incremented].append(item)

        # If one or more additional manifest sections have been inserted, update the mapper to reflect this
        if incremented > target:
            keys = [k for k in mapper.keys() if k >= target]
            for key in keys: mapper[key] += incremented

    return manifest


############################################################################################
def remove_from_manifest(manifest, fle):
    """ Remove an item from the manifest and remove empty sub manifest if created """
    manifest = deepcopy(manifest)

    # flatten the manifest but keep track of what section each path was in
    items = [(item['path'], i) for i, chunk in enumerate(manifest) for item in chunk]

    # find the desired item in the list
    manifest_id = next(y for x, y in enumerate(items) if y[0] == fle['path'])[1]

    # remove from manifest
    manifest_item = next(x for x, y in enumerate(manifest[manifest_id]) if y['path'] == fle['path'])
    del manifest[manifest_id][manifest_item]

    # detect and remove empty manifests
    rebuilt = []
    for i, part in enumerate(manifest):
        if len(part) > 0:
            rebuilt.append(part)
        else:
            pass #TODO will need to update the index when implemented            

    return rebuilt

############################################################################################
def utf8len(s):
    return len(s.encode('utf-8'))

############################################################################################
def get_if_set(lst, idx, default):
    try: 
        if idx >= 0: return lst[idx]; raise IndexError
    except IndexError:
        return default

############################################################################################
def get_serialised_size (manifest):
    """ Gets the serialized length of a manifest """
    return utf8len(json.dumps(manifest))


