import os.path, fnmatch, json, hashlib, copy
from collections import defaultdict

############################################################################################
def force_unicode(text):
    """ Encodes a string as UTF-8 if it isn't already """
    try: return str(text, 'utf-8')
    except TypeError: return text

############################################################################################
def file_get_contents(path):
    """ Returns contents of file located at 'path' """
    with open(path, 'r') as f:
        return f.read()

############################################################################################
def file_put_contents(path, data):
    """ Put passed contents into file located at 'path' """
    with open(path, 'w') as f:
        f.write(data)

############################################################################################
def make_dirs_if_dont_exist(path):
    """ Create directories in path if they do not exist """
    path = os.path.dirname(path)
    if path != '':
        try: os.makedirs(path)
        except OSError: pass

############################################################################################
def allowed_path(path):
    """ Block '..' from occurring in file paths, this should not happen under normal operation. """
    udir = path.split('/')
    for x in udir:
        if x == '..':
            e = '.. in file paths not aloud'
            print(e); raise Exception(e)

############################################################################################
def ensure_extension(path, ext):
    """ Make sure path ends with the correct extension """
    if path.endswith(ext): return path
    else: return path + ext

############################################################################################
def pfx_path(path):
    """ Prefix a path with the OS path separator if it is not already """
    if path[0] != os.path.sep: return os.path.sep + path
    return path

############################################################################################
def cpjoin(*args):
    """ custom path join """
    rooted = False
    if args[0].startswith('/'):
        rooted = True

    # remove leading and trailing slashes
    newargs = []
    for arg in args:
        acopy = arg
        if acopy.startswith('/'):
            acopy = acopy[1:] # remove leading slashes
        newargs.append(acopy)

    path = os.path.join(*newargs) # pylint: disable=no-value-for-parameter 
    if rooted is True: path = os.path.sep + path
    return path

############################################################################################
def get_single_file_info(f_path, int_path):
    """ Gets the creates and last change times for a single file,
    f_path is the path to the file on disk, int_path is an internal
    path relative to a root directory.  """
    return { 'path'     : force_unicode(int_path),
             'created'  : os.path.getctime(f_path),
             'last_mod' : os.path.getmtime(f_path)}

############################################################################################
def hash_file(file_path, block_size = 65536):
    """ Hashes a file with sha256 """
    sha = hashlib.sha256()
    with open(file_path, 'rb') as h_file:
        file_buffer = h_file.read(block_size)
        while len(file_buffer) > 0:
            sha.update(file_buffer)
            file_buffer = h_file.read(block_size)
    return sha.hexdigest()

############################################################################################
def get_file_list(path, ignore_filters = None, visit_mountpoints = True):
    """ Recursively lists all files in a file system below 'path'. """
    f_list = []
    read_errors = []
    def recur_dir(path, newpath = os.path.sep):
        try: files = os.listdir(path)
        except OSError:
            read_errors.append(path)
            return

        for fle in files:
            f_path = cpjoin(path, fle)

            visit_path = True

            if not (ignore_filters is None or not filter_helper(cpjoin(newpath, fle), ignore_filters)):
                visit_path = False

            if not visit_mountpoints and os.path.ismount(f_path):
                visit_path = False

            if visit_path:
                if os.path.isdir(f_path):
                    recur_dir(f_path, cpjoin(newpath, fle))
                elif os.path.isfile(f_path):
                    try:
                        open(f_path, 'r').close()
                        f_list.append(get_single_file_info(f_path, cpjoin(newpath, fle)))
                    except IOError:
                        read_errors.append(f_path)

    recur_dir(path)
    return f_list, read_errors

############################################################################################
def make_dict(s_list):
    """ Convert file list into a dictionary with the file path as its key, and meta data
    as a list stored as the keys value. This format change makes searching easier. """
    return { l_itm['path'] : l_itm for l_itm in s_list}

############################################################################################
def find_manifest_changes(new_file_state, old_file_state):
    """ Find what has changed between two sets of files """
    prev_state_dict = make_dict(old_file_state)

    changed_files = {}

    # Find files which are new on the server
    for itm in new_file_state:
        if itm['path'] in prev_state_dict:
            d_itm = prev_state_dict.pop(itm['path'])

            # If the file has been modified
            if itm['last_mod'] != d_itm['last_mod']:
                n_itm = itm.copy()
                n_itm['status'] = 'changed'
                changed_files[itm['path']] = n_itm
            else:
                pass # The file has not changed

        else:
            # anything here was not found in the remote manifest is new on the server
            n_itm = itm.copy()
            n_itm['status'] = 'new'
            changed_files[itm['path']] = n_itm

    # any files remaining in the remote manifest have been deleted locally
    for itm in prev_state_dict.values():
        n_itm = itm.copy()
        n_itm['status'] = 'deleted'
        changed_files[itm['path']] = n_itm

    return changed_files


############################################################################################
def hash_new_files(diff, base_path):
    processed_files = []
    for val in diff:
        fpath = cpjoin(base_path, val['path'])
        if val['status'] in ['new', 'changed']: val['hash'] = force_unicode(hash_file(fpath))
        processed_files.append(val)
    return processed_files

###########################################################################################
def apply_diffs(diffs, manifest):
    """ Apply a series of differences to a manifest
    diffs is an list(diffs) of list(diff) of dict(change item) """

    manifest = copy.deepcopy(manifest)

    # helper which removes the 'status' key from a diff
    key_filter = lambda item : { key : value for key, value in item.items() if key != 'status'}

    for diff in diffs:
        # dict used to find duplicate items between the diff and manifest
        manifest_dict = {item['path'] : None for item in manifest}

        # dict used to remove items that have been moved
        moved = {change['moved_from'] : None for change in diff if change['status'] == 'moved'}

        # remove deleted, changed and moved items from manifest
        # and filter the result to remove the 'status' key
        deleted = {item['path'] : None for item in diff
            if item['status'] == 'deleted'
            or item['status'] == 'changed'
            or item['status'] == 'moved'
            or item['path'] in manifest_dict} # treat duplicate items as updates

        applied = [key_filter(item) for item in manifest
            if  item['path'] not in deleted
            and item['path'] not in moved]

        # add new and changed items
        applied += [key_filter(item) for item in diff
            if item['status'] == 'new'
            or item['status'] == 'changed'
            or item['status'] == 'moved']

        manifest = applied

    return manifest

############################################################################################
def filter_helper(file_path, ignore_filters):
    """ Returns True if file path matches any of the filters in 'ignores', ignores is a
    list of unix wildcards, for example '/path/to/file' or '*.swp' """
    return next((True for i in ignore_filters if fnmatch.fnmatch(file_path, i)), False)

############################################################################################
def filter_file_list(file_paths, ignore_filters):
    """ Applies a list of unix wildcard filters to all of the files in file_paths,
    returning those which do not match any of the filters. """
    return [f for f in file_paths if not filter_helper(f['path'], ignore_filters)]

############################################################################################
def filter_f_list(f_list, unix_wildcard):
    """ Removes files from list by unix-type wild cards, used to implement ignored files. """
    f_list_filter = []
    for itm in f_list:
        if fnmatch.fnmatch(itm['path'], unix_wildcard): pass
        else: f_list_filter.append(itm)
    return f_list_filter

############################################################################################
#def apply_ignore_filters(f_list):
#    """  Loads file ignore filters from IGNORE_FILTER_FILE and applies them to file list passed """
#    filters = []
#
#    try:
#        IGNORE_FILTER_FILE
#
#        try:
#            f_file = file_get_contents(DATA_DIR + IGNORE_FILTER_FILE)
#            lines = f_file.splitlines()
#            filters = filters + lines
#        except:
#            print('Warning: filters file does not exist')
#    except NameError:
#        print('Warning: configuration var IGNORE_FILTER_FILE is not defined')
#
#    try:
#        filters.append('/' + MANIFEST_FILE)
#        filters.append('/' + CLIENT_CONF_DIR + '*')
#        filters.append('/' + REMOTE_MANIFEST_FILE)
#        filters.append('/' + PULL_IGNORE_FILE)
#    except:
#        pass # on the server remote manifest does not exist
#
#    for f in filters: f_list = filter_f_list(f_list, f)
#    return f_list
