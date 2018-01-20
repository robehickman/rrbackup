import sys, os, boto.utils, dateutil, pprint, json, copy
import rrbackup.fsutil as sfs
import rrbackup.core as core
import rrbackup.pipeline as pipeline
import rrbackup.crypto as crypto
import rrbackup.s3_interface as s3

import collections

#++++++++++++++++++++++++
if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
    print """
Robert's Remote Backup, backup a directory with change history.

[none]                               - run backup from configuration file
list_versions                        - List all versions
list_files    [version_id]           - List all files in a version
list_changes  [version_id]           - List what has changed in the named version
download      [version id]  [target] - Download a file or files from the backup, creates
                                       target directory if it does not exist """

else:
    # Assemble default configuration
    config = core.default_config()
    config = s3.add_default_config(config)
    config = crypto.add_default_config(config)

    # Read and merge configuration file
    args = copy.deepcopy(sys.argv); args.pop(0)

    conf_file = 'configuration.json'
    if len(args) > 0 and args[0] == '--c':
        conf_file = args.pop(0)

    parsed_config = json.loads(sfs.file_get_contents(conf_file))
    if 'meta_pipeline' in parsed_config and type(parsed_config['meta_pipeline']) != list: raise ValueError('meta_pipeline in conf file mist be a list')
    if 'file_pipeline' in parsed_config and type(parsed_config['file_pipeline']) != list: raise ValueError('file_pipeline in conf file mist be a list')
    if 'ignore_files' in parsed_config and type(parsed_config['ignore_files']) != list: raise ValueError('ignore_files in conf file mist be a list')
    if 'skip_delete' in parsed_config and type(parsed_config['skip_delete']) != list: raise ValueError('skip_delete in conf file mist be a list')

    def dict_merge(dct, merge_dct): # recursive dict merge from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
        for k, v in merge_dct.iteritems():
            if (k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], collections.Mapping)):
                dict_merge(dct[k], merge_dct[k])
            else: dct[k] = merge_dct[k]
    dict_merge(config, parsed_config)

    # Setup the interface and core
    interface = s3
    conn = interface.connect(parsed_config)
    #interface.wipe_all(conn)
    #quit()

    config = pipeline.preprocess_config(interface, conn, config)
    core.init(interface, conn, config)

    #++++++++++++++++++++++++
    if len(args) == 0:
        print 'Running backup'
        core.backup(interface, conn, config)

    #++++++++++++++++++++++++
    elif args[0] == 'list_versions':
        versions = core.get_remote_manifest_versions(interface, conn, config)
        print '\nDate and time         : Version ID\n'
        for vers in versions:
            timestr = vers['LastModified'].strftime('%d %b %Y %X')
            print timestr + ' : ' + vers['VersionId']
        print

    #++++++++++++++++++++++++
    elif args[0] == 'list_files':
        if len(args) < 2:
            print "You must provide a Version ID, see help (-h)"; quit()

        versions = core.get_remote_manifest_diffs(interface, conn, config)

        manifest = core.rebuild_manifest_from_diffs(versions, args[1])

        manifest['files'] = sorted(manifest['files'], key=lambda fle:
            (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

        print
        for fle in manifest['files']:
            print fle['path']
        print

    #++++++++++++++++++++++++
    elif args[0] == 'list_changes':
        if len(args) < 2:
            print "You must provide a Version ID, see help (-h)"; quit()

        diff = core.get_remote_manifest_diff(interface, conn, config, args[1])['body']

        print
        for change in diff:
            if change['status'] == 'moved':
                print change['status'].capitalize() + ': ' + change['moved_from']
                print '   To: ' + change['path']
            else:
                print change['status'].capitalize() + ': ' + change['path']
        print


    #++++++++++++++++++++++++
    elif args[0] == 'download':
        if len(args) < 2:
            print "You must provide a Version ID, see help (-h)"; quit()

        if len(args) < 3:
            print "You must provide a target directory, see help (-h)"; quit()

        ignore_filters = None
        if len(args) > 3:
            ignore_filters = sfs.file_get_contents(args[3])
            ignore_filters = ignore_filters.splitlines()
            
        core.download(interface, conn, config, args[1], args[2], ignore_filters)

    else:
        print 'Unknown command'
