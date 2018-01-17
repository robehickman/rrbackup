import sys, os
import shttpfs.common as sfs
import rrbackup.core as core
import rrbackup.pipeline as pipeline
import rrbackup.crypto as crypto
import rrbackup.s3_interface as s3
import boto.utils, dateutil, pprint, json

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
    parsed_config = sfs.read_config('configuration.ini')
    config = core.default_config()
    config.update(crypto.default_config())

    config.update(parsed_config['main'])

    #-------
    interface = s3
    conn = interface.connect(parsed_config)
    #interface.wipe_all(conn)
    #quit()

    config = pipeline.preprocess_config(interface, conn, config)

    core.init(interface, conn, config)

    #++++++++++++++++++++++++
    if len(sys.argv) == 1:
        print 'Running backup'
        core.backup(interface, conn, config)

    #++++++++++++++++++++++++
    elif sys.argv[1] == 'list_versions':
        versions = core.get_remote_manifest_versions(interface, conn, config)
        print '\nDate and time         : Version ID\n'
        for vers in versions:
            timestr = vers['LastModified'].strftime('%d %b %Y %X')
            print timestr + ' : ' + vers['VersionId']
        print

    #++++++++++++++++++++++++
    elif sys.argv[1] == 'list_files':
        if len(sys.argv) < 3:
            print "You must provide a Version ID, see help (-h)"; quit()

        versions = core.get_remote_manifest_diffs(interface, conn, config)

        manifest = core.rebuild_manifest_from_diffs(versions, sys.argv[2])

        manifest['files'] = sorted(manifest['files'], key=lambda fle:
            (os.path.dirname(fle['path']), os.path.basename(fle['path'])))

        print
        for fle in manifest['files']:
            print fle['path']
        print

    #++++++++++++++++++++++++
    elif sys.argv[1] == 'list_changes':
        if len(sys.argv) < 3:
            print "You must provide a Version ID, see help (-h)"; quit()

        diff = core.get_remote_manifest_diff(interface, conn, config, sys.argv[2])['body']

        print
        for change in diff:
            if change['status'] == 'moved':
                print change['status'].capitalize() + ': ' + change['moved_from']
                print '   To: ' + change['path']
            else:
                print change['status'].capitalize() + ': ' + change['path']
        print


    #++++++++++++++++++++++++
    elif sys.argv[1] == 'download':
        if len(sys.argv) < 3:
            print "You must provide a Version ID, see help (-h)"; quit()

        if len(sys.argv) < 4:
            print "You must provide a target directory, see help (-h)"; quit()

        ignore_filters = None
        if len(sys.argv) > 4:
            ignore_filters = sfs.file_get_contents(sys.argv[4])
            ignore_filters = ignore_filters.splitlines()
            
        core.download(interface, conn, config, sys.argv[2], sys.argv[3], ignore_filters)

    else:
        print 'Unknown command'
