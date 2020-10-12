from rrbackup.fsutil import *
from unittest import TestCase
import subprocess, os

def move_helper(status, path, hsh):
    return {'status'   : status,
            'path'     : path,
            'created'  : '',
            'last_mod' : '',
            'hash'     : hsh}

def res_helper(status, moved_from, path, hsh):
    return {'status'    : status,
            'path'      : path,
            'moved_from': moved_from,
            'created'   : '',
            'last_mod'  : '',
            'hash'      : hsh}

def get_state(path, last_mod):
    return {'path'     : path,
            'last_mod' : last_mod}

#--------------------
DATA_DIR = 'test_data'

############################################################################################
class TestCommon(TestCase):
    def test_hash_file(self):
        """ Test that file hash returns the correct result. """

        file_path = 'HASH_TEST_FILE'
        file_put_contents(file_path, 'some file contents')

        p1 = subprocess.Popen (['sha256sum', file_path], stdout=subprocess.PIPE)
        result1= p1.communicate()[0].split(b' ')[0]
        result2 = hash_file(file_path)

        self.assertEqual(result1, result2.encode('utf8'),
            msg = 'Hashes are not the same')

        os.remove(file_path)

    def test_find_manifest_changes(self):
        state_1 = []
        state_2 = [get_state('/file_1', 10)]
        state_3 = [get_state('/file_1', 20)]
        state_4 = [get_state('/file_1', 20), get_state('/file_2', 10)]
        state_5 = [get_state('/file_2', 20)]

        # Do some diffs
        diff_2 = find_manifest_changes(state_2, state_1)
        diff_3 = find_manifest_changes(state_3, state_2)
        diff_4 = find_manifest_changes(state_4, state_3)
        diff_5 = find_manifest_changes(state_5, state_4)

        self.assertEqual(diff_2, {'/file_1': {'status': 'new', 'path': '/file_1', 'last_mod': 10}})
        self.assertEqual(diff_3, {'/file_1': {'status': 'changed', 'path': '/file_1', 'last_mod': 20}})
        self.assertEqual(diff_4, {'/file_2': {'status': 'new', 'path': '/file_2', 'last_mod': 10}})
        self.assertEqual(diff_5, {'/file_2': {'status': 'changed', 'path': '/file_2', 'last_mod': 20},
                                  '/file_1': {'status': 'deleted', 'path': '/file_1', 'last_mod': 20}})

    def test_apply_diffs_new_to_empty(self):
        manifest = []
        diff = [{'path'   : '/file1',
                 'status' : 'new'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1'}])

    def test_apply_diffs_new_not_empty(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file2',
                 'status' : 'new'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1'}, {'path'   : '/file2'}])

    def test_apply_diffs_changed(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file1',
                 'new'    : True,
                 'status' : 'changed'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1', 'new' : True}])

    def test_apply_diffs_moved(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'       : '/file2',
                 'moved_from' : '/file1',
                 'status' : 'moved'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file2', 'moved_from' : '/file1'}])

    def test_apply_diffs_deleted(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file1',
                 'status' : 'deleted'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [])

    def test_detect_moved_files_one(self):
        return True
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/test2', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/test2', '12345')])

    def test_detect_moved_files_multiple(self):
        return True

        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : 'a12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', 'a12345'),
                         move_helper('new', '/a/test2', 'a12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test2', '/a/test2', 'a12345'),
                                  res_helper('moved', '/test', '/a/test', '12345')])

    def test_detect_moved_files_duplicates(self):
        return True

        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test2', '/a/test2', '12345'),
                                  res_helper('moved', '/test', '/a/test', '12345')])

    def test_detect_moved_files_duplicates_with_rename(self):
        return True

        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2n', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/a/test', '12345'),
                                  res_helper('moved', '/test2', '/a/test2n', '12345')])

    def test_detect_moved_files_new_duplicate_not_moved(self):
        return True

        file_manifest = {'files' : [{'path' : '/test',
                                      'hash' : '12345'}]}

        diff          = [move_helper('new', '/a/test', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [move_helper('new', '/a/test', '12345')])


    def test_detect_moved_files_duplicates_and_no_duplicates(self):
        return True

        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'},
                                    {'hash' : 'a12345',
                                    'path' : '/test3'},
                                    {'hash' : 'b12345',
                                    'path' : '/test4'},
                                    {'hash' : 'c12345',
                                    'path' : '/test5'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2', '12345'),
                         move_helper('delete', '/test3', 'a12345'),
                         move_helper('new', '/a/test3', 'a12345'),
                         move_helper('delete', '/test4', 'b12345'),
                         move_helper('new', '/a/test4', 'b12345'),
                         move_helper('delete', '/test5', 'c12345'),
                         move_helper('new', '/a/test5n', 'c12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/a/test', '12345'),
                                  res_helper('moved', '/test5', '/a/test5n', 'c12345'),
                                  res_helper('moved', '/test3', '/a/test3', 'a12345'),
                                  res_helper('moved', '/test2', '/a/test2', '12345'),
                                  res_helper('moved', '/test4', '/a/test4', 'b12345')])

    def test_filter_helper(self):
        self.assertFalse(filter_helper('/test/file', ['/test/file2']))
        self.assertFalse(filter_helper('/test/file', ['/file1', '/test']))
        self.assertTrue(filter_helper('/test/file', ['/file1', '/test/*']))
        self.assertTrue(filter_helper('/test/file', ['*']))

    def test_filter_file_list(self):
        self.assertEqual(filter_file_list([{'path':'test'}], ['test']),
                         [])

        self.assertEqual(filter_file_list([{'path':'test'}], ['other']),
                         [{'path':'test'}])

