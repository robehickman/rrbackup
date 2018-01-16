import unittest
from pprint import pprint
from string import ascii_lowercase
import manifest_data_structure as mds


def get_test_data():
    paths = """A/file1
    A/B/C/D/file3
    A/B/file1
    A/B/file2
    A/B/C/D/file1
    A/file2
    A/W/X/Y/Z/file1
    A/W/file1
    A/W/X/file1
    A/file3
    A/B/C/file1
    A/W/X/Y/file1
    A/B/file2"""

    return [{'path' : s.strip()} for s in paths.splitlines()] 

def get_test_data_2():
    config = mds.get_default_config()
    config['max_manifest_size'] = 40
    new_files = [{'path' : '/a/' + (c * 5)} for c in ascii_lowercase][:6]
    manifest = mds.build_manifest(config, new_files)

    return config, manifest


class test_manifest_data_structure(unittest.TestCase):
############################################################################################
    def test_build_manifest(self):
        pass
        """ Test that items get split into appropriate number of sub manifests depending on there size """

        """
        config = mds.get_default_config()
        config['max_manifest_size'] = 20
        new_files = get_test_data()

        pprint(mds.get_unique_paths(new_files))

        # do test data that results in no splits
        manifest = mds.build_manifest(config, new_files)

        pprint(manifest)
        print '---'

        # do test data that results in 1 splits
        #pprint(mds.build_manifest(config, new_files))

        #self.assertEqual(1, 0)
        """

############################################################################################
    def test_add_to_manifest_before_first(self):
        config, manifest = get_test_data_2()

        path = '/a/aa'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[0][0]['path'], path)


############################################################################################
    def test_add_to_manifest_after_first(self):
        config, manifest = get_test_data_2()

        path = '/a/bbbbbb'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[1][0]['path'], path)

############################################################################################
    def test_add_to_manifest_before_first(self):
        config, manifest = get_test_data_2()

        path = '/a/cc'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[1][0]['path'], path)

############################################################################################
    def test_add_to_manifest_middle_middle(self):
        config, manifest = get_test_data_2()

        path = '/a/ddd'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[2][0]['path'], path)

############################################################################################
    def test_add_to_manifest_after_middle(self):
        config, manifest = get_test_data_2()

        path = '/a/ddddddd'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[2][0]['path'], path)

############################################################################################
    def test_add_to_manifest_before_last(self):
        config, manifest = get_test_data_2()

        path = '/a/ee'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[2][0]['path'], path)

############################################################################################
    def test_add_to_manifest_after_last(self):
        config, manifest = get_test_data_2()

        path = '/a/gggggg'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)
        self.assertEqual(res[3][0]['path'], path)

############################################################################################
    def test_add_to_manifest_multiple(self):
        config, manifest = get_test_data_2()

        # Test add multiple, first should allocate new, second should fill in after it
        path1 = '/a/gg'
        path2 = '/a/gh'
        res = mds.add_to_manifest(config, manifest, {'path' : path1})
        res = mds.add_to_manifest(config, res,      {'path' : path2})
        self.assertEqual(res[3][0]['path'], path1)
        self.assertEqual(res[3][1]['path'], path2)

############################################################################################
    def test_add_to_manifest_blocked_duplicates(self):
        config, manifest = get_test_data_2()

        path = '/a/gggggg'
        file_to_add = {'path' : path}
        res = mds.add_to_manifest(config, manifest, file_to_add)

        # TODO test to make sure that duplicates are not aloud

############################################################################################
    def test_remove_from_manifest(self):
        config, manifest = get_test_data_2()

        # test removal
        res = mds.remove_from_manifest(manifest, {'path' :'/a/ccccc' })
        res = mds.remove_from_manifest(res,      {'path' :'/a/ddddd' })
        #self.assertEqual(1, 0)

if __name__ == "__main__":
    unittest.main()

