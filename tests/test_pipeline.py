import rrbackup.pipeline as pipeline
import pysodium, unittest, pprint

class test_pipeline(unittest.TestCase):
    def test_simple_pipeline(self):
        def write_helper(data, meta, config): meta['data'] = data; return meta
        def read_helper(meta, config): return meta['data'], meta

        config = {'encrypt_opts' : {}, 'stream_crypt_key' : pysodium.crypto_secretstream_xchacha20poly1305_keygen()}

        meta_pl_format = pipeline.get_default_pipeline_format()
        meta_pl_format['format'].update({'compress'   : None,
                                         'encrypt'    : config['encrypt_opts']})

        data_in = b'some data input'

        #-------
        pl_out = pipeline.build_pipeline(write_helper, 'out')
        meta = {'path' : 'test', 'header' : pipeline.serialise_pipeline_format(meta_pl_format)}
        meta2 = pl_out(data_in, meta, config)

        self.assertNotEqual(data_in, meta2['data'])

        #-------
        pl_in = pipeline.build_pipeline(read_helper, 'in')
        data_out, meta3 = pl_in(meta2, config)

        self.assertEqual(data_in, data_out)



