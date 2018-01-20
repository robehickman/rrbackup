import rrbackup.pipeline as pipeline
import pysodium, unittest, pprint

def write_helper(data, meta, config): meta['data'] = data; return meta
def read_helper(meta, config): return meta['data'], meta

class test_pipeline(unittest.TestCase):
    def test_simple_pipeline(self):
        config = {'crypto' : {'encrypt_opts' : {}, 'stream_crypt_key' : pysodium.crypto_secretstream_xchacha20poly1305_keygen()}}

        meta_pl_format = pipeline.get_default_pipeline_format()
        meta_pl_format['format'].update({'compress'   : None,
                                         'encrypt'    : config['crypto']['encrypt_opts']})
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

    def test_simple_pipeline_compress(self):
        config = {}

        meta_pl_format = pipeline.get_default_pipeline_format()
        meta_pl_format['format'].update({'compress'   : None})
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


    def test_simple_pipeline_encrypt(self):
        config = {'crypto' : {'encrypt_opts' : {}, 'stream_crypt_key' : pysodium.crypto_secretstream_xchacha20poly1305_keygen()}}

        meta_pl_format = pipeline.get_default_pipeline_format()
        meta_pl_format['format'].update({'encrypt'    : config['crypto']['encrypt_opts']})
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

    def test_streaming_pipeline(self):
        return

        upload = interface.streaming_upload(conn, remote_path, config['chunk_size'])
        pl     = pipeline.build_pipeline_streaming(upload, 'out', ['encrypt'], config)
        upload.begin()
        with open(fspath, 'rb') as fle:
            while True:
                chunk = fle.read(config['chunk_size'])
                if chunk == "": break
                pl.next_chunk(chunk)

        #-----------
        download = interface.streaming_download(conn, remote_path2, fle['version_id'], config['chunk_size'])
        pl     = pipeline.build_pipeline_streaming(download, 'in', ['encrypt'], config)

        dest = sfs.cpjoin(target_directory, fle['path'])
        sfs.make_dirs_if_dont_exist(dest)
        with open(dest, 'wb') as fle:
            while True:
                res = pl.next_chunk()
                if res == None: break
                fle.write(res)

    def test_streaming_pipeline_org(self):
        return #disabled for the time being
        crypt_key = pysodium.crypto_secretstream_xchacha20poly1305_keygen()
        print 'upload...'
        upload = streaming_upload(client, bucket, key)
        crpt = encrypter(upload, crypt_key)

        upload.begin()

        pl_out = pipeline.build_pipeline_streaming('out', pl_format, config)

        with open('', 'rb') as fle:
            while True:
                chunk = fle.read(chunk_size)
                if chunk == "": break
                crpt.next_chunk(chunk)

        res = upload.finish()

        #---------------
        print 'download...'
        download = streaming_download(client, bucket, key, chunk_size)
        dcrpt = decrypter(download, crypt_key)

        with open('', 'wb') as fle:
            while True:
                res = dcrpt.next_chunk()
                if res == None: break
                fle.write(res)



