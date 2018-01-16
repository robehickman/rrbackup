import pysodium, binascii, base64, pipeline, pprint

def default_config():
    """ The default configuration structure. """
    return {'remote_password_salt_file'  : 'salt_file',  # Remote file used to store the password salt
            'crypt_password'             : None }

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def preprocess_config(interface, conn, config):
    if config['crypt_password'] == None or config['crypt_password'] == '':
        raise ValueError('Password has not been set')

    # attempt to get salt from remote, if does not exist
    # randomly generate a salt and store it on the remote
    try:
        res = interface.get_object(conn, config['remote_password_salt_file'])
        salt = binascii.unhexlify(res['body'].read())

    except ValueError:
        salt =pysodium.randombytes(pysodium.crypto_pwhash_SALTBYTES)
        interface.put_object(conn, config['remote_password_salt_file'], binascii.hexlify(salt))

    # Everything in here is included as a header, never put anything in this dict that must be private
    config['encrypt_opts'] = {
        'A' : 'ARGON2I13',
        'O' : pysodium.crypto_pwhash_argon2i_OPSLIMIT_INTERACTIVE,
        'M' : pysodium.crypto_pwhash_argon2i_MEMLIMIT_INTERACTIVE,
        'S' : base64.b64encode(salt)
    }

    key = pysodium.crypto_pwhash(pysodium.crypto_secretstream_xchacha20poly1305_KEYBYTES,
                                 config['crypt_password'], salt,
                                 config['encrypt_opts']['O'],
                                 config['encrypt_opts']['M'],
                                 pysodium.crypto_pwhash_ALG_ARGON2I13)
    config['stream_crypt_key'] = key; return config

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# One-shot encryption and decryption
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def encrypt(child, data, meta, config):
    if type(data) != str: raise TypeError('Data must be a byte string')

    pl_format = pipeline.parse_pipeline_format(meta['header'])
    pl_format['format']['encrypt']['E'] = 'sodssxcc20'
    meta['header'] = pipeline.serialise_pipeline_format(pl_format)

    crypt_key = config['stream_crypt_key']; ad_data = meta['header']
    state, header = pysodium.crypto_secretstream_xchacha20poly1305_init_push(crypt_key)
    cyphertext = pysodium.crypto_secretstream_xchacha20poly1305_push(state, data, ad_data, 0)
    final = header + cyphertext
    return child(final, meta, config)

def decrypt(child, meta, config):
    data, meta2 = child(meta, config)
    if type(data) != str: raise TypeError('Data must be a byte string')

    crypt_key = config['stream_crypt_key']; ad_data = meta['header']
    header = data[:pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES]
    chunk = data[pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES:]
    state = pysodium.crypto_secretstream_xchacha20poly1305_init_pull(header, crypt_key)
    final, tag = pysodium.crypto_secretstream_xchacha20poly1305_pull(state, chunk, ad_data)
    return final, meta2


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# Streaming (chunked) encryption and decryption
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
class streaming_encrypt:
    def __init__(self, child):
        self.child = child; self.chunk_id = 0
        self.state = self.header = None

    def pass_config(self, config):
        crypt_key = config['stream_crypt_key']
        self.state, self.header = pysodium.crypto_secretstream_xchacha20poly1305_init_push(crypt_key)

    def next_chunk(self, chunk):
        if type(chunk) != str: raise TypeError('Data must be a byte string')
        res = pysodium.crypto_secretstream_xchacha20poly1305_push(self.state, chunk, None, 0)
        if self.chunk_id == 0: res = self.header + res
        self.child.next_chunk(res); self.chunk_id += 1

class streaming_decrypt:
    def __init__(self, child):
        self.child = child; self.crypt_key = None; self.chunk_id = 0; self.tag = None

    def pass_config(self, config):
        self.crypt_key = config['stream_crypt_key']

    def next_chunk(self):
        if self.chunk_id == 0:
            chunk = self.child.next_chunk(pysodium.crypto_secretstream_xchacha20poly1305_ABYTES
                                          + pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES)
            if type(chunk) != str: raise TypeError('Data must be a byte string')
            header = chunk[:pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES]
            chunk = chunk[pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES:]
            self.state = pysodium.crypto_secretstream_xchacha20poly1305_init_pull(header, self.crypt_key)
        else:
            chunk = self.child.next_chunk(pysodium.crypto_secretstream_xchacha20poly1305_ABYTES)
            if chunk != None and type(chunk) != str: raise TypeError('Data must be a byte string')

        if chunk == None: return None
        msg, self.tag = pysodium.crypto_secretstream_xchacha20poly1305_pull(self.state, chunk, None)
        self.chunk_id += 1
        return msg

