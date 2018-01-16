from boto.s3.connection import S3Connection
from boto.s3.key import Key
import hashlib
import json
import scrypt
import nacl.signing
import nacl.secret

from common import *

salt     = ''
DATA_DIR = './test_dir/'

ACCESS_KEY = ''
SECRET_KEY = ''
BUCKET     = ''

############################################################################################
# Generate a key from a password
############################################################################################
def key_from_password(password):
    return scrypt.hash(password, 'random salt', 4096 , 100, 1, nacl.secret.SecretBox.KEY_SIZE)

############################################################################################
# Encrypt a private key
############################################################################################
def encrypt_private(password, private):
    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    # nonce must only be used once, make a new one every time
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)

    # Encrypted result stores authentication information and nonce alongside it,
    # do not need to store these separately.

    result = box.encrypt(private, nonce)
    #result = hexlify(result)
    return result

############################################################################################
# Decrypt a private key
############################################################################################
def decrypt_private(password, crypt_private):
    #crypt_private = unhexlify(crypt_private)

    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    return box.decrypt(crypt_private)

class s3_backup:
############################################################################################
    def __init__(self, aws_access_key, aws_secret_key, s3_bucket):
        """ Create s3 connection and get the bucket.  """

        self.conn = S3Connection(aws_access_key, aws_secret_key)
        self.bucket = self.conn.get_bucket(s3_bucket)


############################################################################################
    def hash_path(self, path):
        """ Hash a file path to create am obscure 'key' for s3.  """
        return hashlib.sha256((salt + path).encode('utf-8')).hexdigest()

############################################################################################
    def update_manifest_s3(self, manifest):
        """ Update the manifest file stored on s3.  """

        key_name = self.hash_path('manifest_xzf.json')

        k = Key(self.bucket)
        k.key = key_name

        file_put_contents('manifest', json.dumps(manifest))

        contents = encrypt_private('test', json.dumps(manifest))

        k.set_contents_from_string(contents)


############################################################################################
    def get_validate_remote_manifest(self):
        """ Get and validate the remote manifest. """

    # Check if the manifest exists and validate it if it does
        key = self.bucket.get_key(self.hash_path('manifest_xzf.json'))
        if key != None:
            print 'Getting manifest'

            manifest = key.get_contents_as_string()
            manifest = decrypt_private('test', manifest)
            manifest = json.loads(manifest)

            manifest_dict = {}

            for fle in manifest['files']:
                manifest_dict[fle['hashed_path']] = fle

        # make sure objects listed in the manifest actually exist on S3
            filter_manifest = []
            listing = self.bucket.list()
            for itm in listing:
            # make sure this key exists in the manifest
                if itm.key in manifest_dict:
                    filter_manifest.append(manifest_dict.pop(itm.key))

            manifest['files'] = filter_manifest

            """
            print filter_manifest
            print ''
            # manifest dict now contains files which do not exist on the remote
            print manifest_dict
            print ''
            """
        
    # If no manifest, create one
        else:
            print 'creating manifest'

            manifest = {'files' : []}

            self.update_manifest_s3(manifest)

        return manifest


############################################################################################
    def find_new_files(self, manifest, local_file_dict):
        """ Compare manifest to local file system finding new files """

        local_missing = []

        for fle in manifest['files']:
            if fle['path'] in local_file_dict:
                local_file_dict.pop(fle['path']) # item already exists on s3
            else:
                # file does not exist locally
                local_missing.append(fle)

        new_files = []

        # Any thing still in the dict does not exist on the remote
        for key, val in local_file_dict.iteritems():
            val['hashed_path'] = self.hash_path(val['path'])
            manifest['files'].append(val)
            new_files.append(val)
    
        return (new_files, local_missing)


############################################################################################
    def send_file(self, fle):
        """ Send a local file to s3 """

        k = Key(self.bucket)
        k.key = fle['hashed_path']

        contents = file_get_contents(cpjoin(DATA_DIR, fle['path']))

        crypt_contents = encrypt_private('test', contents)

        k.set_contents_from_string(crypt_contents)







backup = s3_backup(ACCESS_KEY, SECRET_KEY, BUCKET)

manifest = backup.get_validate_remote_manifest()

mode = 'push'

if mode == 'push':
    new_files, local_missing = backup.find_new_files(manifest, make_dict(get_file_list(DATA_DIR)))

    print '--------------------'

    if new_files == []:
        print 'Nothing to do'
    else:
        # Update the manifest on s3. All new files are added to the manifest at once.
        # This is done to reduce network traffic. Manifest is updated before any uploads
        # happen as we want to make sure we can map the hashed keys back to file names
        # in the future even if we are not able to upload all of them in this session
        backup.update_manifest_s3(manifest)

        # Upload any files that don't exist 
        for fle in new_files:
            print 'Sending: ' + fle['path']

            backup.send_file(fle)
            

        # Remove files from remote that have been removed locally


# Download all files which do not exist locally. This needs to have the capacity to download only a subset
elif mode == 'pull':
    new_files, local_missing = backup.find_new_files(make_dict(get_file_list(DATA_DIR)))

    for fle in local_missing:
        print 'Getting: ' + fle['path']

        backup.get_file(fle)




