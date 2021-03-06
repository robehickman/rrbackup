Robert's remote backup (rrbackup) is a simple versioning file backup system, It includes a command line client and may also be used as a library.  Stored files may be encrypted before upload using libsodium and are grouped into atomic commits. Currently S3 is the only backed supported. As each file is stored as a separate object it is always possible to recover part of a backup.

Amazon S3 has object versioning but cannot group objects as an atomic commit, this functionality is achieved by referencing all files from a central manifest stored as a progression of diffs, this minimises network overhead as only metadata about changed files needs to be uploaded. Only files referenced within the manifest are considered to exist so if a backup is interrupted by a power failure the system reverts to the prior good state.

## Usage

First you must create a configuration file "configuration.json", a minimal configuration follows:

```json
{
    "base_path":                 "/path/to/directory/to/back/up",
    "local_manifest_file":       "/path/to/local/manifest",
    "local_lock_file":           "/path/to/rrbackup_lock",
    "s3" : {
        "access_key": "your aws access key",
        "secret_key": "your aws secret key",
        "bucket":     "aws bucket to use"
    },
}
```

These values are:

* "base\_path"  
This is the base path of the directory which will be backed up.

* "local\_manifest\_file"  
Path to the local manifest file. The local manifest is a flat JSON file which stores the state of the files on the last run and is used for change detection. The manifest should not be in the base path as the system would detect it and needlessly back it up.

* "local\_lock\_file"  

Because s3 lacks synchronisation features rrbackup uses flock as a sanity check to prevent multiple clients running simultaneously on the same directory. To prevent write access to multiple clients on multiple computers I recommend creating separate IAM accounts, how to do so is described later.

* "s3" : "access\_key"  
The access key of the AWS (or IAM) account you wish to back up to.

* "s3" : "secret\_key"  
The secret key of the AWS (or IAM) account you wish to back up to.

* "s3" : "bucket"  
The secret key of the AWS (or IAM) account you wish to back up to. Note that this bucket must already exist and must have versioning enabled.


### Running

Instillation through setup.py creates a system command 'rrbackup'. By default the application looks for it's configuration file in the current working directory, an alternate location can be specified with --c [conf file path] as the first argument. This also allows you to rename the file if you wish.

Note that rrbackup does follow symlinks and mount points so you need to take care that the backed up filesystem does not include cyclic links. Wine for instance has multiple links to the home folder in .wine, I recommend blanket ignoring *.wine*, see ignoring files below.

To run a backup just run 'rrbackup' at the command line with no arguments, it will detect the files within the configured directory and upload them. To list or download prior backups the following arguments may be used:


* rrbackup -h  
Display help information

* rrbackup list\_versions  
Lists all versions that exist, newest last


* rrbackup list\_files [version id]  
List all files in a version


* rrbackup list\_changes  [version id]  
List what files changed in the named version


* rrbackup download [version id]  [target] [ignore filters]  
Download a file or files from the backup. This creates the target directory if it does not exist. Note that this will overwrite any existing files in the directory so using a new one is recommended. If you do not wish to download everything list files you don't want in a text file then pass it's path to the Ignore filters parameter. These are listed one per line and support Unix wildcards similar to gitignore. To ignore everything in the directory 'foo' in the root you would use '/foo\*'. *Note that the leading slash is required*.


## Additional options

### File processing pipelines

By default this application applies no processing to backed up files, storing them exactly as-is. Pipelines of transformations can be applied using arbitrary wildcards to encrypt files, compress them or obfuscate there names. This can be used to restrict compression to known compressible files or apply encryption to sensitive data, storing things which are already public as-is. This avoids unneeded processing overhead.

Pipeline formats are expressed as a list of keywords and the order of items does not matter, Valid filters are 'hash\_names', 'compress' and 'encrypt'. It does not matter what order you specify them, they are always handled 'hash\_names' -> 'compress' -> 'encrypt'. When downloading the order is reversed with names restored from the manifest. Please see the section 'encryption' below for usage of encryption feature.

Two pipeline specifiers exist, one applies to the applications metadata and the other to the backed up files.


#### Metadata pipeline

The metadata pipeline is defined within the top-level of the json file, it applies to the applications metadata: manifest diffs, garbage collection log and garbage object log if operating in write-only mode (see later). Note that you cannot store these with hashed names, if you wish to obfuscate there names see "Obfuscating the names of metadata files" below.

```json
{
    "meta_pipeline": ["compress", "encrypt"]
}
```


#### File pipelines

File pipelines can be applied to single files or groups of files using identifiers and Unix wildcards, the format of this is as follows:

```json
{
    "file_pipeline": [
        ["/bar", []],
        ["*", ["hash_names", "encrypt"]]
    ]
}
```

The above says 'apply no processing to the file 'bar' in the root and 'hash names' and 'encrypt' to everything else.  Note that filters must always start with a slash '/. Also note that the order of these wildcards are listed matters: they are are evaluated top to bottom so must be most to least specific. For example placing a match all wildcard '\*' first matches everything and following items will not be considered.


### Encryption

Encryption uses the 'secret stream' api provided by libsodium so can handle arbitrarily large files without exhausting memory.  To make use of encryption first you must configure the encryption password in the conf file, this is processed using the ARGON2I13 key derivation function to create the encryption key:

```json
{
    "crypto" : {
        "crypt_password":            "crypt password"
    }
}
```

Once this is configured just add 'encrypt' to the pipelines as desired:

```json
{
    "meta_pipeline": ["encrypt"],
    "file_pipeline": [
        ["*", ["encrypt"]]
    ]
}
```

Note that encryption is TNO: only you know the password, if you lose it you will lose your data.


### Read only operation

If using this system to back up a server you may want a client on another computer, as S3 lacks synchronisation features it is a bad idea to have multiple clients writing to the same s3 bucket. The client can be configured to operate in read-only mode by adding the following to the configuration file. This can be enforced with IAM permissions.

First set the following in the configuration:

```json
{
    "read_only":                 true,
    "write_only":                false,
}
```

Then add the following policy to IAM and attach it to a new read only user, replace YOUR-BUCKET with the name of your bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketVersioning",
                "s3:ListBucketVersions"
                "s3:getObject",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET",
                "arn:aws:s3:::YOUR-BUCKET/*"
            ]
        }
    ]
}

```

 

### Write only operation

If using this to back up a server you may want to run in write-only mode enforced with IAM permissions. Files are write only but read access is required for the manifest, salt file, gc log and garbage objects file. It also requires permission to insert delete markers on the garbage collection log. Also note that you cannot do initial setup with these permissions: a grant-all account should be used for setup.

If a backup fails in a way that leaves garbage objects on s3 these will be concatenated onto a garbage objects file when running in write-only mode. It is important to clean these periodically using a client with read-write permissions. S3 cannot append to objects thus the whole thing is downloaded and reuploaded.

First set the following in the configuration:


```json
{
    "read_only":                 false,
    "write_only":                true,
    "allow_delete_versions":     false
}
```

Then add the following policy to IAM and attach it to a new write only user, replace YOUR-BUCKET with the name of your bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetBucketVersioning",
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload",
                "s3:ListBucketVersions"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET",
                "arn:aws:s3:::YOUR-BUCKET/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:getObject",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET/salt_file",
                "arn:aws:s3:::YOUR-BUCKET/manifest_diffs",
                "arn:aws:s3:::YOUR-BUCKET/gc_log",
                "arn:aws:s3:::YOUR-BUCKET/garbage_objects"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:deleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET/gc_log"
            ]
        }
    ]
}

```

### Using bucket life cycle rules to cleanup garbage collection logs

Unfortunately S3 versioning cannot be controlled at the scope of individual objects and consequently old versions of the garbage collection log and garbage object log (if in write only mode) will accumulate. As only the latest are needed for normal operation I advise creating a life cycle rules to delete old versions of 'gc\_log' and 'garbage\_objects', or there equivalent names if you have renamed them.

Note: NEVER create rules to delete old manifest diffs or old versions of anything in the 'files' directory as you will corrupt your backup.


### Obfuscating the names of metadata files

If you wish to obfuscate the names of the remote manifest diffs, remote GC log and password salt file this can be done by adding the following to the configuration:

```json
{
    "remote_manifest_diff_file": "asdfgjkll",
    "remote_gc_log_file":        "cvbnmoytety",
    "remote_password_salt_file": "qwertyuio"
}
```

Note that doing this adds little security as the function of these files can be deduced from how they are used. There contents may be protected with encryption as described above.  If you are rename these files and use a write-only IAM policy remember to update the names in the policy as well.


### Ignoring files

You may have files which you never wish to back up, such as transient cash files. These can be ignored by adding them to the ignored files array:

```json
{
    "ignore_files" : [
        "/ignored*"
    ]
}
```

Note that these are again evaluated top to bottom so be careful with wildcards.  If a file is added to the ignore list after it has been backed up previously, the next time backup is run it will be removed from the latest manifest diff and will not appear in following backups.


### Skipping delete

Sometimes you may want to add a file to a backup, keeping it in the backup but deleting it from the local file system to save space, deltas of database snapshots for instance. Such files should be added to 'ignore delete', they will be added when they appear in the filesystem but will not be deleted from the backup when removed. Once again these are evaluated top to bottom so be careful with wildcards.

```json
{
    "skip_delete" : [
        "/skip_delete*"
    ]
}
```


### Complete example configuration

The following is a single configuration with all of the options above.

```json
{
    "base_path":                 "/path/to/directory/to/back/up",
    "local_manifest_file":       "/path/to/local/manifest",
    "local_lock_file":           "/path/to/rrbackup_lock",

    "s3" : {
        "access_key": "your aws access key",
        "secret_key": "your aws secret key",
        "bucket":     "aws bucket to use"
    },

    "crypto" : {
        "crypt_password":            "crypt password"
    },

    "meta_pipeline": ["compress", "encrypt"],
    "file_pipeline": [
        ["/bar", []],
        ["*", ["hash_names", "encrypt"]]
    ],

    "read_only":                 false,
    "allow_delete_versions":     true,

    "remote_manifest_diff_file": "asdfgjkll",
    "remote_gc_log_file":        "cvbnmoytety",
    "remote_password_salt_file": "qwertyuio",

    "ignore_files" : [
        "/ignored*"
    ],

    "skip_delete" : [
        "/skip_delete*"
    ]
}
```


### Usage as a library

The command line client is a thin interface to an underlying library, please see the command line client (cli/rrbackup) and 'core.py' for usage.
