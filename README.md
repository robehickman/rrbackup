Robert's remote backup (rrbackup) is a simple versioning file backup system, It includes a command line client and may also be used as a library.  Stored files may be stored encrypted using libsodium and are grouped into atomic commits. Currently S3 is the only backed supported and the design of this system works sympathetically with the services object versioning. Each file is stored as a separate object it is always possible to recover part of a backup.

Amazon S3 has object versioning but cannot group objects as an atomic commit, this functionality is achieved by referencing all files from a central manifest, which is stored as a progression of diffs, storing diffs in this way minimises network overhead as only metadata about changed files is maintained. As only files referenced within the manifest are considered to exist if a backup is interrupted by a power failure the system reverts to the prior complete backup. 

## Usage

Instillation through setup.py creates a system command 'rrbackup', in order to use this you must create "configuration.json". By default the application looks for this file in the current working directory, but an alternate location can be specified with --c [conf file path] as the first argument. A minimal configuration follows:

```json
{
    "base_path":                 "/path/to/directory/to/back/up",
    "local_manifest_file":       "/path/to/local/manifest",

    "s3" : {
        "access_key": "your aws access key",
        "secret_key": "your aws secret key",
        "bucket":     "aws bucket to use"
    },
}
```

These values are:

* "base_path"

--- This is the base path of the directory which will be backed up.

* "local manifest file"

--- Path to the local manifest file. The local manifest is a flat JSON file which stores the state of the files on the last run and is used for change detection. The manifest should not be in the base path as the system would detect it and needlessly back it up.

* "s3" : "access_key":
--- The access key of the AWS (or IAM) account you wish to back up to.

* "s3" : "secret_key":
--- The secret key of the AWS (or IAM) account you wish to back up to.

* "s3" : "bucket":
--- The secret key of the AWS (or IAM) account you wish to back up to. Note that this bucket must already exist and must have versioning enabled.


### Running

Once the configuration has been created With these in place you can run a backup by calling 'rrbackup' at the command line, it will detect the files within the directory and upload them.

Other parameters can be used to list and download backups which have been created:

* rrbackup list_versions

--- Lists all versions that exist, newest last


* rrbackup list_files [version_id]

--- List all files in a version


* rrbackup list_changes  [version_id]

--- List what files changed in the named version


* rrbackup download [version id]  [target] [ignore filters]

--- Download a file or files from the backup. This creates the target directory if it does not exist. Note that this will overwrite any existing files in the directory so using a new one is recommended.

--- Ignore filters is an optional parameter which specifies files that should not be downloaded, these are listed one per line and support Unix wildcards similar to gitignore. To ignore everything in the directory 'foo' in the root of the backed up directory you would use '/foo*'. The leading slash is required.


## Additional options

### File processing pipelines

By default this application applies no processing to backed up files, storing them exactly as-is. Pipelines of transformations can be applied using arbitrary wildcards to encrypt files, compress them or obfuscate there names. This can be used to restrict compression to known compressible files or apply encryption to sensitive data, storing things which are already public as-is. This avoids unneeded processing overhead.

Pipeline formats are expressed as a list of keywords and the order of items does not matter, 'hash_names', 'compress' and 'encrypt'. It does not matter what order you specify them, they are always handled in that order with encryption applied last. When downloading the order is reversed with names restored from the manifest. Please see the section 'encryption' below for usage of encryption feature.

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

The above says 'apply no processing to the file 'bar' in the root and 'hash names' and 'encrypt' to everything else.  Note that filters must always start with a slash '/. Also note that the order of these wildcards are listed matters: they are are evaluated top to bottom so must be most to least specific. For example placing a match all wildcard '*' first matches everything and following items will not be considered.


### Encryption

Encryption uses the 'secret stream' api provided by libsodium so can handle arbitrarily large files without excusing memory.  To make use of encryption first you must configure the encryption password in the conf file, this is processed using the ARGON2I13 key derivation function to create the encryption key:

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
    "meta_pipeline": ["compress", "encrypt"],
    "file_pipeline": [
        ["*", ["encrypt"]]
    ]
}
```


### Read only operation

If using this system to back up a server you may want a client on another computer, as S3 lacks synchronisation features it is a bad idea to have multiple clients writing to the same s3 bucket. The client can be configured to operate in read-only mode by adding the following to the configuration file. This can be enforced with IAM permissions.

```json
{
    "read_only":                 false,
    "allow_delete_versions":     true
}
```
 

### Write only operation

If using this to back up a server you may want to configure it to work in write-only mode enforced with IAM permissions. This feature is not currently implemented.


### Obfuscating the names of metadata files

If you wish to obfuscate the names of the Remote manifest diffs, remote GC log and password salt file this can be done by adding the following to the configuration:

```json
{
    "remote_manifest_diff_file": "asdfgjkll",
    "remote_gc_log_file":        "cvbnmoytety",
    "remote_password_salt_file": "qwertyuio"
}
```

Note that doing this adds little security as the function of these files can be deduced from how they are used. There contents may be protected with encryption as described above.


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

Sometimes you may want to add a file to a backup, keeping it in the backup but deleting it from the local file system to save space. An example being database snapshots. Such files should be added to 'ignore delete', they will be added when they appear in the filesystem but will not be deleted from the backup when removed. Once again these are evaluated top to bottom so be careful with wildcards.

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

The command line client is a thin interface to an underlying library, please see the command line client and 'core.py' for usage.