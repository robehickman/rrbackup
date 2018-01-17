"""
When data is uploaded or downloaded an arbitrary set of transformations
may be applied to the data in transit including encryption. This file
assembles pipelines to apply these transformations depending on configuration.
"""
import crypto, compress, functools, json, re

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def preprocess_config(interface, conn, config):
    """ apply transformations to configuration data which should only be done once,
    for example key derivation """

    return crypto.preprocess_config(interface, conn, config)

#================================================================
#================================================================
def get_default_pipeline_format():
    return {'version' : 1,
            'format'  : {}}

#------------------
serialise_mapper = {'encrypt'    : 'E'.encode('utf8'),
                    'compress'   : 'C'.encode('utf8'),
                    'hash_names' : 'H'.encode('utf8')}

def serialise_pipeline_format(pl_format):
    """ For a given version the output of this MUST NOT CHANGE as it
    is used as additional data for validation"""
    if not isinstance(pl_format['version'], int):
        raise TypeError('Version must be an integer')

    to_json = {'V' : str(pl_format['version'])}

    for i in pl_format['format']:
        if not (type(i) == unicode or type(i) == str):
            raise TypeError('Format specifiers must be strings')

        if type(pl_format['format'][i]) == dict:  to_json[serialise_mapper[i]] = pl_format['format'][i]
        elif pl_format['format'][i] == None: to_json[serialise_mapper[i]] = ''
        else: raise TypeError('Unexpected type')

    return json.dumps(to_json, separators=(',',':'))

def parse_pipeline_format(serialised_pl_format):
    raw = json.loads(serialised_pl_format)

    if 'V' not in raw: raise ValueError('Version not found')
    version = raw.pop('V')
    if not re.compile(r'^[0-9]+$').match(version): raise ValueError('Invalid version number')

    pl_format = {'version' : int(version),
                 'format'  : {}}

    inv_map = {v: k for k, v in serialise_mapper.iteritems()}
    for k, v in raw.iteritems():
        if v == '': pl_format['format'][inv_map[k.encode('utf8')]] = None
        elif type(v) == dict: pl_format['format'][inv_map[k.encode('utf8')]] = v
        else: raise ValueError('Unknown type in serialised pipeline format')

    return pl_format

#================================================================
#================================================================
def build_pipeline(interface, direction, pipeline_format):
    """ Build a flat pipeline of transformers,

        Direction specifies whether processing data heading to storage or returning,
        it has two valid options: out or in.

        Pipeline_format is the processing options which should be enabled:
        'hash_names', 'compress' and 'encrypt'. If direction is out, they
        are applied to the content, if in they are removed.

        Config is a dict of currently only 'box_crypt_key', the encryption key.
     """

    if type(pipeline_format) != dict: raise TypeError('pipeline format must be a dict')

    pipeline = interface

    if direction == 'out':
        # Remember that these are executed in the reverse order than they are listed
        if 'encrypt' in pipeline_format:
            pipeline = functools.partial(crypto.encrypt, pipeline)

        if 'compress' in pipeline_format:
            pipeline = functools.partial(compress.compress, pipeline)

    elif direction == 'in':
        if 'encrypt' in pipeline_format:
            pipeline = functools.partial(crypto.decrypt, pipeline)

        if 'compress' in pipeline_format:
            pipeline = functools.partial(compress.decompress, pipeline)
    else:
        raise ValueError('Unknown pipeline direction')

    return pipeline

# -----------------
def build_pipeline_streaming(interface, direction, pipeline_format, config):
    """ Build a chunked (streaming) pipeline of transformers """

    pipeline = interface

    if direction == 'out':
        if 'compress' in pipeline_format:
            pass

        if 'encrypt' in pipeline_format:
            pipeline = crypto.streaming_encrypt(pipeline)

    elif direction == 'in':
        if 'encrypt' in pipeline_format:
            pipeline = crypto.streaming_decrypt(pipeline)

        if 'compress' in pipeline_format:
            pass

    # ----
    pipeline.pass_config(config)
    return pipeline

