#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def serialise_pipeline_format(pl_format):
    """ For a given version the output of this MUST NOT CHANGE as it
    is used as additional data for validation"""
    if not isinstance(pl_format['version'], int):
        raise TypeError('Version must be an integer')

    serialised = 'V[' + str(pl_format['version']) + ']:'

    for i in pl_format['format']:
        if not (type(i) == unicode or type(i) == str):
            raise TypeError('Format specifiers must be strings')

        serialised += serialise_mapper[i]
        serialised += ':'
    return serialised[:-1]

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def parse_pipeline_format(serialised_pl_format):
    # Validate
    if serialised_pl_format[:2] != 'V[': raise ValueError('Unexpected initial character')
    f1 = re.compile(r'^[a-zA-Z]$'); f2 = re.compile(r'^[a-zA-Z]\[[0-9]+\]$')

    split = serialised_pl_format.split(':')
    for i in split:
        if not(f1.match(i) or f2.match(i)):
            raise ValueError('not a valid serialised pipeline format')

    res = {}
    for i in split:
        if len(i) == 1: res[i] = None
        elif i [1] == '[': res[i[0]] = i[2:-1]
        else: raise ValueError('this should not happen')

    pl_format = {'version' : res.pop('V'),
                 'format'  : []}

    inv_map = {v: k for k, v in serialise_mapper.iteritems()}

    for k, v in res.iteritems():
        pl_format['format'].append(inv_map[k])

    return pl_format


