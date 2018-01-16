import hashlib, copy, pipeline

def hash(child, data, meta, config):
    meta = copy.deepcopy(meta)
    path = meta['path']
    hashed_path = hashlib.sha256(path).hexdigest()
    meta['path'] = hashed_path
    res_meta = child(data, meta, config)
    res_meta['path'] = path
    res_meta['hashed_path'] = hashed_path
    return res_meta

def restore(child, meta, config):
    meta = copy.deepcopy(meta)
    path = meta['path']
    hashed_path = hashlib.sha256(path).hexdigest()
    meta['path'] = hashed_path
    data, meta2 = child(meta, config)
    meta2['path'] = path
    meta2['hashed_path'] = hashed_path
    return data, meta2
