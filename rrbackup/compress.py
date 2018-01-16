import bz2, pipeline

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# One-shot compression and decompression
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def compress(child, data, meta, config):
    pl_format = pipeline.parse_pipeline_format(meta['header'])
    pl_format['format']['compress'] = {'A' : 'bz2'}
    meta['header'] = pipeline.serialise_pipeline_format(pl_format)
    final = bz2.compress(data)
    return child(final, meta, config)

def decompress(child, meta, config):
    data, meta2 = child(meta, config)
    final = bz2.decompress(data)
    return final, meta2

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# Streaming (chunked) compression and decompression
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==

# compression object, store chunk length as fixed size entity at
# start of stream, if compressed result less than chunk size
# grab another chunk from the provider. Merge n chunks, truncating
# the last one and adding remainder to next chunk, Store in header.
