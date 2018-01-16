############################################################################################
def json_numeric_keys(from_json):
    """ Filters data returned from json handler and converts numeric keys into numbers """
    def get_value(value):
        if hasattr(value, '__iter__'): return js_keys(value)
        else: return value
    def convert_key(key):
        try: return int(key)
        except: return key
    if type(from_json) == dict:
        return {convert_key(k) : get_value(v) for k, v in from_json.iteritems()}
    elif type(from_json) == list:
        return [get_value(value) for value in from_json] 

