from base64 import b64decode
from base64 import b64encode
from functools import partial
import pickle
import sys
try:
    from urllib.parse import quote_from_bytes
    from urllib.parse import unquote_to_bytes
    from urllib.parse import urlencode
except ImportError:
    from urllib import quote as quote_from_bytes
    from urllib import unquote as unquote_to_bytes
    from urllib import urlencode

try:
    import msgpack
    m_packb = msgpack.packb
    m_unpackb = msgpack.unpackb
except ImportError:
    msgpack = m_packb = m_unpackb = None

import requests


IS_PY2 = sys.version_info[0] == 2

if not IS_PY2:
    unicode = str


quote_b = partial(quote_from_bytes, safe='')
unquote_b = partial(unquote_to_bytes, safe='')

# Pickle.
p_dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
p_loads = pickle.loads


def encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif isinstance(s, bytes):
        return s
    elif s is None:
        return b''
    elif not IS_PY2:
        return str(s).decode('utf-8')
    return str(s)

def decode(b):
    if isinstance(b, bytes):
        return b.decode('utf-8')
    else:
        return b

def decode_from_content_type(content_type):
    if content_type.endswith('colenc=B'):
        return b64decode
    elif content_type.endswith('colenc=U'):
        return unquote_b

class KyotoTycoonError(Exception): pass


class HTTPKyotoTycoon(object):
    _content_type = 'text/tab-separated-values; colenc=B'

    def __init__(self, host='127.0.0.1', port=1978, default_db=0,
                 decode_keys=True, pickle_values=False, msgpack_values=False,
                 json_values=False):
        self._host = host
        self._port = port
        self._default_db = default_db
        self._decode_keys = decode_keys
        if msgpack_values and msgpack is None:
            raise Exception('could not import "msgpack" library')

        if pickle_values:
            self._encode_value = p_dumps
            self._decode_value = p_loads
        elif msgpack_values:
            self._encode_value = m_packb
            self._decode_value = m_unpackb
        elif json_values:
            self._encode_value = lambda v: (json
                                            .dumps(v, separators=(',', ':'))
                                            .encode('utf-8'))
            self._decode_value = lambda v: json.loads(v.decode('utf-8'))
        else:
            self._encode_value = encode
            self._decode_value = decode

        self._prefix = 'http://%s:%s/rpc' % (self._host, self._port)
        self._session = None

    def _encode_dict(self, data):
        accum = []
        for key, value in data.items():
            bkey = encode(key)
            bvalue = self._encode_value(value)
            accum.append(b'%s\t%s' % (b64encode(bkey), b64encode(bvalue)))

        return b'\n'.join(accum)

    def _encode_keys(self, keys):
        accum = []
        for key in keys:
            bkey = b'_' + encode(key)
            accum.append(b'%s\t' % b64encode(bkey))
        return b'\n'.join(accum)

    def _decode_response(self, tsv, content_type):
        decoder = decode_from_content_type(content_type)
        accum = {}
        for line in tsv.split(b'\n'):
            try:
                key, value = line.split(b'\t', 1)
            except ValueError:
                continue

            if decoder is not None:
                key, value = decoder(key), decoder(value)

            if self._decode_keys:
                key = decode(key)
            accum[key] = self._decode_value(value)

        return accum

    def path(self, url):
        return ''.join((self._prefix, url))

    def open(self):
        if self._session is not None:
            return False

        self._session = requests.Session()
        self._session.headers['Content-Type'] = self._content_type
        return True

    def close(self):
        if self._session is None:
            return False

        self._session = None
        return True

    def _post(self, path, data, db=None):
        if db is None: db = self._default_db
        if isinstance(data, dict):
            body = self._encode_dict(data)
        else:
            body = data
        r = self._session.post(self.path(path + '?DB=%s' % db), data=body)
        return (self._decode_response(r.content, r.headers['content-type']),
                r.status_code)

    def _check_error(self, status_code):
        if status_code != 200:
            raise KyotoTycoonError('protocol error [%s]' % status_code)

    def get(self, key):
        resp, status = self._post('/get', {'key': key})
        if status == 404:
            return

        self._check_error(status)
        value = b'value' if not self._decode_keys else 'value'
        return resp[value]

    def set(self, key, value):
        resp, status = self._post('/set', {'key': key, 'value': value})
        self._check_error(status)
        return resp

    def check(self, key):
        resp, status = self._post('/check', {'key': key})
        if status == 450:  # Record not found.
            return False
        self._check_error(status)
        return True

    def set_bulk(self, __data=None, **params):
        if __data is not None:
            params.update(__data)
        accum = {}
        for key in params:
            accum['_%s' % key] = params[key]
        resp, status = self._post('/set_bulk', accum)
        self._check_error(status)
        return resp

    def get_bulk(self, keys):
        data = self._encode_keys(keys)
        resp, status = self._post('/get_bulk', data)
        self._check_error(status)

        num = 'num' if self._decode_keys else b'num'
        n = resp.pop(num)
        if n == '0':
            return {}

        accum = {}
        for key, value in resp.items():
            accum[key[1:]] = value
        return accum

    def remove_bulk(self, keys):
        data = self._encode_keys(keys)
        resp, status = self._post('/remove_bulk', data)
        self._check_error(status)
        return resp
