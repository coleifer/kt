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
                 json_values=False, auto_connect=True):
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
        if auto_connect:
            self.open()

    def _encode_keys_values(self, data):
        accum = []
        for key, value in data.items():
            bkey = encode(key)
            bvalue = encode(value)
            accum.append(b'%s\t%s' % (b64encode(bkey), b64encode(bvalue)))

        return b'\n'.join(accum)

    def _encode_keys(self, keys):
        accum = []
        for key in keys:
            accum.append(b'%s\t' % b64encode(b'_' + encode(key)))
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
            accum[key] = value

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

    def _post(self, path, body, db):
        if db is None:
            db = self._default_db
        if db is not False:
            path += '?DB=%s' % db
        return self._session.post(self.path(path), data=body)

    def request(self, path, data, db=None, allowed_status=None):
        if isinstance(data, dict):
            body = self._encode_keys_values(data)
        elif isinstance(data, list):
            body = self._encode_keys(data)
        else:
            body = data

        r = self._post(path, body, db)
        if r.status_code != 200:
            if allowed_status is None or r.status_code not in allowed_status:
                raise KyotoTycoonError('protocol error [%s]' % r.status_code)

        return (self._decode_response(r.content, r.headers['content-type']),
                r.status_code)

    def status(self, db=None):
        resp, status = self.request('/status', {}, db)
        return resp

    def report(self):
        resp, status = self.request('/report', {}, None)
        return resp

    def clear(self, db=None):
        resp, status = self.request('/clear', {}, db)
        return True

    def play_script(self, name, __data=None, **params):
        if __data is not None:
            params.update(__data)

        accum = {}
        for key, value in params.items():
            accum['_%s' % key] = self._encode_value(value)

        resp, status = self.request('/play_script', accum, False, (450,))
        if status == 450:
            return

        accum = {}
        for key, value in resp.items():
            accum[key[1:]] = self._decode_value(value)
        return accum

    def get(self, key, db=None):
        resp, status = self.request('/get', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp['value' if self._decode_keys else b'value']
        return self._decode_value(value)

    def _simple_write(self, cmd, key, value, db=None, expire_time=None):
        data = {'key': key, 'value': self._encode_value(value)}
        if expire_time is not None:
            data['xt'] = str(expire_time)
        resp, status = self.request('/%s' % cmd, data, db, (450,))
        return status != 450

    def set(self, key, value, db=None, expire_time=None):
        return self._simple_write('set', key, value, db, expire_time)

    def add(self, key, value, db=None, expire_time=None):
        return self._simple_write('add', key, value, db, expire_time)

    def replace(self, key, value, db=None, expire_time=None):
        return self._simple_write('replace', key, value, db, expire_time)

    def append(self, key, value, db=None, expire_time=None):
        return self._simple_write('append', key, value, db, expire_time)

    def remove(self, key, db=None):
        resp, status = self.request('/remove', {'key': key}, db, (450,))
        return status != 450

    def check(self, key, db=None):
        resp, status = self.request('/check', {'key': key}, db, (450,))
        return status != 450

    def set_bulk(self, __data=None, **params):
        db = params.pop('db', None)
        expire_time = params.pop('expire_time', None)
        if __data is not None:
            params.update(__data)

        accum = {}
        if expire_time is not None:
            accum['xt'] = str(expire_time)

        # Keys must be prefixed by "_".
        for key, value in params.items():
            accum['_%s' % key] = self._encode_value(value)

        resp, status = self.request('/set_bulk', accum, db)
        return resp

    def get_bulk(self, keys, db=None):
        resp, status = self.request('/get_bulk', keys, db)

        n = resp.pop('num' if self._decode_keys else b'num', b'0')
        if n == b'0':
            return {}

        accum = {}
        for key, value in resp.items():
            accum[key[1:]] = self._decode_value(value)
        return accum

    def remove_bulk(self, keys, db=None):
        resp, status = self.request('/remove_bulk', keys, db)
        return int(resp.pop('num' if self._decode_keys else b'num'))

    def seize(self, key, db=None):
        resp, status = self.request('/seize', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp['value' if self._decode_keys else b'value']
        return self._decode_value(value)

    def cas(self, key, old_val, new_val, db=None, expire_time=None):
        if old_val is None and new_val is None:
            raise ValueError('old value and/or new value must be specified.')

        data = {'key': key}
        if old_val is not None:
            data['oval'] = self._encode_value(old_val)
        if new_val is not None:
            data['nval'] = self._encode_value(new_val)
        if expire_time is not None:
            data['xt'] = str(expire_time)

        resp, status = self.request('/cas', data, db, (450,))
        return status != 450
