from base64 import b64decode
from base64 import b64encode
from functools import partial
import datetime
import sys
try:
    from urllib.parse import quote_from_bytes
    from urllib.parse import unquote_to_bytes
    from urllib.parse import urlencode
except ImportError:
    from urllib import quote as quote_from_bytes
    from urllib import unquote as unquote_to_bytes
    from urllib import urlencode

import requests

from ._binary import decode
from ._binary import encode
from ._binary import noop_decode
from .exceptions import ProtocolError
from .exceptions import ServerError


IS_PY2 = sys.version_info[0] == 2

if not IS_PY2:
    unicode = str


quote_b = partial(quote_from_bytes, safe='')
unquote_b = partial(unquote_to_bytes, safe='')


def decode_from_content_type(content_type):
    if content_type.endswith('colenc=B'):
        return b64decode
    elif content_type.endswith('colenc=U'):
        return unquote_b


class HttpProtocol(object):
    _content_type = 'text/tab-separated-values; colenc=B'

    def __init__(self, host='127.0.0.1', port=1978, decode_keys=True,
                 encode_value=None, decode_value=None, timeout=None):
        self._host = host
        self._port = port
        if decode_keys:
            self.decode_key = decode
        else:
            self.decode_key = noop_decode
        self.encode_value = encode_value or encode
        self.decode_value = decode_value or decode
        self._prefix = 'http://%s:%s/rpc' % (self._host, self._port)
        self._session = requests.Session()
        self._session.headers['Content-Type'] = self._content_type

    def close(self):
        self._session.close()

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

            accum[self.decode_key(key)] = value

        return accum

    def path(self, url):
        return ''.join((self._prefix, url))

    def _post(self, path, body, db):
        return self._session.post(self.path(path), data=body)

    def request(self, path, data, db=None, allowed_status=None):
        if isinstance(data, dict):
            body = self._encode_keys_values(data)
        elif isinstance(data, list):
            body = self._encode_keys(data)
        else:
            body = data

        if db is not False:
            db_data = self._encode_keys_values({'DB': db or 0})
            if body:
                body = b'\n'.join((body, db_data))
            else:
                body = db_data

        r = self._post(path, body, db)
        if r.status_code != 200:
            if allowed_status is None or r.status_code not in allowed_status:
                raise ProtocolError('protocol error [%s]' % r.status_code)

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
        return status == 200

    def script(self, name, __data=None, **params):
        if __data is not None:
            params.update(__data)

        accum = {}
        for key, value in params.items():
            accum['_%s' % key] = self.encode_value(value)

        resp, status = self.request('/play_script', accum, False, (450,))
        if status == 450:
            return

        accum = {}
        for key, value in resp.items():
            accum[key[1:]] = self.decode_value(value)
        return accum

    def get(self, key, db=None):
        resp, status = self.request('/get', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp[self.decode_key(b'value')]
        return self.decode_value(value)

    def ulog_list(self):
        resp, status = self.request('/ulog_list', {}, None)
        log_list = []
        for filename, meta in resp.items():
            size, ts_str = meta.decode('utf-8').split(':')
            ts = datetime.datetime.fromtimestamp(int(ts_str) / 1e9)
            log_list.append((filename, size, ts))
        return log_list

    def ulog_remove(self, max_dt=None):
        max_dt = max_dt or datetime.datetime.now()
        data = {'ts': str(int(max_dt.timestamp() * 1e9))}
        resp, status = self.request('/ulog_remove', data, None)
        return status == 200

    def synchronize(self, hard=False, db=None):
        data = {'hard': ''} if hard else {}
        _, status = self.request('/synchronize', data, db)
        return status == 200

    def count(self, db=None):
        resp = self.status(db)
        return int(resp.get('count') or 0)

    def size(self, db=None):
        resp = self.status(db)
        return int(resp.get('size') or 0)

    def vacuum(self, step=0, db=None):
        # If step > 0, the whole region is scanned.
        data = {'step': str(step)} if step > 0 else {}
        resp, status = self.request('/vacuum', data, db)
        return status == 200

    def _simple_write(self, cmd, key, value, db=None, expire_time=None):
        data = {'key': key, 'value': self.encode_value(value)}
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

    def set_bulk(self, data, db=0, expire_time=None):
        accum = {}
        if expire_time is not None:
            accum['xt'] = str(expire_time)

        # Keys must be prefixed by "_".
        for key, value in data.items():
            accum['_%s' % key] = self.encode_value(value)

        resp, status = self.request('/set_bulk', accum, db)
        return resp

    def remove_bulk(self, keys, db=None):
        resp, status = self.request('/remove_bulk', keys, db)
        return int(resp.pop(self.decode_key(b'num')))

    def seize(self, key, db=None):
        resp, status = self.request('/seize', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp[self.decode_key(b'value')]
        return self.decode_value(value)

    def cas(self, key, old_val, new_val, db=None, expire_time=None):
        if old_val is None and new_val is None:
            raise ValueError('old value and/or new value must be specified.')

        data = {'key': key}
        if old_val is not None:
            data['oval'] = self.encode_value(old_val)
        if new_val is not None:
            data['nval'] = self.encode_value(new_val)
        if expire_time is not None:
            data['xt'] = str(expire_time)

        resp, status = self.request('/cas', data, db, (450,))
        return status != 450

    def increment(self, key, n=1, orig=None, db=None, expire_time=None):
        data = {'key': key, 'num': str(n)}
        if orig is not None:
            data['orig'] = str(orig)
        if expire_time is not None:
            data['xt'] = str(expire_time)
        resp, status = self.request('/increment', data, db)
        return int(resp['num'])

    def increment_double(self, key, n=1, orig=None, db=None, expire_time=None):
        data = {'key': key, 'num': str(n)}
        if orig is not None:
            data['orig'] = str(orig)
        if expire_time is not None:
            data['xt'] = str(expire_time)
        resp, status = self.request('/increment_double', data, db)
        return float(resp['num'])

    def _do_bulk_command(self, cmd, params, db=None):
        resp, status = self.request(cmd, params, db)

        n = resp.pop(self.decode_key(b'num'))
        if n == b'0':
            return {}

        accum = {}
        for key, value in resp.items():
            accum[key[1:]] = self.decode_value(value)
        return accum

    def get_bulk(self, keys, db=None):
        return self._do_bulk_command('/get_bulk', keys, db)

    def _do_bulk_sorted_command(self, cmd, params, db=None):
        results = self._do_bulk_command(cmd, params, db)
        return sorted(results, key=lambda k: int(results[k]))

    def match_prefix(self, prefix, max_keys=None, db=None):
        data = {'prefix': prefix}
        if max_keys is not None:
            data['max'] = str(max_keys)
        return self._do_bulk_sorted_command('/match_prefix', data, db)

    def match_regex(self, regex, max_keys=None, db=None):
        data = {'regex': regex}
        if max_keys is not None:
            data['max'] = str(max_keys)
        return self._do_bulk_sorted_command('/match_regex', data, db)

    def match_similar(self, origin, distance=None, max_keys=None, db=None):
        data = {'origin': origin, 'utf': 'true'}
        if distance is not None:
            data['range'] = str(distance)
        if max_keys is not None:
            data['max'] = str(max_keys)
        return self._do_bulk_sorted_command('/match_similar', data, db)
