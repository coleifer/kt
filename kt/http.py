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
    cursor_id = 0

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

    def __del__(self):
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

    def report(self):
        resp, status = self.request('/report', {}, None)
        return resp

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

    def tune_replication(self, host=None, port=None, timestamp=None,
                         interval=None):
        data = {}
        if host is not None:
            data['host'] = host
        if port is not None:
            data['port'] = str(port)
        if timestamp is not None:
            data['ts'] = str(timestamp)
        if interval is not None:
            data['iv'] = str(interval)
        resp, status = self.request('/tune_replication', data, None)
        return status == 200

    def status(self, db=None):
        resp, status = self.request('/status', {}, db)
        return resp

    def clear(self, db=None):
        resp, status = self.request('/clear', {}, db)
        return status == 200

    def synchronize(self, hard=False, command=None, db=None):
        data = {}
        if hard:
            data['hard'] = ''
        if command is not None:
            data['command'] = command
        _, status = self.request('/synchronize', data, db)
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

    def remove(self, key, db=None):
        resp, status = self.request('/remove', {'key': key}, db, (450,))
        return status != 450

    def get(self, key, db=None):
        resp, status = self.request('/get', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp[self.decode_key(b'value')]
        return self.decode_value(value)

    def check(self, key, db=None):
        resp, status = self.request('/check', {'key': key}, db, (450,))
        return status != 450

    def seize(self, key, db=None):
        resp, status = self.request('/seize', {'key': key}, db, (450,))
        if status == 450:
            return
        value = resp[self.decode_key(b'value')]
        return self.decode_value(value)

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

    def vacuum(self, step=0, db=None):
        # If step > 0, the whole region is scanned.
        data = {'step': str(step)} if step > 0 else {}
        resp, status = self.request('/vacuum', data, db)
        return status == 200

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

    def _cursor_command(self, cmd, cursor_id, data, db=None):
        data['CUR'] = cursor_id
        resp, status = self.request('/%s' % cmd, data, db, (450, 501))
        if status == 501:
            raise NotImplementedError('%s is not supported' % cmd)
        return resp, status

    def cur_jump(self, cursor_id, key=None, db=None):
        data = {'key': key} if key else {}
        resp, s = self._cursor_command('cur_jump', cursor_id, data, db)
        return s == 200

    def cur_jump_back(self, cursor_id, key=None, db=None):
        data = {'key': key} if key else {}
        resp, s = self._cursor_command('cur_jump_back', cursor_id, data, db)
        return s == 200

    def cur_step(self, cursor_id):
        resp, status = self._cursor_command('cur_step', cursor_id, {})
        return status == 200

    def cur_step_back(self, cursor_id):
        resp, status = self._cursor_command('cur_step_back', cursor_id, {})
        return status == 200

    def cur_set_value(self, cursor_id, value, step=False, expire_time=None):
        data = {'value': self.encode_value(value)}
        if expire_time is not None:
            data['xt'] = str(expire_time)
        if step:
            data['step'] = ''
        resp, status = self._cursor_command('cur_set_value', cursor_id, data)
        return status == 200

    def cur_remove(self, cursor_id):
        resp, status = self._cursor_command('cur_remove', cursor_id, {})
        return status == 200

    def cur_get_key(self, cursor_id, step=False):
        data = {'step': ''} if step else {}
        resp, status = self._cursor_command('cur_get_key', cursor_id, data)
        if status == 450:
            return
        return self.decode_key(resp[self.decode_key(b'key')])

    def cur_get_value(self, cursor_id, step=False):
        data = {'step': ''} if step else {}
        resp, status = self._cursor_command('cur_get_value', cursor_id, data)
        if status == 450:
            return
        return self.decode_value(resp[self.decode_key(b'value')])

    def cur_get(self, cursor_id, step=False):
        data = {'step': ''} if step else {}
        resp, status = self._cursor_command('cur_get', cursor_id, data)
        if status == 450:
            return
        key = self.decode_key(resp[self.decode_key(b'key')])
        value = self.decode_key(resp[self.decode_key(b'value')])
        return (key, value)

    def cur_seize(self, cursor_id, step=False):
        resp, status = self._cursor_command('cur_seize', cursor_id, {})
        if status == 450:
            return
        key = self.decode_key(resp[self.decode_key(b'key')])
        value = self.decode_key(resp[self.decode_key(b'value')])
        return (key, value)

    def cur_delete(self, cursor_id):
        resp, status = self._cursor_command('cur_delete', cursor_id, {})
        return status == 200

    def cursor(self, cursor_id=None, db=None):
        if cursor_id is None:
            HttpProtocol.cursor_id += 1
            cursor_id = HttpProtocol.cursor_id
        return Cursor(self, cursor_id, db)

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

    def count(self, db=None):
        resp = self.status(db)
        return int(resp.get('count') or 0)

    def size(self, db=None):
        resp = self.status(db)
        return int(resp.get('size') or 0)


class Cursor(object):
    def __init__(self, protocol, cursor_id, db=None):
        self.protocol = protocol
        self.cursor_id = cursor_id
        self.db = db
        self._valid = False

    def __iter__(self):
        if not self._valid:
            self.jump()
        return self

    def is_valid(self):
        return self._valid

    def jump(self, key=None):
        self._valid = self.protocol.cur_jump(self.cursor_id, key, self.db)
        return self._valid

    def jump_back(self, key=None):
        self._valid = self.protocol.cur_jump_back(self.cursor_id, key, self.db)
        return self._valid

    def step(self):
        self._valid = self.protocol.cur_step(self.cursor_id)
        return self._valid

    def step_back(self):
        self._valid = self.protocol.cur_step_back(self.cursor_id)
        return self._valid

    def key(self):
        if self._valid:
            return self.protocol.cur_get_key(self.cursor_id)

    def value(self):
        if self._valid:
            return self.protocol.cur_get_value(self.cursor_id)

    def get(self):
        if self._valid:
            return self.protocol.cur_get(self.cursor_id)

    def set_value(self, value):
        if self._valid:
            if not self.protocol.cur_set_value(self.cursor_id, value):
                self._valid = False
        return self._valid

    def remove(self):
        if self._valid:
            if not self.protocol.cur_remove(self.cursor_id):
                self._valid = False
        return self._valid

    def seize(self):
        if self._valid:
            kv = self.protocol.cur_seize(self.cursor_id)
            if kv is None:
                self._valid = False
            return kv

    def close(self):
        if self._valid and self.protocol.cur_delete(self.cursor_id):
            self._valid = False
            return True
        return False

    def __next__(self):
        if not self._valid:
            raise StopIteration
        kv = self.protocol.cur_get(self.cursor_id)
        if kv is None:
            self._valid = False
            raise StopIteration
        elif not self.step():
            self._valid = False
        return kv
    next = __next__
