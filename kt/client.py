from contextlib import contextmanager
from functools import partial
import json
import re
import socket
import sys
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import msgpack
except ImportError:
    msgpack = None

from ._binary import KTBinaryProtocol
from ._binary import TTBinaryProtocol
from ._binary import decode
from ._binary import dict_to_table
from ._binary import encode
from ._binary import table_to_dict
from .constants import IOP_DELETE
from .constants import IOP_KEEP
from .constants import IOP_OPTIMIZE
from .constants import ORDER_STR_ASC
from .exceptions import ImproperlyConfigured
from .exceptions import KyotoTycoonError
from .exceptions import ProtocolError
from .exceptions import ServerConnectionError
from .exceptions import ServerError
from .http import HttpProtocol


if sys.version_info[0] > 2:
    basestring = (bytes, str)


KT_BINARY = 'binary'
KT_JSON = 'json'
KT_MSGPACK = 'msgpack'
KT_NONE = 'none'
KT_PICKLE = 'pickle'
TT_TABLE = 'table'
KT_SERIALIZERS = set((KT_BINARY, KT_JSON, KT_MSGPACK, KT_NONE, KT_PICKLE,
                      TT_TABLE))


class BaseClient(object):
    def __init__(self, host='127.0.0.1', port=1978, serializer=KT_BINARY,
                 decode_keys=True, timeout=None, connection_pool=False):
        self._host = host
        self._port = port
        self._serializer = serializer
        self._decode_keys = decode_keys
        self._timeout = timeout
        self._connection_pool = connection_pool

        if self._serializer == KT_MSGPACK and msgpack is None:
            raise ImproperlyConfigured('msgpack library not found')
        elif self._serializer == KT_BINARY:
            self._encode_value = encode
            self._decode_value = decode
        elif self._serializer == KT_JSON:
            self._encode_value = lambda v: (json
                                            .dumps(v, separators=(',', ':'))
                                            .encode('utf-8'))
            self._decode_value = lambda v: json.loads(v.decode('utf-8'))
        elif self._serializer == KT_MSGPACK:
            self._encode_value = lambda o: msgpack.packb(o, use_bin_type=True)
            self._decode_value = lambda b: msgpack.unpackb(b, raw=False)
        elif self._serializer == KT_NONE:
            self._encode_value = encode
            self._decode_value = lambda x: x
        elif self._serializer == KT_PICKLE:
            self._encode_value = partial(pickle.dumps,
                                         protocol=pickle.HIGHEST_PROTOCOL)
            self._decode_value = pickle.loads
        elif self._serializer == TT_TABLE:
            self._encode_value = dict_to_table
            self._decode_value = table_to_dict
        else:
            raise ImproperlyConfigured('unrecognized serializer "%s" - use one'
                                       ' of: %s' % (self._serializer,
                                                    ','.join(KT_SERIALIZERS)))

        # Session and socket used for rpc and binary protocols, respectively.
        self._initialize_protocols()

    @property
    def lua(self):
        if not hasattr(self, '_script_runner'):
            self._script_runner = ScriptRunner(self)
        return self._script_runner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._protocol.close()

    def close(self, allow_reuse=True):
        self._protocol.close(allow_reuse)

    def close_all(self):
        return self._protocol.close_all()

    def close_idle(self, cutoff=60):
        return self._protocol.close_idle(cutoff)


class ScriptRunner(object):
    def __init__(self, client):
        self.client = client

    def __getattr__(self, attr_name):
        def run_script(*args, **kwargs):
            return self.client._script(attr_name, *args, **kwargs)
        return run_script


class KyotoTycoon(BaseClient):
    def __init__(self, *args, **kwargs):
        self._default_db = kwargs.pop('default_db', 0)
        super(KyotoTycoon, self).__init__(*args, **kwargs)

    def _initialize_protocols(self):
        # Protocol handlers.
        self._protocol = KTBinaryProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=self._encode_value,
            decode_value=self._decode_value,
            timeout=self._timeout,
            connection_pool=self._connection_pool,
            default_db=self._default_db)
        self._http = HttpProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=self._encode_value,
            decode_value=self._decode_value,
            default_db=self._default_db)

    def close(self, allow_reuse=True):
        self._protocol.close(allow_reuse)
        self._http.close()

    def get_bulk(self, keys, db=None, decode_values=True):
        return self._protocol.get_bulk(keys, db, decode_values)

    def get_bulk_details(self, keys, db=None, decode_values=True):
        return self._protocol.get_bulk_details(keys, db, decode_values)

    def get_bulk_raw(self, db_key_list, decode_values=True):
        return self._protocol.get_bulk_raw(db_key_list, decode_values)

    def get_bulk_raw_details(self, db_key_list, decode_values=True):
        return self._protocol.get_bulk_raw_details(db_key_list, decode_values)

    def get(self, key, db=None):
        return self._protocol.get(key, db, True)

    def get_bytes(self, key, db=None):
        return self._protocol.get(key, db, False)

    def set_bulk(self, data, db=None, expire_time=None, no_reply=False,
                 encode_values=True):
        return self._protocol.set_bulk(data, db, expire_time, no_reply,
                                       encode_values)

    def set_bulk_raw(self, data, no_reply=False, encode_values=True):
        return self._protocol.set_bulk_raw(data, no_reply, encode_values)

    def set(self, key, value, db=None, expire_time=None, no_reply=False):
        return self._protocol.set(key, value, db, expire_time, no_reply, True)

    def set_bytes(self, key, value, db=None, expire_time=None, no_reply=False):
        return self._protocol.set(key, value, db, expire_time, no_reply, False)

    def remove_bulk(self, keys, db=None, no_reply=False):
        return self._protocol.remove_bulk(keys, db, no_reply)

    def remove_bulk_raw(self, db_key_list, no_reply=False):
        return self._protocol.remove_bulk_raw(db_key_list, no_reply)

    def remove(self, key, db=None, no_reply=False):
        return self._protocol.remove(key, db, no_reply)

    def _script(self, name, __data=None, no_reply=False, encode_values=True,
                decode_values=True, **kwargs):
        if __data is None:
            __data = kwargs
        elif kwargs:
            __data.update(kwargs)
        return self._protocol.script(name, __data, no_reply, encode_values,
                                     decode_values)

    def script(self, name, data=None, no_reply=False, encode_values=True,
               decode_values=True):
        return self._protocol.script(name, data, no_reply, encode_values,
                                     decode_values)

    def clear(self, db=None):
        return self._http.clear(db)

    def status(self, db=None):
        return self._http.status(db)

    def report(self):
        return self._http.report()

    def ulog_list(self):
        return self._http.ulog_list()

    def ulog_remove(self, max_dt):
        return self._http.ulog_remove(max_dt)

    def synchronize(self, hard=False, command=None, db=None):
        return self._http.synchronize(hard, command, db)

    def vacuum(self, step=0, db=None):
        return self._http.vacuum(step, db)

    def add(self, key, value, db=None, expire_time=None, encode_value=True):
        return self._http.add(key, value, db, expire_time, encode_value)

    def replace(self, key, value, db=None, expire_time=None,
                encode_value=True):
        return self._http.replace(key, value, db, expire_time, encode_value)

    def append(self, key, value, db=None, expire_time=None, encode_value=True):
        return self._http.append(key, value, db, expire_time, encode_value)

    def exists(self, key, db=None):
        return self._http.check(key, db)

    def length(self, key, db=None):
        return self._http.length(key, db)

    def seize(self, key, db=None, decode_value=True):
        return self._http.seize(key, db, decode_value)

    def cas(self, key, old_val, new_val, db=None, expire_time=None,
            encode_value=True):
        return self._http.cas(key, old_val, new_val, db, expire_time,
                              encode_value)

    def incr(self, key, n=1, orig=None, db=None, expire_time=None):
        return self._http.increment(key, n, orig, db, expire_time)

    def incr_double(self, key, n=1., orig=None, db=None, expire_time=None):
        return self._http.increment_double(key, n, orig, db, expire_time)

    def _kdb_from_key(self, key):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError('expected key-tuple of (key, db)')
            return key
        return (key, None)

    def __getitem__(self, key):
        return self.get(*self._kdb_from_key(key))

    def __setitem__(self, key, value):
        key, db = self._kdb_from_key(key)
        if isinstance(value, tuple):
            if len(value) != 2:
                raise ValueError('expected value-tuple of (value, expires)')
            value, expire_time = value
        else:
            expire_time = None
        self._protocol.set(key, value, db, expire_time, no_reply=True)

    def __delitem__(self, key):
        self.remove(*self._kdb_from_key(key))

    def update(self, __data=None, **kwargs):
        if __data is None:
            __data = kwargs
        elif kwargs:
            __data.update(kwargs)
        return self.set_bulk(__data)

    pop = seize

    def __contains__(self, key):
        return self.exists(*self._kdb_from_key(key))

    def __len__(self):
        return int(self.status()['count'])

    def count(self, db=None):
        return int(self.status(db)['count'])

    def match_prefix(self, prefix, max_keys=None, db=None):
        return self._http.match_prefix(prefix, max_keys, db)

    def match_regex(self, regex, max_keys=None, db=None):
        return self._http.match_regex(regex, max_keys, db)

    def match_similar(self, origin, distance=None, max_keys=None, db=None):
        return self._http.match_similar(origin, distance, max_keys, db)

    def cursor(self, db=None, cursor_id=None):
        return self._http.cursor(cursor_id, db)

    def keys(self, db=None):
        cursor = self.cursor(db=db)
        if not cursor.jump(): return
        while True:
            key = cursor.key()
            if key is None: return
            yield key
            if not cursor.step(): return

    def keys_nonlazy(self, db=None):
        return self.match_prefix('', db=db)

    def values(self, db=None):
        cursor = self.cursor(db=db)
        if not cursor.jump(): return
        while True:
            value = cursor.value()
            if value is None: return
            yield value
            if not cursor.step(): return

    def items(self, db=None):
        cursor = self.cursor(db=db)
        if not cursor.jump(): return
        while True:
            kv = cursor.get()
            if kv is None: return
            yield kv
            if not cursor.step(): return

    def __iter__(self):
        return iter(self.keys())

    @property
    def size(self):
        return int(self.status()['size'])

    @property
    def path(self):
        return decode(self.status()['path'])

    def set_database(self, db):
        self._default_database = db
        self._protocol.set_database(db)
        self._http.set_database(db)
        return self


class TokyoTyrant(BaseClient):
    def _initialize_protocols(self):
        self._protocol = TTBinaryProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=self._encode_value,
            decode_value=self._decode_value,
            timeout=self._timeout,
            connection_pool=self._connection_pool)

    def get_bulk(self, keys, decode_values=True):
        return self._protocol.mget(keys, decode_values)

    def get(self, key):
        return self._protocol.get(key, True)

    def get_bytes(self, key):
        return self._protocol.get(key, False)

    def set_bulk(self, data, no_reply=False, encode_values=True):
        if no_reply:
            self._protocol.putnr_bulk(data, encode_values)
        else:
            return self._protocol.misc_putlist(data, True, encode_values)

    def set(self, key, value, no_reply=False):
        if no_reply:
            self._protocol.putnr(key, value, True)
        else:
            return self._protocol.put(key, value, True)

    def set_bytes(self, key, value):
        if no_reply:
            self._protocol.putnr(key, value, False)
        else:
            return self._protocol.put(key, value, False)

    def remove_bulk(self, keys):
        return self._protocol.misc_outlist(keys)

    def remove(self, key):
        return self._protocol.out(key)

    def script(self, name, key=None, value=None, lock_records=False,
               lock_all=False, encode_value=True, decode_value=False,
               as_list=False, as_dict=False, as_int=False):
        res = self._protocol.ext(name, key, value, lock_records, lock_all,
                                 encode_value, decode_value)
        if as_list or as_dict:
            # In the event the return value is an empty string, then we just
            # return the empty container type.
            if not res:
                return {} if as_dict else []

            # Split on newlines -- dicts are additionally split on tabs.
            delim = '\n' if decode_value else b'\n'
            res = res.rstrip(delim).split(delim)
            if as_dict:
                delim = '\t' if decode_value else b'\t'
                res = dict([r.split(delim) for r in res])
        elif as_int:
            res = int(res) if res else None
        return res
    _script = script

    def clear(self):
        return self._protocol.vanish()

    def status(self):
        data = self._protocol.stat()
        status = {}
        for key_value in data.decode('utf-8').splitlines():
            key, val = key_value.split('\t', 1)
            if val.replace('.', '').isdigit():
                try:
                    val = float(val) if val.find('.') >= 0 else int(val)
                except ValueError:
                    pass
            status[key] = val
        return status

    def synchronize(self):
        return self._protocol.sync()

    def optimize(self, options):
        return self._protocol.optimize(options)

    def add(self, key, value, encode_value=True):
        return self._protocol.putkeep(key, value, encode_value)

    def append(self, key, value, encode_value=True):
        return self._protocol.putcat(key, value, encode_value)

    def addshl(self, key, value, width, encode_value=True):
        return self._protocol.putshl(key, value, width, encode_value)

    def exists(self, key):
        return self._protocol.vsiz(key) is not None

    def length(self, key):
        return self._protocol.vsiz(key)

    def seize(self, key, decode_value=True):
        return self._protocol.seize(key, decode_value)

    def incr(self, key, n=1):
        return self._protocol.addint(key, n)

    def incr_double(self, key, n=1.):
        return self._protocol.adddouble(key, n)

    def count(self):
        return self._protocol.rnum()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_range(item.start, item.stop or None)
        else:
            return self.get(item)

    def __setitem__(self, key, value):
        self._protocol.putnr(key, value, True)

    __delitem__ = remove
    __contains__ = exists
    __len__ = count
    pop = seize

    def update(self, __data=None, no_reply=False, encode_values=True, **kw):
        if __data is None:
            __data = kw
        elif kw:
            __data.update(kw)
        return self.set_bulk(__data, no_reply, encode_values)

    def setdup(self, key, value, encode_value=True):
        return self._protocol.misc_putdup(key, value, True, encode_value)

    def setdupback(self, key, value, encode_value=True):
        return self._protocol.misc_putdupback(key, value, True, encode_value)

    def get_part(self, key, start=None, end=None, decode_value=True):
        return self._protocol.misc_getpart(key, start or 0, end, decode_value)

    def misc(self, cmd, args=None, update_log=True, decode_values=False):
        ok, data = self._protocol.misc(cmd, args, update_log, decode_values)
        if ok:
            return data

    @property
    def size(self):
        return self._protocol.size()

    @property
    def error(self):
        error_str = self._protocol.misc_error()
        if error_str is not None:
            code, msg = error_str.split(': ', 1)
            return int(code), msg

    def copy(self, path):
        return self._protocol.copy(path)

    def _datetime_to_timestamp(self, dt):
        timestamp = time.mktime(dt.timetuple())
        timestamp *= 1000000
        return int(timestamp + dt.microsecond)

    def restore(self, path, timestamp, options=0):
        if isinstance(timestamp, datetime.datetime):
            timestamp = self._datetime_to_timestamp(timestamp)
        return self._protocol.restore(path, timestamp, options)

    def set_master(self, host, port, timestamp, options=0):
        if isinstance(timestamp, datetime.datetime):
            timestamp = self._datetime_to_timestamp(timestamp)
        return self._protocol.setmst(host, port, timestamp, options)

    def clear_cache(self):
        return self._protocol.misc_cacheclear()

    def defragment(self, nsteps=None):
        return self._protocol.misc_defragment(nsteps)

    def get_range(self, start, stop=None, max_keys=0, decode_values=True):
        return self._protocol.misc_range(start, stop, max_keys, decode_values)

    def get_rangelist(self, start, stop=None, max_keys=0, decode_values=True):
        return self._protocol.misc_rangelist(start, stop, max_keys,
                                             decode_values)

    def match_prefix(self, prefix, max_keys=None):
        return self._protocol.fwmkeys(prefix, max_keys)

    def match_regex(self, regex, max_keys=None, decode_values=True):
        return self._protocol.misc_regex(regex, max_keys, decode_values)

    def match_regexlist(self, regex, max_keys=None, decode_values=True):
        return self._protocol.misc_regexlist(regex, max_keys, decode_values)

    def iter_from(self, start_key):
        return self._protocol.items(start_key)

    def keys(self):
        return self._protocol.keys()

    def keys_fast(self):
        return self._protocol.fwmkeys('')

    def items(self, start_key=None):
        return self._protocol.items(start_key)

    def items_fast(self):
        return self._protocol.misc_rangelist('')

    def set_index(self, name, index_type, check_exists=False):
        if check_exists:
            index_type |= IOP_KEEP
        return self._protocol.misc_setindex(name, index_type)

    def optimize_index(self, name):
        return self._protocol.misc_setindex(name, IOP_OPTIMIZE)

    def delete_index(self, name):
        return self._protocol.misc_setindex(name, IOP_DELETE)

    def search(self, expressions, cmd=None):
        conditions = [_pack_misc_cmd(*expr) for expr in expressions]
        return self._protocol.misc_search(conditions, cmd)

    def genuid(self):
        return self._protocol.misc_genuid()

    def __iter__(self):
        return iter(self._protocol.keys())


def _pack_misc_cmd(*args):
    message = [encode(str(arg) if not isinstance(arg, basestring) else arg)
               for arg in args]
    return b'\x00'.join(message)


def clone_query(method):
    def inner(self, *args, **kwargs):
        clone = self.clone()
        method(clone, *args, **kwargs)
        return clone
    return inner


class QueryBuilder(object):
    def __init__(self):
        self._conditions = []
        self._order_by = []
        self._limit = None
        self._offset = None

    def clone(self):
        obj = QueryBuilder()
        obj._conditions = list(self._conditions)
        obj._order_by = list(self._order_by)
        obj._limit = self._limit
        obj._offset = self._offset
        return obj

    @clone_query
    def filter(self, column, op, value):
        self._conditions.append((column, op, value))

    @clone_query
    def order_by(self, column, ordering=None):
        self._order_by.append((column, ordering or ORDER_STR_ASC))

    @clone_query
    def limit(self, limit=None):
        self._limit = limit

    @clone_query
    def offset(self, offset=None):
        self._offset = offset

    def build_search(self):
        cmd = [('addcond', col, op, val) for col, op, val in self._conditions]
        for col, order in self._order_by:
            cmd.append(('setorder', col, order))
        if self._limit is not None or self._offset is not None:
            cmd.append(('setlimit', self._limit or 1 << 31, self._offset or 0))
        return cmd

    def execute(self, client):
        return client.search(self.build_search())

    def delete(self, client):
        return client.search(self.build_search(), 'out')

    def get(self, client):
        results = client.search(self.build_search(), 'get')
        accum = []
        for key, raw_data in results:
            accum.append((key, table_to_dict(raw_data)))
        return accum

    def count(self, client):
        return client.search(self.build_search(), 'count')
