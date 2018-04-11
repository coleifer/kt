from functools import partial
import json
import socket
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
from ._binary import encode
from .exceptions import ImproperlyConfigured
from .exceptions import KyotoTycoonError
from .exceptions import ProtocolError
from .exceptions import ServerConnectionError
from .exceptions import ServerError
from .http import HttpProtocol


KT_BINARY = 'binary'
KT_JSON = 'json'
KT_MSGPACK = 'msgpack'
KT_NONE = 'none'
KT_PICKLE = 'pickle'
KT_SERIALIZERS = set((KT_BINARY, KT_JSON, KT_MSGPACK, KT_NONE, KT_PICKLE))


class BaseClient(object):
    def __init__(self, host='127.0.0.1', port=1978, serializer=KT_BINARY,
                 decode_keys=True, auto_connect=True, timeout=None):
        self._host = host
        self._port = port
        self._serializer = serializer
        self._decode_keys = decode_keys
        self._auto_connect = auto_connect
        self._timeout = timeout

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
            self._encode_value = msgpack.packb
            self._decode_value = msgpack.unpackb
        elif self._serializer == KT_NONE:
            self._encode_value = lambda x: x
            self._decode_value = lambda x: x
        elif self._serializer == KT_PICKLE:
            self._encode_value = partial(pickle.dumps,
                                         protocol=pickle.HIGHEST_PROTOCOL)
            self._decode_value = pickle.loads
        else:
            raise ImproperlyConfigured('unrecognized serializer "%s" - use one'
                                       ' of: %s' % (self._serializer,
                                                    ','.join(KT_SERIALIZERS)))

        # Session and socket used for rpc and binary protocols, respectively.
        self._connected = False
        self._initialize_protocols()

        if self._auto_connect:
            self.open()

    def open(self):
        if self._connected:
            return False

        self._open_protocols()
        self._connected = True
        return True

    def close(self):
        if not self._connected:
            return False

        self._close_protocols()
        self._connected = False
        return True

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


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
            timeout=self._timeout)
        self._protocol_http = HttpProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=self._encode_value,
            decode_value=self._decode_value)

    def _open_protocols(self):
        self._protocol.open()
        self._protocol_http.open()

    def _close_protocols(self):
        self._protocol.close()
        self._protocol_http.close()

    def get(self, key, db=None):
        db = self._default_db if db is None else db
        return self._protocol.get(key, db)

    def set(self, key, value, db=None, expire_time=None):
        db = self._default_db if db is None else db
        return self._protocol.set(key, value, db, expire_time)

    def remove(self, key, db=None):
        db = self._default_db if db is None else db
        return self._protocol.remove(key, db)

    def get_bulk(self, keys, db=None):
        db = self._default_db if db is None else db
        return self._protocol.get_bulk(keys, db)

    def set_bulk(self, __data=None, **kwargs):
        db = kwargs.pop('db', self._default_db)
        expire_time = kwargs.pop('expire_time', None)
        if __data is not None:
            kwargs.update(__data)
        return self._protocol.set_bulk(kwargs, db, expire_time)

    def remove_bulk(self, keys, db=None):
        db = self._default_db if db is None else db
        return self._protocol.remove_bulk(keys, db)

    def script(self, name, __data=None, **params):
        if __data is not None:
            params.update(__data)
        return self._protocol.script(name, params)

    def clear(self, db=None):
        db = self._default_db if db is None else db
        return self._protocol_http.clear(db)

    def status(self, db=None):
        db = self._default_db if db is None else db
        return self._protocol_http.status(db)

    def report(self):
        return self._protocol_http.report()

    def add(self, key, value, db=None, expire_time=None):
        db = self._default_db if db is None else db
        return self._protocol_http.add(key, value, db, expire_time)

    def replace(self, key, value, db=None, expire_time=None):
        db = self._default_db if db is None else db
        return self._protocol_http.replace(key, value, db, expire_time)

    def append(self, key, value, db=None, expire_time=None):
        db = self._default_db if db is None else db
        return self._protocol_http.append(key, value, db, expire_time)

    def exists(self, key, db=None):
        db = self._default_db if db is None else db
        return self._protocol_http.check(key, db)

    def seize(self, key, db=None):
        db = self._default_db if db is None else db
        return self._protocol_http.seize(key, db)

    def cas(self, key, old_val, new_val, db=None, expire_time=None):
        db = self._default_db if db is None else db
        return self._protocol_http.cas(key, old_val, new_val, db, expire_time)

    def _kdb_from_key(self, key):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError('expected key-tuple of (key, db)')
            return key
        return key, self._default_db

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
        self.set(key, value, db, expire_time)

    def __delitem__(self, key):
        self.remove(*self._kdb_from_key(key))

    pop = seize
    update = set_bulk

    def __contains__(self, key):
        return self.exists(*self._kdb_from_key(key))

    def __len__(self):
        return int(self.status(self._default_db)['count'])

    @property
    def size(self):
        return int(self.status(self._default_db)['size'])

    @property
    def path(self):
        return decode(self.status(self._default_db)['path'])

    def set_database(self, db):
        self._default_db = db
        return self


class TokyoTyrant(BaseClient):
    def _initialize_protocols(self):
        self._protocol = TTBinaryProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=self._encode_value,
            decode_value=self._decode_value,
            timeout=self._timeout)

    def _open_protocols(self):
        return self._protocol.open()

    def _close_protocols(self):
        return self._protocol.close()

    def get(self, key):
        return self._protocol.get(key)

    def set(self, key, value):
        return self._protocol.put(key, value)

    def remove(self, key):
        return self._protocol.out(key)

    def get_bulk(self, keys):
        return self._protocol.get_bulk(keys)

    def set_bulk(self, __data=None, **kwargs):
        if __data is not None:
            kwargs.update(__data)
        return self._protocol.misc('putlist', data=kwargs)

    def remove_bulk(self, keys):
        return self._protocol.misc('outlist', keys=keys)

    def script(self, name, key=None, value=None):
        return self._protocol.script(name, key, value)

    def clear(self):
        return self._protocol.vanish()

    def status(self):
        return self._protocol.stat()

    def add(self, key, value):
        return self._protocol.putkeep(key, value)

    def append(self, key, value):
        return self._protocol.putcat(key, value)

    def get_part(self, key, start=None, end=None):
        params = [key]
        if start is not None or end is not None:
            params.append(str(start or 0))
        if end is not None:
            params.append(str(end))
        return self._protocol.misc('getpart', params)

    def exists(self, key):
        return self._protocol.vsiz(key)

    def incr(self, key, n=1):
        return self._protocol.addint(key, n)

    def incr_double(self, key, n=1.):
        return self._protocol.adddouble(key, n)

    def misc(self, cmd, keys=None, data=None):
        return self._protocol.misc(cmd, keys, data)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_range(item.start, item.stop or None)
        else:
            return self.get(item)

    __setitem__ = set
    __delitem__ = remove
    update = set_bulk

    def __contains__(self, key):
        return self._protocol.vsiz(key) is not None

    def __len__(self):
        return self._protocol.rnum()

    @property
    def size(self):
        return self._protocol.size()

    @property
    def error(self):
        return self._protocol.misc('error', [])

    def optimize(self):
        return self._protocol.misc('optimize', [])

    def clear_cache(self):
        return self._protocol.misc('cacheclear', [])

    def get_range(self, start, stop=None, max_keys=0):
        args = [start, str(max_keys)]
        if stop is not None:
            args.append(stop)
        rv = self._protocol.misc('range', args)
        return {} if rv is True else rv

    def match_prefix(self, prefix, max_keys=1024):
        return self._protocol.match_prefix(prefix, max_keys)

    def match_regex(self, regex, max_keys=1024):
        rv = self._protocol.misc('regex', [regex, str(max_keys)])
        return {} if rv is True else rv

    def iter_from(self, start_key):
        self._protocol.misc('iterinit', [item.start])
        accum = {}
        while True:
            kv = self._protocol.misc('iternext', [])
            if kv:
                accum.update(kv)
            else:
                break
        return accum

    def keys(self):
        return self._protocol.keys()

    def __iter__(self):
        return iter(self._protocol.keys())
