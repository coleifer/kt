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

from ._binary import BinaryProtocol
from ._binary import TokyoTyrantProtocol
from ._binary import decode
from ._binary import encode
from .exceptions import ImproperlyConfigured
from .exceptions import KyotoTycoonError
from .exceptions import ProtocolError
from .exceptions import ServerError
from .http import HttpProtocol


KT_BINARY = 'binary'
KT_JSON = 'json'
KT_MSGPACK = 'msgpack'
KT_PICKLE = 'pickle'
KT_SERIALIZERS = set((KT_BINARY, KT_JSON, KT_MSGPACK, KT_PICKLE))


class KyotoTycoon(object):
    def __init__(self, host='127.0.0.1', port=1978, default_db=0,
                 serializer=KT_BINARY, decode_keys=True, auto_connect=True,
                 timeout=None):
        self._host = host
        self._port = port
        self._default_db = default_db
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

        # Protocol handlers.
        self._protocol_binary = BinaryProtocol(self)
        self._protocol_http = HttpProtocol(self)

        if self._auto_connect:
            self.open()

    def open(self):
        if self._connected:
            return False

        self._protocol_binary.open()
        self._protocol_http.open()
        self._connected = True
        return True

    def close(self):
        if not self._connected:
            return False

        self._protocol_binary.close()
        self._protocol_http.close()
        self._connected = False
        return True

    def get(self, key, db=None):
        return self._protocol_binary.get(key, db or self._default_db)

    def set(self, key, value, db=None, expire_time=None):
        return self._protocol_binary.set(key, value, db or self._default_db,
                                         expire_time)

    def remove(self, key, db=None):
        return self._protocol_binary.remove(key, db or self._default_db)

    def get_bulk(self, keys, db=None):
        return self._protocol_binary.get_bulk(keys, db or self._default_db)

    def set_bulk(self, __data=None, **kwargs):
        db = kwargs.pop('db', None)
        expire_time = kwargs.pop('expire_time', None)
        if __data is not None:
            kwargs.update(__data)
        return self._protocol_binary.set_bulk(kwargs, db or self._default_db,
                                              expire_time)

    def remove_bulk(self, keys, db=None):
        return self._protocol_binary.remove_bulk(keys, db or self._default_db)

    def play_script(self, name, __data=None, **params):
        if __data is not None:
            params.update(__data)
        return self._protocol_binary.play_script(name, params)

    def status(self, db=None):
        return self._protocol_http.status(db or self._default_db)

    def report(self):
        return self._protocol_http.report()

    def clear(self, db=None):
        return self._protocol_http.clear(db or self._default_db)

    def add(self, key, value, db=None, expire_time=None):
        return self._protocol_http.add(key, value, db or self._default_db,
                                       expire_time)

    def replace(self, key, value, db=None, expire_time=None):
        return self._protocol_http.replace(key, value, db or self._default_db,
                                           expire_time)

    def append(self, key, value, db=None, expire_time=None):
        return self._protocol_http.append(key, value, db or self._default_db,
                                          expire_time)

    def check(self, key, db=None):
        return self._protocol_http.check(key, db or self._default_db)

    def seize(self, key, db=None):
        return self._protocol_http.seize(key, db or self._default_db)

    def cas(self, key, old_val, new_val, db=None, expire_time=None):
        return self._protocol_http.cas(key, old_val, new_val,
                                       db or self._default_db, expire_time)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

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

    def __contains__(self, key):
        return self.check(*self._kdb_from_key(key))

    def __len__(self):
        return int(self.status(self._default_db)['count'])

    @property
    def size(self):
        return int(self.status(self._default_db)['size'])

    @property
    def path(self):
        return decode(self.status(self._default_db)['path'])

    pop = seize
    update = set_bulk

    def set_database(self, db):
        self._default_db = db
        return self


class TokyoTyrant(object):
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
            encode_value = encode
            decode_value = decode
        elif self._serializer == KT_JSON:
            encode_value = lambda v: (json
                                      .dumps(v, separators=(',', ':'))
                                      .encode('utf-8'))
            decode_value = lambda v: json.loads(v.decode('utf-8'))
        elif self._serializer == KT_MSGPACK:
            encode_value = msgpack.packb
            decode_value = msgpack.unpackb
        elif self._serializer == KT_PICKLE:
            encode_value = partial(pickle.dumps,
                                   protocol=pickle.HIGHEST_PROTOCOL)
            decode_value = pickle.loads
        else:
            raise ImproperlyConfigured('unrecognized serializer "%s" - use one'
                                       ' of: %s' % (self._serializer,
                                                    ','.join(KT_SERIALIZERS)))

        # Session and socket used for rpc and binary protocols, respectively.
        self._connected = False

        # Protocol handlers.
        self._protocol = TokyoTyrantProtocol(
            host=self._host,
            port=self._port,
            decode_keys=self._decode_keys,
            encode_value=encode_value,
            decode_value=decode_value,
            timeout=self._timeout)

        if self._auto_connect:
            self.open()

    def open(self):
        if self._connected:
            return False

        self._protocol.open()
        self._connected = True
        return True

    def close(self):
        if not self._connected:
            return False

        self._protocol.close()
        self._connected = False
        return True

    def get(self, key):
        return self._protocol.get(key)

    def set(self, key, value):
        return self._protocol.put(key, value)

    def add(self, key, value):
        return self._protocol.putkeep(key, value)

    def append(self, key, value):
        return self._protocol.putcat(key, value)

    def remove(self, key):
        return self._protocol.out(key)

    def incr(self, key, n=1):
        return self._protocol.addint(key, n)

    def get_bulk(self, keys):
        return self._protocol.mget(keys)

    def set_bulk(self, __data=None, **kwargs):
        if __data is not None:
            kwargs.update(__data)
        return self._protocol.misc('putlist', data=kwargs)

    def remove_bulk(self, keys):
        return self._protocol.misc('outlist', keys=keys)

    def misc(self, cmd, keys=None, data=None):
        return self._protocol.misc(cmd, keys, data)

    def check(self, key):
        return self._protocol.vsiz(key)

    __getitem__ = get
    __setitem__ = set
    __delitem__ = remove
    update = set_bulk

    def __contains__(self, key):
        return self._protocol.vsiz(key) is not None

    def __len__(self):
        return self._protocol.rnum()

    def clear(self):
        return self._protocol.vanish()

    @property
    def size(self):
        return self._protocol.size()

    def status(self):
        return self._protocol.stat()

    def play_script(self, name, key=None, value=None):
        return self._protocol.ext(name, 0, key, value)
