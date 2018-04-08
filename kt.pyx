from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION

import io
import pickle
import socket
import struct
import time

try:
    import msgpack
    m_pack = msgpack.packb
    m_unpack = msgpack.unpackb
except ImportError:
    msgpack = None
    m_pack = m_unpack = None

s_pack = struct.pack
s_unpack = struct.unpack

p_dumps = pickle.dumps
p_loads = pickle.loads


DEF KT_SET_BULK = 0xb8
DEF KT_GET_BULK = 0xba
DEF KT_REMOVE_BULK = 0xb9
DEF KT_PLAY_SCRIPT = 0xb4
DEF KT_ERROR = 0xbf
DEF KT_NOREPLY = 0x01
DEF EXPIRE = 0x7fffffffffffffff


cdef bint IS_PY3K = PY_MAJOR_VERSION == 3

cdef inline bytes encode(obj):
    cdef bytes result
    if PyUnicode_Check(obj):
        result = PyUnicode_AsUTF8String(obj)
    elif PyBytes_Check(obj):
        result = <bytes>obj
    elif obj is None:
        return None
    elif IS_PY3K:
        result = PyUnicode_AsUTF8String(str(obj))
    else:
        result = bytes(obj)
    return result

cdef inline unicode decode(obj):
    cdef unicode result
    if PyBytes_Check(obj):
        result = obj.decode('utf-8')
    elif PyUnicode_Check(obj):
        result = <unicode>obj
    elif obj is None:
        return None
    else:
        result = str(obj)
    return result


class KyotoTycoonError(Exception): pass
class ConfigurationError(KyotoTycoonError): pass


cdef class Database(object)  # Forward declaration.


cdef class KyotoTycoon(object):
    cdef:
        readonly bytes host
        readonly int port
        readonly timeout
        readonly bint _decode_keys
        readonly bint _msgpack_values
        readonly bint _pickle_values
        _socket

    def __init__(self, host='127.0.0.1', port=1978, timeout=None,
                 pickle_values=False, msgpack_values=False, decode_keys=True,
                 auto_connect=True):
        self.host = encode(host)
        self.port = port
        self.timeout = timeout
        self._decode_keys = decode_keys
        self._msgpack_values = msgpack_values
        if msgpack_values and msgpack is None:
            raise ConfigurationError('msgpack library not installed')
        self._pickle_values = pickle_values
        self._socket = None
        if auto_connect:
            self.open()

    def __del__(self):
        if self._socket is not None:
            self._socket.close()

    def __enter__(self):
        self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self._socket is not None:
            return False

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((self.host, self.port))
        if self.timeout:
            conn.settimeout(self.timeout)
        self._socket = conn.makefile('rwb')
        return True

    def close(self):
        if self._socket is None:
            return False

        try:
            self._socket.close()
        except OSError:
            pass

        self._socket = None
        return True

    cpdef bint is_connected(self):
        return True if self._socket is not None else False

    cdef bytes _make_request_keys(self, keys, action, flags, db):
        cdef:
            bytes bkey

        buf = io.BytesIO()
        buf.write(s_pack('!BII', action, flags, len(keys)))
        for key in keys:
            bkey = encode(key)
            buf.write(s_pack('!HI', db, len(bkey)))
            buf.write(bkey)

        return <bytes>buf.getvalue()

    cdef bytes _make_request_keys_values(self, data, action, flags, db,
                                         expire_time):
        cdef:
            bytes bkey, bvalue

        buf = io.BytesIO()
        buf.write(s_pack('!BII', action, flags, len(data)))
        for key in data:
            bkey = encode(key)
            if self._pickle_values:
                bvalue = p_dumps(data[key], pickle.HIGHEST_PROTOCOL)
            elif self._msgpack_values:
                bvalue = m_pack(data[key])
            else:
                bvalue = encode(data[key])
            buf.write(s_pack('!HIIq', db, len(bkey), len(bvalue), expire_time))
            buf.write(bkey)
            buf.write(bvalue)

        return <bytes>buf.getvalue()

    cdef int _check_response(self, action) except -1:
        cdef:
            bytes bmagic
            int magic

        bmagic = self._socket.read(1)
        if not bmagic:
            self.close()
            raise KyotoTycoonError('Server went away')

        magic, = s_unpack('!B', bmagic)
        if magic == action:
            return 0
        elif magic == KT_ERROR:
            raise KyotoTycoonError('Internal server error processing request.')
        else:
            raise KyotoTycoonError('Unexpected server response: %x' % magic)

    cdef _get(self, keys, int db):
        cdef:
            bytes request

        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        request = self._make_request_keys(keys, KT_GET_BULK, 0, db)
        self._socket.write(request)
        self._socket.flush()
        self._check_response(KT_GET_BULK)

        cdef:
            bytes bkey, bvalue
            int nkeys, nkey, nval
            dict result = {}

        read = self._socket.read
        nkeys, = s_unpack('!I', read(4))
        for _ in range(nkeys):
            _, nkey, nval, _ = s_unpack('!HIIq', read(18))
            bkey = read(nkey)
            bvalue = read(nval)
            key = decode(bkey) if self._decode_keys else bkey
            if self._pickle_values:
                result[key] = p_loads(bvalue)
            elif self._msgpack_values:
                result[key] = m_unpack(bvalue)
            else:
                result[key] = bvalue

        return result

    def get(self, key, db=0):
        """
        Get the value associated with a single key.

        :param bytes key: key to look-up
        :param int db: database index
        :return: value associated with key or ``None`` if missing.
        """
        cdef:
            bytes bkey = encode(key)
        response = self._get((bkey,), db)
        return response.get(decode(bkey) if self._decode_keys else bkey)

    def mget(self, keys, db=0):
        """
        Get one or more key/value pairs from the given database.

        :param list keys: keys to look-up
        :param int db: database index
        :return: dictionary containing key/value pairs that were found in db.
        """
        return self._get(keys, db)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError('Expected tuple of size 2 (key, dbnum)')
            key, db = key
        else:
            db = 0
        return self.get(key, db)

    cdef _set(self, dict data, int db, bint async, expire_time):
        cdef:
            bytes request
            int flags

        expire_time = expire_time or EXPIRE
        flags = KT_NOREPLY if async else 0
        request = self._make_request_keys_values(data, KT_SET_BULK, flags, db,
                                                 expire_time)
        self._socket.write(request)
        self._socket.flush()
        if async:
            return

        self._check_response(KT_SET_BULK)
        return s_unpack('!I', self._socket.read(4))[0]

    def set(self, key, value, db=0, async=False, expire_time=None):
        """
        Set the value for the given key.

        :param bytes key: key to set
        :param value: value to store
        :param int db: database index
        :param bool async: return immediately without db confirmation.
        :param int expire_time: expire time (in number of seconds)
        :return: 1 if set successfully and async=False.
        """
        return self._set({key: value}, db, async, expire_time)

    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError('Expected tuple of size 2 (key, dbnum)')
            key, db = key
        else:
            db = 0

        if isinstance(value, tuple):
            if len(value) != 2:
                raise ValueError('Expected tuple of size 2 (val, expire)')
            value, expire = value
        else:
            expire = None

        self._set({key: value}, db, False, expire)

    def mset(self, __data=None, **kwargs):
        """
        Set multiple key/value pairs in one operation.

        :param dict __data: a dictionary of key/value pairs
        :param kwargs: key/value pairs as keyword arguments
        :param int db: database index
        :param bool async: return immediately without db confirmation.
        :param int expire_time: expire time (in number of seconds)
        :return: number of keys set if async=False.
        """
        db = kwargs.pop('db', 0)
        async = kwargs.pop('async', False)
        expire_time = kwargs.pop('expire_time', None)
        if __data:
            kwargs.update(__data)
        return self._set(kwargs, db, async, expire_time)

    update = mset  # Alias mset -> update for dict compatibility.

    cdef _remove(self, keys, int db, bint async):
        cdef:
            bytes request
            int flags

        flags = KT_NOREPLY if async else 0
        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        request = self._make_request_keys(keys, KT_REMOVE_BULK, flags, db)
        self._socket.write(request)
        self._socket.flush()
        if async:
            return

        self._check_response(KT_REMOVE_BULK)
        return s_unpack('!I', self._socket.read(4))[0]

    def remove(self, key, db=0, async=False):
        """
        Remove the given key from the database.

        :param bytes key: key to remove.
        :param int db: database index.
        :param bool async: return immediately without db confirmation.
        :return: 1 if key was removed and async=False.
        """
        return self._remove((key,), db, async)

    def mremove(self, keys, db=0, async=False):
        """
        Remove multiple keys from the database in one operation.

        :param list keys: keys to remove.
        :param int db: database index.
        :param bool async: return immediately without db confirmation.
        :return: Number of keys removed if async=False.
        """
        return self._remove(keys, db, async)

    def __delitem__(self, key):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError('Expected tuple of size 2 (key, dbnum)')
            key, db = key
        else:
            db = 0
        self.remove(key, db)

    def run_script(self, name, data=None):
        """
        Execute a lua script.

        :param bytes name: name of the lua script.
        :param dict data: arbitrary key/value data to send to script.
        :return: A dictionary of key/value pairs returned by script.
        """
        cdef:
            bytes bkey, bvalue
            bytes bname = encode(name)
            dict result
            int klen, vlen, nkeys, i

        data = data or {}
        buf = io.BytesIO()
        buf.write(s_pack('!BIII', KT_PLAY_SCRIPT, 0, len(bname), len(data)))
        buf.write(bname)
        for key in data:
            bkey = encode(key)
            bvalue = encode(data[key])
            buf.write(s_pack('!II', len(bkey), len(bvalue)))
            buf.write(bkey)
            buf.write(bvalue)

        self._socket.write(buf.getvalue())
        self._socket.flush()
        self._check_response(KT_PLAY_SCRIPT)

        read = self._socket.read
        nkeys, = struct.unpack('!I', read(4))
        result = {}
        for i in range(nkeys):
            klen, vlen = s_unpack('!II', read(8))
            bkey = read(klen)
            bvalue = read(vlen)
            if self._decode_keys:
                result[decode(bkey)] = bvalue
            else:
                result[bkey] = bvalue
        return result

    def database(self, db):
        """
        Context-manager for operating on a particular database.

        :param int db: Database index.
        :return: a :py:class:`Database` object that acts as a context manager.
        """
        return Database(self, db)


cdef class Database(object):
    cdef:
        readonly KyotoTycoon kt
        public int db

    def __init__(self, kt, db=0):
        self.kt = kt
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def __getitem__(self, key):
        return self.kt[key, self.db]

    def get(self, key):
        return self.kt.get(key, self.db)

    def mget(self, keys):
        return self.kt.mget(keys, self.db)

    def __setitem__(self, key, value):
        self.kt[key, self.db] = value

    def set(self, key, value, async=False, expire_time=None):
        return self.kt.set(key, value, self.db, async, expire_time)

    def mset(self, __data=None, **kwargs):
        return self.kt.mset(__data, db=self.db, **kwargs)
    update = mset  # Alias mset -> update for dict compatibility.

    def __delitem__(self, key):
        self.kt.remove(key, self.db)

    def remove(self, key, async=False):
        return self.kt.remove(key, self.db, async)

    def mremove(self, keys, async=False):
        return self.kt.mremove(keys, self.db, async)
