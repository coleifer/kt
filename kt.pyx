from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION

import io
import msgpack
import socket
import struct
import time

s_pack = struct.pack
s_unpack = struct.unpack


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


class KyotoTycoonError(Exception): pass


cdef class KyotoTycoon(object):
    cdef:
        readonly bytes host
        readonly int port
        readonly timeout
        readonly bint _raw
        _socket

    def __init__(self, host='127.0.0.1', port=1978, timeout=None, raw=False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._raw = raw
        self._socket = None

    def open(self):
        if self._socket is not None:
            return False

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((self.host, self.port))
        if self.timeout:
            conn.settimeout(self.timeout)
        self._socket = conn.makefile()
        return True

    def close(self):
        if self._socket is None:
            return False

        self._socket.close()
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
            if self._raw:
                bvalue = encode(data[key])
            else:
                bvalue = msgpack.packb(data[key])
            buf.write(s_pack('!HIIq', db, len(bkey), len(bvalue), expire_time))
            buf.write(bkey)
            buf.write(bvalue)

        return <bytes>buf.getvalue()

    cdef int _check_response(self, action) except -1:
        cdef int magic
        magic, = s_unpack('!B', self._socket.read(1))
        if magic == action:
            return 0
        elif magic == KT_ERROR:
            raise KyotoTycoonError('Internal server error processing request.')
        else:
            raise KyotoTycoonError('Unexpected server response: %x' % magic)

    def get(self, keys, db=0):
        cdef:
            bytes request

        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        request = self._make_request_keys(keys, KT_GET_BULK, 0, db)
        self._socket.write(request)
        self._socket.flush()
        self._check_response(KT_GET_BULK)

        cdef:
            int nkeys, nkey, nval
            dict result = {}

        read = self._socket.read
        nkeys, = s_unpack('!I', read(4))
        for _ in range(nkeys):
            _, nkey, nval, _ = s_unpack('!HIIq', read(18))
            key = read(nkey)
            value = read(nval)
            if self._raw:
                result[key] = value
            else:
                result[key] = msgpack.unpackb(value)

        return result

    def set(self, dict data, db=0, async=False, expire_time=None):
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

    def remove(self, keys, db=0, async=False):
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
