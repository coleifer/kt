from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION

import io
import socket
import struct
import time

from kt.exceptions import ProtocolError
from kt.exceptions import ServerError

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

cdef inline bytes _encode(obj):
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

cdef inline unicode _decode(obj):
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

def encode(obj):
    return _encode(obj)

def decode(obj):
    return _decode(obj)


cdef class BinaryProtocol(object):
    cdef:
        public object client
        _socket

    def __init__(self, client):
        self.client = client
        self._socket = None

    def __del__(self):
        if self._socket is not None:
            self._socket.close()

    def open(self):
        if self._socket is not None:
            return False

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((self.client._host, self.client._port))
        if self.client._timeout:
            conn.settimeout(self.client._timeout)
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

    cdef bytes _make_request_keys(self, keys, action, flags, db):
        cdef:
            bytes bkey

        buf = io.BytesIO()
        buf.write(s_pack('!BII', action, flags, len(keys)))
        for key in keys:
            bkey = _encode(key)
            buf.write(s_pack('!HI', db, len(bkey)))
            buf.write(bkey)

        return <bytes>buf.getvalue()

    cdef bytes _make_request_keys_values(self, data, action, flags, db,
                                         expire_time):
        cdef:
            bytes bkey, bvalue

        buf = io.BytesIO()
        buf.write(s_pack('!BII', action, flags, len(data)))
        encode_value = self.client._encode_value

        for key, value in data.items():
            bkey = _encode(key)
            bvalue = encode_value(value)
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
            raise ServerError('Server went away')

        magic, = s_unpack('!B', bmagic)
        if magic == action:
            return 0
        elif magic == KT_ERROR:
            raise ProtocolError('Internal server error processing request.')
        else:
            raise ServerError('Unexpected server response: %x' % magic)

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

        decode_keys = self.client._decode_keys
        decode_value = self.client._decode_value
        read = self._socket.read
        nkeys, = s_unpack('!I', read(4))
        for _ in range(nkeys):
            _, nkey, nval, _ = s_unpack('!HIIq', read(18))
            bkey = read(nkey)
            bvalue = read(nval)
            key = _decode(bkey) if decode_keys else bkey
            result[key] = decode_value(bvalue)

        return result

    def get(self, key, db=0):
        cdef:
            bytes bk = _encode(key)
        response = self._get((bk,), db)
        return response.get(_decode(bk) if self.client._decode_keys else bk)

    def get_bulk(self, keys, db=0):
        return self._get(keys, db)

    cdef _set(self, dict data, int db, expire_time):
        cdef:
            bytes request

        expire_time = expire_time or EXPIRE
        request = self._make_request_keys_values(data, KT_SET_BULK, 0, db,
                                                 expire_time)
        self._socket.write(request)
        self._socket.flush()
        self._check_response(KT_SET_BULK)
        return s_unpack('!I', self._socket.read(4))[0]

    def set(self, key, value, db=0, expire_time=None):
        return self._set({key: value}, db, expire_time)

    def set_bulk(self, data, db=0, expire_time=None):
        return self._set(data, db, expire_time)

    cdef _remove(self, keys, int db):
        cdef:
            bytes request

        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        request = self._make_request_keys(keys, KT_REMOVE_BULK, 0, db)
        self._socket.write(request)
        self._socket.flush()
        self._check_response(KT_REMOVE_BULK)
        return s_unpack('!I', self._socket.read(4))[0]

    def remove(self, key, db=0):
        return self._remove((key,), db)

    def remove_bulk(self, keys, db=0):
        return self._remove(keys, db)

    def play_script(self, name, data=None):
        cdef:
            bytes bkey, bvalue
            bytes bname = _encode(name)
            dict result
            int klen, vlen, nkeys, i

        data = data or {}
        buf = io.BytesIO()
        buf.write(s_pack('!BIII', KT_PLAY_SCRIPT, 0, len(bname), len(data)))
        buf.write(bname)
        for key in data:
            bkey = _encode(key)
            bvalue = _encode(data[key])
            buf.write(s_pack('!II', len(bkey), len(bvalue)))
            buf.write(bkey)
            buf.write(bvalue)

        self._socket.write(buf.getvalue())
        self._socket.flush()
        self._check_response(KT_PLAY_SCRIPT)

        decode_keys = self.client._decode_keys
        read = self._socket.read
        nkeys, = struct.unpack('!I', read(4))
        result = {}
        for i in range(nkeys):
            klen, vlen = s_unpack('!II', read(8))
            bkey = read(klen)
            bvalue = read(vlen)
            result[_decode(bkey) if decode_keys else bkey] = bvalue
        return result
