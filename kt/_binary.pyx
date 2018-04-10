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


cdef class TokyoTyrantProtocol(object):
    cdef:
        readonly str _host
        readonly int _port
        readonly bint _decode_keys
        readonly _encode_value
        readonly _decode_value
        readonly _timeout
        _socket

    def __init__(self, host='127.0.0.1', port=1978, decode_keys=True,
                 encode_value=None, decode_value=None, timeout=None):
        self._host = host
        self._port = port
        self._decode_keys = decode_keys
        self._encode_value = encode_value or encode
        self._decode_value = decode_value or decode
        self._timeout = timeout
        self._socket = None

    def __del__(self):
        if self._socket is not None:
            self._socket.close()

    def open(self):
        if self._socket is not None:
            return False

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((self._host, self._port))
        if self._timeout:
            conn.settimeout(self._timeout)
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

    cdef bytes _make_request_key(self, key, magic):
        cdef:
            bytes bkey

        buf = io.BytesIO()
        buf.write(magic)

        bkey = _encode(key)
        buf.write(s_pack('!I', len(bkey)))
        buf.write(bkey)
        return <bytes>buf.getvalue()

    cdef bytes _make_request_key_value(self, key, value, magic):
        cdef:
            bytes bkey, bvalue

        buf = io.BytesIO()
        buf.write(magic)

        bkey = _encode(key)
        bvalue = self._encode_value(value)
        buf.write(s_pack('!II', len(bkey), len(bvalue)))
        buf.write(bkey)
        buf.write(bvalue)
        return <bytes>buf.getvalue()

    cdef bytes _make_request_keys(self, keys, magic):
        cdef:
            bytes bkey

        buf = io.BytesIO()
        buf.write(magic)
        buf.write(s_pack('!I', len(keys)))
        for key in keys:
            bkey = _encode(key)
            buf.write(s_pack('!I', len(bkey)))
            buf.write(bkey)

        return <bytes>buf.getvalue()

    cdef int _check_response(self) except -1:
        cdef:
            bytes bmagic
            int magic

        bmagic = self._socket.read(1)
        if not bmagic:
            self.close()
            raise ServerError('Server went away')

        magic, = s_unpack('!B', bmagic)
        if magic == 0:
            return True
        elif magic == 1:
            return False
        else:
            raise ServerError('server error: %x' % magic)

    cdef _key_value_cmd(self, key, value, bytes bmagic):
        cdef bytes request
        request = self._make_request_key_value(key, value, bmagic)
        self._socket.write(request)
        self._socket.flush()
        return self._check_response()

    def put(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x10')

    def putkeep(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x11')

    def putcat(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x12')

    def out(self, key):
        cdef bytes request
        request = self._make_request_key(key, b'\xc8\x20')
        self._socket.write(request)
        self._socket.flush()
        return self._check_response()

    def get(self, key):
        cdef:
            bytes request, bval
            int nval

        request = self._make_request_key(key, b'\xc8\x30')
        self._socket.write(request)
        self._socket.flush()
        if self._check_response():
            vsiz, = s_unpack('!I', self._socket.read(4))
            bval = self._socket.read(vsiz)
            return self._decode_value(bval)

    def mget(self, keys):
        cdef:
            bytes request, bkey, bval
            int nitems, nkey, nval

        request = self._make_request_keys(keys, b'\xc8\x31')
        self._socket.write(request)
        self._socket.flush()
        if not self._check_response():
            return

        nitems, = s_unpack('!I', self._socket.read(4))
        accum = {}

        read = self._socket.read
        for _ in range(nitems):
            nkey, nval = s_unpack('!II', read(8))
            bkey = read(nkey)
            bvalue = read(nval)
            key = _decode(bkey) if self._decode_keys else bkey
            accum[key] = self._decode_value(bvalue)

        return accum

    def vsiz(self, key):
        cdef:
            bytes request
            int nval

        request = self._make_request_key(key, b'\xc8\x38')
        self._socket.write(request)
        self._socket.flush()
        if not self._check_response():
            return None

        nval, = s_unpack('!I', self._socket.read(4))
        return nval

    def addint(self, key, value):
        cdef:
            bytes bkey = _encode(key)
            int nval

        buf = io.BytesIO()
        buf.write(b'\xc8\x60')
        buf.write(s_pack('!II', len(bkey), value))
        buf.write(bkey)
        self._socket.write(buf.getvalue())
        self._socket.flush()

        if self._check_response():
            nval, = s_unpack('!I', self._socket.read(4))
            return nval

    def ext(self, name, int options, key=None, value=None):
        cdef:
            bytes bname = _encode(name)
            bytes bkey = _encode(key or '')
            bytes bval = self._encode_value(value or '')

        buf = io.BytesIO()
        buf.write(b'\xc8\x68')
        buf.write(s_pack('!IIII', len(bname), options, len(bkey), len(bval)))
        buf.write(bname)
        buf.write(bkey)
        buf.write(bval)
        self._socket.write(buf.getvalue())
        self._socket.flush()

        if not self._check_response():
            return

        cdef int resplen
        resplen, = s_unpack('!I', self._socket.read(4))
        return self._socket.read(resplen)

    def misc(self, name, keys=None, data=None):
        # TokyoTyrant supports "fluent" commands - kinda like Redis, you pass
        # a command name and the requested parameters, get appropriate resp.
        if keys is not None and data is not None:
            raise ValueError('misc() requires only one of "keys" or "data" be '
                             'specified.')
        cmds = set(('put', 'out', 'get', 'putlist', 'outlist', 'getlist'))
        if name not in cmds:
            raise ValueError('unsupported command. use one of %s' %
                             ', '.join(sorted(cmds)))

        if keys is not None and not isinstance(keys, (list, tuple)):
            keys = (keys,)

        if name == 'put' and len(data) > 1:
            name = 'putlist'
        elif name == 'get' and len(keys) > 1:
            name = 'getlist'
        elif name == 'out' and len(keys) > 1:
            name = 'outlist'

        cdef:
            bint is_put = False
            bytes bkey, bval
            bytes bname = _encode(name)
            int nargs

        # Number of parameters we will be providing.
        if name.startswith('put'):
            nargs = len(data) * 2
        else:
            nargs = len(keys)

        buf = io.BytesIO()
        bw = buf.write
        bw(b'\xc8\x90')
        bw(s_pack('!III', len(bname), 0, nargs))
        bw(bname)

        if bname.startswith(b'put'):
            for key, value in data.items():
                bkey = _encode(key)
                bval = self._encode_value(value)
                bw(s_pack('!I', len(bkey)))
                bw(bkey)
                bw(s_pack('!I', len(bval)))
                bw(bval)
        else:
            for key in keys:
                bkey = _encode(key)
                bw(s_pack('!I', len(bkey)))
                bw(bkey)

        self._socket.write(buf.getvalue())
        self._socket.flush()
        if not self._check_response():
            return

        read = self._socket.read
        nelem, = s_unpack('!I', read(4))

        if nelem == 0 and bname != b'getlist':
            return True
        elif nelem == 1:
            nval, = s_unpack('!I', read(4))
            if nval > 0:
                bval = self._socket.read(nval)
                return self._decode_value(bval)
        else:
            accum = {}
            for _ in range(nelem // 2):
                klen, = s_unpack('!I', read(4))
                bkey = read(klen)
                vlen, = s_unpack('!I', read(4))
                bval = read(vlen)
                key = _decode(bkey) if self._decode_keys else bkey
                accum[key] = self._decode_value(bval)
            return accum

    def vanish(self):
        self._socket.write(b'\xc8\x72')
        self._socket.flush()
        return self._check_response()

    def _long_cmd(self, bytes bmagic):
        cdef long n
        self._socket.write(bmagic)
        self._socket.flush()
        self._check_response()
        n, = s_unpack('!q', self._socket.read(8))
        return n

    def rnum(self):
        return self._long_cmd(b'\xc8\x80')

    def size(self):
        return self._long_cmd(b'\xc8\x81')

    def stat(self):
        self._socket.write(b'\xc8\x88')
        self._socket.flush()
        self._check_response()
        n, = s_unpack('!I', self._socket.read(4))
        return self._socket.read(n)
