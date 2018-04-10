from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION

import io
import socket
import struct
import time

from kt.exceptions import ProtocolError
from kt.exceptions import ServerConnectionError
from kt.exceptions import ServerError

s_pack = struct.pack
s_unpack = struct.unpack


DEF KT_SET_BULK = b'\xb8'
DEF KT_GET_BULK = b'\xba'
DEF KT_REMOVE_BULK = b'\xb9'
DEF KT_PLAY_SCRIPT = b'\xb4'
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

def noop_decode(obj):
    return obj


cdef class RequestBuffer(object):
    cdef:
        object key_encode
        object value_encode
        public object buf
        _socket

    def __init__(self, socket_file, key_encode=None, value_encode=None):
        self._socket = socket_file
        self.key_encode = key_encode
        self.value_encode = value_encode
        self.buf = io.BytesIO()

    cdef RequestBuffer write_magic(self, magic):
        self.buf.write(magic)
        return self

    cdef RequestBuffer write_int(self, int i):
        self.buf.write(s_pack('!I', i))
        return self

    cdef RequestBuffer write_ints(self, ints):
        fmt = 'I' * len(ints)
        self.buf.write(s_pack('!%s' % fmt, *ints))
        return self

    cdef RequestBuffer write_short(self, s):
        self.buf.write(s_pack('!H', s))
        return self

    cdef RequestBuffer write_long(self, l):
        self.buf.write(s_pack('!q', l))
        return self

    cdef RequestBuffer write_bytes(self, bytes data, write_length):
        if write_length:
            self.write_int(len(data))
        self.buf.write(data)
        return self

    cdef RequestBuffer write_key_list_with_db(self, keys, db):
        cdef bytes bkey
        self.write_int(len(keys))
        for key in keys:
            bkey = self.key_encode(key)
            (self
             .write_short(db)
             .write_bytes(bkey, True))
        return self

    cdef RequestBuffer write_keys_values_with_db_expire(self, data, db,
                                                        expire):
        cdef bytes bkey, bval
        self.write_int(len(data))
        for key, value in data.items():
            bkey = self.key_encode(key)
            bval = self.value_encode(value)
            (self
             .write_short(db)
             .write_ints((len(bkey), len(bval)))
             .write_long(expire)
             .write_bytes(bkey, False)
             .write_bytes(bval, False))
        return self

    cdef RequestBuffer write_key(self, key):
        return self.write_bytes(self.key_encode(key), True)

    cdef RequestBuffer write_key_list(self, keys):
        cdef bytes bkey
        self.write_int(len(keys))
        for key in keys:
            bkey = self.key_encode(key)
            self.write_bytes(bkey, True)
        return self

    cdef RequestBuffer write_keys_values(self, data):
        cdef bytes bkey, bval
        self.write_int(len(data))
        for key, value in data.items():
            self.write_key_value(key, value)
        return self

    cdef RequestBuffer write_key_value(self, key, value):
        cdef:
            bytes bkey = self.key_encode(key)
            bytes bval = self.value_encode(value)
        return (self
                .write_ints((len(bkey), len(bval)))
                .write_bytes(bkey, False)
                .write_bytes(bval, False))

    cdef send(self):
        self._socket.write(self.buf.getvalue())
        self._socket.flush()


cdef class BaseResponseHandler(object):
    cdef:
        object key_decode
        object value_decode
        public object _socket

    def __init__(self, socket_file, key_decode, value_decode):
        self._socket = socket_file
        self.key_decode = key_decode
        self.value_decode = value_decode

    cdef int read_int(self):
        return s_unpack('!I', self._socket.read(4))[0]

    cdef int read_long(self):
        return s_unpack('!q', self._socket.read(8))[0]

    cdef bytes read_bytes(self):
        return self._socket.read(self.read_int())

    cdef read_key(self):
        return self.key_decode(self.read_bytes())

    cdef read_value(self):
        return self.value_decode(self.read_bytes())

    cdef tuple read_key_value(self):
        cdef:
            int klen, vlen
        klen, vlen = s_unpack('!II', self._socket.read(8))
        return (self.key_decode(self._socket.read(klen)),
                self.value_decode(self._socket.read(vlen)))

    cdef dict read_keys_values(self):
        cdef:
            dict accum = {}
            int i, n_items

        n_items = self.read_int()
        for i in range(n_items):
            key, value = self.read_key_value()
            accum[key] = value
        return accum

    cdef tuple read_key_value_with_db_expire(self):
        cdef:
            int klen, vlen

        _, klen, vlen, _ = s_unpack('!HIIq', self._socket.read(18))
        return (self.key_decode(self._socket.read(klen)),
                self.value_decode(self._socket.read(vlen)))

    cdef dict read_keys_values_with_db_expire(self):
        cdef:
            dict accum = {}
            int i, n_items

        n_items = self.read_int()
        for i in range(n_items):
            key, value = self.read_key_value_with_db_expire()
            accum[key] = value
        return accum


cdef class KTResponseHandler(BaseResponseHandler):
    cdef int check_error(self, magic) except -1:
        cdef:
            bytes bmagic
            int imagic

        bmagic = self._socket.read(1)
        if not bmagic:
            raise ServerConnectionError('Server went away')

        if bmagic == magic:
            return 0
        elif bmagic == KT_ERROR:
            raise ProtocolError('Internal server error processing request.')
        else:
            raise ServerError('Unexpected server response: %s' % bmagic)


cdef class TTResponseHandler(BaseResponseHandler):
    cdef int check_error(self) except -1:
        cdef:
            bytes bmagic
            int imagic

        bmagic = self._socket.read(1)
        if not bmagic:
            raise ServerConnectionError('Server went away')

        imagic, = s_unpack('!B', bmagic)
        if imagic == 0 or imagic == 1:
            return imagic
        else:
            raise ServerError('Unexpected server response: %x' % imagic)


cdef class BinaryProtocol(object):
    cdef:
        readonly str _host
        readonly int _port
        readonly _timeout
        readonly decode_key
        readonly encode_value
        readonly decode_value
        _socket

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

    cdef RequestBuffer request(self):
        return RequestBuffer(
            self._socket,
            encode,
            self.encode_value)


cdef class KTBinaryProtocol(BinaryProtocol):
    cdef KTResponseHandler response(self):
        return KTResponseHandler(
            self._socket,
            self.decode_key,
            self.decode_value)

    def get_bulk(self, keys, db):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response

        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        (request
         .write_magic(KT_GET_BULK)
         .write_int(0)  # Flags.
         .write_key_list_with_db(keys, db)
         .send())

        response = self.response()
        response.check_error(KT_GET_BULK)
        return response.read_keys_values_with_db_expire()

    def get(self, key, db):
        cdef bytes bkey = encode(key)
        result = self.get_bulk((bkey,), db)
        return result.get(self.decode_key(bkey))

    def set_bulk(self, data, db, expire_time):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response

        (request
         .write_magic(KT_SET_BULK)
         .write_int(0)  # Flags.
         .write_keys_values_with_db_expire(data, db, expire_time or EXPIRE)
         .send())

        response = self.response()
        response.check_error(KT_SET_BULK)
        return response.read_int()

    def set(self, key, value, db, expire_time):
        return self.set_bulk({key: value}, db, expire_time)

    def remove_bulk(self, keys, db):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response

        if not isinstance(keys, (list, tuple, set)):
            keys = (keys,)

        (request
         .write_magic(KT_REMOVE_BULK)
         .write_int(0)  # Flags.
         .write_key_list_with_db(keys, db)
         .send())

        response = self.response()
        response.check_error(KT_REMOVE_BULK)
        return response.read_int()

    def remove(self, key, db):
        return self.remove_bulk((key,), db)

    def script(self, name, data=None):
        cdef:
            bytes bname = _encode(name)
            RequestBuffer request = self.request()
            KTResponseHandler response

        data = data or {}
        (request
         .write_magic(KT_PLAY_SCRIPT)
         .write_ints((0, len(bname), len(data)))
         .write_bytes(bname, False)
         .write_keys_values(data)
         .send())

        response = self.response()
        response.check_error(KT_PLAY_SCRIPT)
        return response.read_keys_values()


cdef class TTBinaryProtocol(BinaryProtocol):
    cdef TTResponseHandler response(self):
        return TTResponseHandler(
            self._socket,
            self.decode_key,
            self.decode_value)

    cdef _key_value_cmd(self, key, value, bytes bmagic):
        cdef:
            RequestBuffer request = self.request()

        (request
         .write_magic(bmagic)
         .write_key_value(key, value)
         .send())
        return self.response().check_error()

    def put(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x10') == 0

    def putkeep(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x11') == 0

    def putcat(self, key, value):
        return self._key_value_cmd(key, value, b'\xc8\x12') == 0

    def out(self, key):
        cdef:
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x20')
         .write_key(key)
         .send())
        return self.response().check_error()

    def get(self, key):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x30')
         .write_key(key)
         .send())

        response = self.response()
        if not response.check_error():
            return response.read_value()

    def get_bulk(self, keys):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x31')
         .write_key_list(keys)
         .send())

        response = self.response()
        if not response.check_error():
            return response.read_keys_values()

    def vsiz(self, key):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x38')
         .write_key(key)
         .send())
        response = self.response()
        if not response.check_error():
            return response.read_int()

    def addint(self, key, value):
        cdef:
            bytes bkey = _encode(key)
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x60')
         .write_ints((len(bkey), value))
         .write_bytes(bkey, False)
         .send())
        response = self.response()
        if not response.check_error():
            return response.read_int()

    def script(self, name, key=None, value=None):
        cdef:
            bytes bname = _encode(name)
            bytes bkey = _encode(key or '')
            bytes bval = self.encode_value(value or '')
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x68')
         .write_ints((len(bname), 0, len(bkey), len(bval)))
         .write_bytes(bname, False)
         .write_bytes(bkey, False)
         .write_bytes(bval, False)
         .send())

        response = self.response()
        if response.check_error():
            return response.read_bytes()

    def misc(self, name, keys=None, data=None):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

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

        (request
         .write_magic(b'\xc8\x90')
         .write_ints((len(bname), 0, nargs))
         .write_bytes(bname, False))

        if bname.startswith(b'put'):
            for key, value in data.items():
                (request
                 .write_key(key)
                 .write_bytes(self.encode_value(value), True))
        else:
            for key in keys:
                request.write_key(key)

        request.send()
        response = self.response()
        if response.check_error():
            return

        nelem = response.read_int()

        if nelem == 0 and bname != b'getlist':
            return True
        elif nelem == 1:
            nval = response.read_int()
            if nval > 0:
                return self.decode_value(self._socket.read(nval))
        else:
            accum = {}
            for _ in range(nelem // 2):
                key = response.read_key()
                value = response.read_value()
                accum[key] = value
            return accum

    def vanish(self):
        self.request().write_magic(b'\xc8\x72').send()
        return self.response().check_error()

    def _long_cmd(self, bytes bmagic):
        self.request().write_magic(bmagic).send()
        response = self.response()
        response.check_error()
        return response.read_long()

    def rnum(self):
        return self._long_cmd(b'\xc8\x80')

    def size(self):
        return self._long_cmd(b'\xc8\x81')

    def stat(self):
        self.request().write_magic(b'\xc8\x88').send()
        response = self.response()
        response.check_error()
        return response.read_bytes()
