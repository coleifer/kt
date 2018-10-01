from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdint cimport int64_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport memcpy

import heapq
import io
import math
import socket
import struct
import threading
try:
    from threading import get_ident
except ImportError:
    from thread import get_ident
import time

from kt.exceptions import ProtocolError
from kt.exceptions import ScriptError
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


cdef int READSIZE = 64 * 1024


cdef class _Socket(object):
    cdef:
        bytearray recvbuf
        int bytes_read
        int bytes_written
        readonly bint is_closed
        buf
        _socket

    def __init__(self, s):
        self._socket = s
        self.is_closed = False
        self.buf = io.BytesIO()
        self.bytes_read = self.bytes_written = 0
        self.recvbuf = bytearray(READSIZE)

    def __del__(self):
        if not self.is_closed:
            self.buf.close()
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()

    cdef _read_from_socket(self, int length):
        cdef:
            int l = 0
            int marker = 0

        recvptr = memoryview(self.recvbuf)
        self.buf.seek(self.bytes_written)

        try:
            while True:
                l = self._socket.recv_into(recvptr, READSIZE)
                if not l:
                    self.close()
                    raise ServerConnectionError('server went away')
                self.buf.write(recvptr[:l])
                self.bytes_written += l
                marker += l
                if length > 0 and length > marker:
                    continue
                break
        except socket.timeout:
            raise ServerConnectionError('timed out reading from socket')
        except socket.error:
            raise ServerConnectionError('error while reading from socket')

    cdef recv(self, int length):
        cdef:
            bytes data
            int buflen = self.bytes_written - self.bytes_read

        if length > buflen:
            self._read_from_socket(length - buflen)

        self.buf.seek(self.bytes_read)
        data = self.buf.read(length)
        self.bytes_read += length

        if self.bytes_read == self.bytes_written:
            self.purge()
        return data

    cdef send(self, bytes data):
        try:
            self._socket.sendall(data)
        except IOError:
            self.close()
            raise ServerConnectionError('server went away')

    cdef purge(self):
        self.buf.seek(0)
        self.buf.truncate()
        self.bytes_read = self.bytes_written = 0

    cdef bint close(self):
        if self.is_closed:
            return False

        self.purge()
        self.buf.close()
        self.buf = None

        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self.is_closed = True
        return True


cdef class SocketPool(object):
    cdef:
        dict in_use
        list free
        readonly bint nodelay
        readonly int port
        readonly str host
        readonly timeout
        mutex

    def __init__(self, host, port, timeout=None, nodelay=False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.nodelay = nodelay
        self.in_use = {}
        self.free = []
        self.mutex = threading.Lock()

    cdef _Socket checkout(self):
        cdef:
            float now = time.time()
            float ts
            long tid = get_ident()
            _Socket s

        with self.mutex:
            if tid in self.in_use:
                s = self.in_use[tid]
                if s.is_closed:
                    del self.in_use[tid]
                else:
                    return s

            while self.free:
                ts, s = heapq.heappop(self.free)
                self.in_use[tid] = s
                return s

            s = self.create_socket()
            self.in_use[tid] = s
            return s

    cdef checkin(self):
        cdef:
            long tid = get_ident()
            _Socket s

        if tid in self.in_use:
            s = self.in_use.pop(tid)
            if not s.is_closed:
                heapq.heappush(self.free, (time.time(), s))

    cdef close(self):
        cdef:
            long tid = get_ident()
            _Socket s

        s = self.in_use.pop(tid, None)
        if s and not s.is_closed:
            s.close()

    cdef int close_idle(self, cutoff=60):
        cdef:
            float now = time.time()
            float ts
            int n = 0
            _Socket sock

        with self.mutex:
            while self.free:
                ts, sock = heapq.heappop(self.free)
                if ts > (now - cutoff):
                    heapq.heappush(self.free, (ts, sock))
                    break
                else:
                    n += 1

        return n

    cdef close_all(self):
        cdef:
            int n = 0
            _Socket sock

        with self.mutex:
            while self.free:
                _, sock = self.free.pop()
                sock.close()
                n += 1

            tmp = self.in_use
            self.in_use = {}
            for sock in tmp.values():
                sock.close()
                n += 1

        return n

    cdef _Socket create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.nodelay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.connect((self.host, self.port))
        if self.timeout:
            sock.settimeout(self.timeout)
        return _Socket(sock)


cdef class RequestBuffer(object):
    cdef:
        object value_encode
        public object buf
        _Socket _socket

    def __init__(self, _Socket socket, value_encode=None):
        self._socket = socket
        self.value_encode = value_encode
        self.buf = io.BytesIO()

    cdef RequestBuffer write_magic(self, magic):
        self.buf.write(magic)
        return self

    cdef RequestBuffer write_int(self, int i):
        self.buf.write(s_pack('>I', i))
        return self

    cdef RequestBuffer write_ints(self, ints):
        fmt = 'I' * len(ints)
        self.buf.write(s_pack('>%s' % fmt, *ints))
        return self

    cdef RequestBuffer write_short(self, s):
        self.buf.write(s_pack('>H', s))
        return self

    cdef RequestBuffer write_long(self, l):
        self.buf.write(s_pack('>q', l))
        return self

    cdef RequestBuffer write_double(self, d):
        m, i = math.modf(d)
        m, i = int(m * 1e12), int(i)
        self.buf.write(s_pack('>QQ', i, m))
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
            bkey = _encode(key)
            (self
             .write_short(db)
             .write_bytes(bkey, True))
        return self

    cdef RequestBuffer write_keys_values_with_db_expire(self, data, db,
                                                        expire):
        cdef bytes bkey, bval
        self.write_int(len(data))
        for key, value in data.items():
            bkey = _encode(key)
            bval = self.value_encode(value)
            (self
             .write_short(db)
             .write_ints((len(bkey), len(bval)))
             .write_long(expire)
             .write_bytes(bkey, False)
             .write_bytes(bval, False))
        return self

    cdef RequestBuffer write_key(self, key):
        return self.write_bytes(_encode(key), True)

    cdef RequestBuffer write_key_list(self, keys):
        cdef bytes bkey
        self.write_int(len(keys))
        for key in keys:
            bkey = _encode(key)
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
            bytes bkey = _encode(key)
            bytes bval = self.value_encode(value)
        return (self
                .write_ints((len(bkey), len(bval)))
                .write_bytes(bkey, False)
                .write_bytes(bval, False))

    cdef send(self):
        self._socket.send(self.buf.getvalue())
        self.buf = io.BytesIO()


cdef class BaseResponseHandler(object):
    cdef:
        bint _decode_keys
        object value_decode
        public _Socket _socket

    def __init__(self, sock, decode_keys, value_decode):
        self._socket = sock
        self._decode_keys = decode_keys
        self.value_decode = value_decode

    cdef bytes read(self, int n):
        cdef bytes value
        if n > 0:
            value = self._socket.recv(n)
        else:
            value = b''
        return value

    cdef int read_int(self):
        return s_unpack('>I', self.read(4))[0]

    cdef int read_long(self):
        return s_unpack('>q', self.read(8))[0]

    cdef double read_double(self):
        cdef:
            int64_t i, m
        i, m = s_unpack('>QQ', self.read(16))
        return i + (m * 1e-12)

    cdef bytes read_bytes(self):
        return self.read(self.read_int())

    cdef read_key(self):
        cdef bytes bkey = self.read_bytes()
        return _decode(bkey) if self._decode_keys else bkey

    cdef read_value(self):
        return self.value_decode(self.read_bytes())

    cdef list read_keys(self):
        cdef:
            int i
            int n = self.read_int()
            list accum = []

        for i in range(n):
            accum.append(self.read_key())
        return accum

    cdef tuple read_key_value(self, decode_value):
        cdef:
            bytes bkey, bval
            int klen, vlen
        klen, vlen = s_unpack('>II', self.read(8))
        bkey = self.read(klen)
        bval = self.read(vlen)
        return ((_decode(bkey) if self._decode_keys else bkey),
                self.value_decode(bval) if decode_value else bval)

    cdef dict read_keys_values(self, decode_values):
        cdef:
            dict accum = {}
            int i, n_items

        n_items = self.read_int()
        for i in range(n_items):
            key, value = self.read_key_value(decode_values)
            accum[key] = value
        return accum

    cdef tuple read_key_value_with_db_expire(self, decode_value):
        cdef:
            bytes bkey, bval
            int klen, vlen

        _, klen, vlen, _ = s_unpack('>HIIq', self.read(18))
        bkey = self.read(klen)
        bval = self.read(vlen)
        return ((_decode(bkey) if self._decode_keys else bkey),
                self.value_decode(bval) if decode_value else bval)

    cdef dict read_keys_values_with_db_expire(self, decode_values):
        cdef:
            dict accum = {}
            int i, n_items

        n_items = self.read_int()
        for i in range(n_items):
            key, value = self.read_key_value_with_db_expire(decode_values)
            accum[key] = value
        return accum


cdef class KTResponseHandler(BaseResponseHandler):
    cdef int check_error(self, magic) except -1:
        cdef:
            bytes bmagic
            int imagic

        bmagic = self.read(1)
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

        bmagic = self.read(1)
        imagic = ord(bmagic)
        if imagic == 0 or imagic == 1:
            return imagic
        else:
            raise ServerError('Unexpected server response: %x' % imagic)


cdef class BinaryProtocol(object):
    cdef:
        readonly bint _decode_keys
        readonly str _host
        readonly int _port
        readonly encode_value
        readonly decode_value
        readonly SocketPool _socket_pool

    def __init__(self, host='127.0.0.1', port=1978, decode_keys=True,
                 encode_value=None, decode_value=None, timeout=None):
        self._host = host
        self._port = port
        self._decode_keys = decode_keys
        self.encode_value = encode_value or encode
        self.decode_value = decode_value or decode
        self._socket_pool = SocketPool(self._host, self._port, timeout,
                                       nodelay=True)

    def __del__(self):
        self._socket_pool.close()

    def checkin(self):
        self._socket_pool.checkin()

    def close(self):
        return self._socket_pool.close()

    def close_all(self):
        return self._socket_pool.close_all()

    def close_idle(self, n=60):
        return self._socket_pool.close_idle(n)

    def serialize_list(self, data):
        return _serialize_list(data)

    def deserialize_list(self, data, decode_values=True):
        return _deserialize_list(data, decode_values)

    def serialize_dict(self, data):
        return _serialize_dict(data)

    def deserialize_dict(self, data, decode_values=True):
        return _deserialize_dict(data, decode_values)

    cdef RequestBuffer request(self):
        return RequestBuffer(
            self._socket_pool.checkout(),
            self.encode_value)


cdef class KTBinaryProtocol(BinaryProtocol):
    cdef KTResponseHandler response(self):
        return KTResponseHandler(
            self._socket_pool.checkout(),
            self._decode_keys,
            self.decode_value)

    def get_bulk(self, keys, db, decode_value=True):
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
        return response.read_keys_values_with_db_expire(decode_value)

    def get(self, key, db, decode_value=True):
        cdef bytes bkey = encode(key)
        result = self.get_bulk((bkey,), db, decode_value)
        return result.get(_decode(bkey) if self._decode_keys else bkey)

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

    def script(self, name, data=None, encode_values=True):
        cdef:
            bytes bname = _encode(name)
            bytes bkey, bval
            RequestBuffer request = self.request()
            KTResponseHandler response

        data = data or {}
        (request
         .write_magic(KT_PLAY_SCRIPT)
         .write_ints((0, len(bname), len(data)))
         .write_bytes(bname, False))

        for key in data:
            bkey = _encode(key)
            if encode_values:
                bval = self.encode_value(data[key])
            else:
                bval = _encode(data[key])
            (request
             .write_ints((len(bkey), len(bval)))
             .write_bytes(bkey, False)
             .write_bytes(bval, False))

        request.send()

        response = self.response()
        response.check_error(KT_PLAY_SCRIPT)

        return response.read_keys_values(encode_values)


cdef class TTBinaryProtocol(BinaryProtocol):
    cdef TTResponseHandler response(self):
        return TTResponseHandler(
            self._socket_pool.checkout(),
            self._decode_keys,
            self.decode_value)

    cdef _key_value_cmd(self, key, value, bytes bmagic):
        cdef RequestBuffer request = self.request()

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

    def putshl(self, key, value, width):
        cdef:
            bytes bkey = _encode(key)
            bytes bval = self.encode_value(value)
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x13')
         .write_ints((len(bkey), len(bval), width))
         .write_bytes(bkey, False)
         .write_bytes(bval, False)
         .send())

        return self.response().check_error() == 0

    def putnr(self, key, value):
        cdef RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x18')
         .write_key_value(key, value)
         .send())

    def mputnr(self, data):
        cdef RequestBuffer request = self.request()
        for key, value in data.items():
            (request
             .write_magic(b'\xc8\x18')
             .write_key_value(key, value)
             .send())

    def out(self, key):
        cdef RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x20')
         .write_key(key)
         .send())
        return 0 if self.response().check_error() else 1

    def get(self, key, decode_value=True):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        request.write_magic(b'\xc8\x30').write_key(key).send()
        response = self.response()
        if not response.check_error():
            return (response.read_value()
                    if decode_value else response.read_bytes())

    def mget(self, keys, decode_values=True):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x31')
         .write_key_list(keys)
         .send())

        response = self.response()
        if not response.check_error():
            return response.read_keys_values(decode_values)

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

    cdef _simple_command(self, bytes bmagic):
        cdef RequestBuffer request = self.request()
        request.write_magic(bmagic).send()
        return self.response().check_error() == 0

    def iterinit(self):
        return self._simple_command(b'\xc8\x50')

    def iternext(self):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        request.write_magic(b'\xc8\x51').send()
        response = self.response()
        if not response.check_error():
            return response.read_key()

    def fwmkeys(self, prefix, max_keys=1024):
        cdef:
            bytes bprefix = _encode(prefix)
            RequestBuffer request = self.request()
            TTResponseHandler response

        # fwmkeys method.
        (request
         .write_magic(b'\xc8\x58')
         .write_ints((len(bprefix), max_keys))
         .write_bytes(bprefix, False)
         .send())

        response = self.response()
        if not response.check_error():
            return response.read_keys()

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

    def adddouble(self, key, value):
        cdef:
            bytes bkey = _encode(key)
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x61')
         .write_int(len(bkey))
         .write_double(value)
         .write_bytes(bkey, False)
         .send())

        response = self.response()
        if not response.check_error():
            return response.read_double()

    def ext(self, name, key=None, value=None, lock_records=False,
            lock_all=False, encode_value=True, decode_result=False):
        cdef:
            bytes bname = _encode(name)
            bytes bkey = _encode(key or '')
            bytes bval
            int opts = 0
            RequestBuffer request = self.request()
            TTResponseHandler response

        if encode_value:
            bval = self.encode_value(value or '')
        else:
            bval = _encode(value or '')

        if lock_records and lock_all:
            raise ValueError('cannot specify both record and global locking.')

        if lock_records:
            opts = 1
        elif lock_all:
            opts = 2

        (request
         .write_magic(b'\xc8\x68')
         .write_ints((len(bname), opts, len(bkey), len(bval)))
         .write_bytes(bname, False)
         .write_bytes(bkey, False)
         .write_bytes(bval, False)
         .send())

        response = self.response()
        if response.check_error():
            raise ScriptError('error calling %s(%s, %s)' % (name, key, value))

        resp = response.read_bytes()
        if resp == b'true':
            return True
        elif resp == b'false':
            return False
        if decode_result:
            return self.decode_value(resp)
        return resp

    def sync(self):
        return self._simple_command(b'\xc8\x70')

    def optimize(self, options):
        cdef:
            bytes boptions = _encode(options)
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x71')
         .write_bytes(boptions, True)
         .send())
        return self.response().check_error() == 0

    def vanish(self):
        return self._simple_command(b'\xc8\x72')

    def copy(self, path):
        cdef:
            bytes bpath = _encode(path)
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x73')
         .write_bytes(bpath, True)
         .send())
        return self.response().check_error() == 0

    def restore(self, path, timestamp, opts=0):
        cdef:
            bytes bpath = _encode(path)
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x74')
         .write_int(len(bpath))
         .write_long(timestamp)
         .write_int(opts)
         .write_bytes(bpath, False)
         .send())
        return self.response().check_error() == 0

    def setmst(self, host, port, timestamp, opts=0):
        cdef:
            bytes bhost = _encode(host)
            RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x78')
         .write_ints((len(bhost), port))
         .write_long(timestamp)
         .write_int(opts)
         .write_bytes(bhost, False)
         .send())
        return self.response().check_error() == 0

    def _long_cmd(self, bytes bmagic):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        request.write_magic(bmagic).send()

        response = self.response()
        if not response.check_error():
            return response.read_long()

    def rnum(self):
        return self._long_cmd(b'\xc8\x80')

    def size(self):
        return self._long_cmd(b'\xc8\x81')

    def stat(self):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        self.request().write_magic(b'\xc8\x88').send()
        response = self.response()
        if not response.check_error():
            return response.read_bytes()

    cdef _misc(self, name, args, update_log):
        cdef:
            bytes arg
            bytes bname = _encode(name)
            int nargs
            int rv
            list accum = []
            RequestBuffer request = self.request()
            TTResponseHandler response

        # TokyoTyrant supports "fluent" commands - kinda like Redis, you pass
        # a command name and the requested parameters, get appropriate resp.
        if args is None:
            args = []
            nargs = 0
        else:
            nargs = len(args)

        (request
         .write_magic(b'\xc8\x90')
         .write_ints((len(bname), 0 if update_log else 1, nargs))
         .write_bytes(bname, False))

        # Write all arguments, which are assumed to be bytes.
        for arg in args:
            request.write_bytes(arg, True)

        request.send()

        response = self.response()
        rv = response.check_error()  # 1 if simple error, 0 if OK.
        nelem = response.read_int()
        if rv != 0:
            return

        for _ in range(nelem):
            accum.append(response.read_bytes())
        return accum

    def misc(self, name, args=None, update_log=True):
        return self._misc(name, args or [], update_log)

    cdef _misc_kv(self, cmd, key, value, update_log):
        cdef:
            bytes bkey = _encode(key)
            bytes bval = self.encode_value(value)
        return self._misc(cmd, [bkey, bval], update_log) is not None

    def misc_put(self, key, value, update_log=True):
        return self._misc_kv('put', key, value, update_log)

    def misc_putkeep(self, key, value, update_log=True):
        return self._misc_kv('putkeep', key, value, update_log)

    def misc_putcat(self, key, value, update_log=True):
        return self._misc_kv('putcat', key, value, update_log)

    def misc_putdup(self, key, value, update_log=True):
        return self._misc_kv('putdup', key, value, update_log)

    def misc_putdupback(self, key, value, update_log=True):
        return self._misc_kv('putdupback', key, value, update_log)

    def misc_out(self, key, update_log=True):
        return self._misc('out', [_encode(key)], update_log) is not None

    def misc_get(self, key):
        res = self._misc('get', [_encode(key)], False)
        if res:
            return self.decode_value(res[0])

    def misc_putlist(self, data, update_log=True):
        cdef list accum = []
        for key, value in data.items():
            accum.append(_encode(key))
            accum.append(self.encode_value(value))
        return self._misc('putlist', accum, update_log) is not None

    def _misc_key_list(self, cmd, keys, update_log):
        cdef list accum = [_encode(key) for key in keys]
        return self._misc(cmd, accum, update_log)

    def misc_outlist(self, keys, update_log=True):
        self._misc_key_list('outlist', keys, update_log)
        return True

    cdef _misc_list_to_dict(self, list items):
        if items is None: return {}

        cdef:
            dict result = {}
            int i = 0, l = len(items)

        while i < l:
            result[_decode(items[i])] = self.decode_value(items[i + 1])
            i += 2
        return result

    def misc_getlist(self, keys, update_log=True):
        cdef list items = self._misc_key_list('getlist', keys, update_log)
        return self._misc_list_to_dict(items)

    def misc_getpart(self, key, start=0, length=None):
        args = [_encode(key), _encode(str(start))]
        if length is not None:
            args.append(_encode(str(length)))
        result = self._misc('getpart', args, False)
        if result:
            return _decode(result[0])

    def misc_iterinit(self, key=None):
        args = [_encode(key)] if key else []
        return self._misc('iterinit', args, False) is not None

    def misc_iternext(self):
        ret = self._misc('iternext', [], False)
        if ret:
            return (_decode(ret[0]), self.decode_value(ret[1]))

    def misc_sync(self):
        return self._misc('sync', [], True) is not None

    def misc_optimize(self, opts, update_log=True):
        return self._misc('optimize', [_encode(opts)], update_log) is not None

    def misc_vanish(self, update_log=True):
        return self._misc('vanish', [], update_log) is not None

    def misc_error(self):
        ret = self._misc('error', [], False)
        if ret:
            return _decode(ret[0])

    def misc_cacheclear(self):
        return self._misc('cacheclear', [], False) is not None

    def misc_defragment(self, nsteps=None, update_log=True):
        args = [_encode(str(nsteps))] if nsteps is not None else []
        return self._misc('defrag', args, update_log) is not None

    def misc_regex(self, regex, max_records=None):
        cdef list items
        args = [_encode(regex)]
        if max_records is not None:
            args.append(_encode(str(max_records)))
        items = self._misc('regex', args, False)
        return self._misc_list_to_dict(items)

    def misc_range(self, start, stop=None, max_records=0):
        cdef list items
        args = [_encode(start), _encode(str(max_records))]
        if stop is not None:
            args.append(_encode(stop))
        items = self._misc('range', args, False)
        return self._misc_list_to_dict(items)

    def misc_setindex(self, column, index_type, update_log=True):
        args = [_encode(column), _encode(str(index_type))]
        return self._misc('setindex', args, update_log) is not None

    def misc_search(self, conditions, cmd=None, update_log=True):
        cdef list items
        if cmd is not None:
            conditions.append(_encode(cmd))

        items = self._misc('search', conditions, update_log)
        if cmd is None:
            return [_decode(key) for key in items]
        elif cmd == 'get':
            accum = []
            for item in items:
                key, rest = item[1:].split(b'\x00', 1)
                accum.append((_decode(key), rest))
            return accum
        elif cmd == 'count':
            return int(items[0])
        elif len(items) == 0:
            return True
        else:
            raise ProtocolError('Unexpected results for search cmd=%s' % cmd)

    def misc_genuid(self, update_log=True):
        ret = self._misc('genuid', [], update_log)
        return int(ret[0])

    def keys(self):
        cdef TTResponseHandler response

        # iterinit method.
        self.request().write_magic(b'\xc8\x50').send()
        if self.response().check_error():
            return []

        while True:
            # iternext method.
            self.request().write_magic(b'\xc8\x51').send()
            response = self.response()
            if response.check_error():
                raise StopIteration
            yield response.read_key()

    def items(self, start_key=None):
        self.misc_iterinit(start_key)
        while True:
            result = self.misc_iternext()
            if result is None:
                raise StopIteration
            yield result


def dict_to_table(dict d):
    buf = io.BytesIO()
    for key, value in d.items():
        buf.write(encode(key))
        buf.write(b'\x00')
        buf.write(encode(value))
        buf.write(b'\x00')
    return buf.getvalue()


def table_to_dict(bytes table):
    cdef:
        bytes bkey, bval
        dict d = {}
        list items = table.split(b'\x00')
        int i = 0
        int l = len(items) - 1

    while i < l:
        bkey = items[i]
        bval = items[i + 1]
        d[decode(bkey)] = decode(bval)
        i += 2
    return d


# Serialization method compatible with KyotoTycoon's lua "mapdump" function.
cdef bytes _serialize_dict(dict d):
    cdef:
        bytes bkey, bnum, bvalue
        char *kbuf
        char *vbuf
        int knbytes, vnbytes
        Py_ssize_t kbuflen, vbuflen
        unsigned char knumbuf[8]
        unsigned char vnumbuf[8]

    data = io.BytesIO()

    for key in d:
        bkey = _encode(key)
        bvalue = _encode(d[key])
        PyBytes_AsStringAndSize(bkey, &kbuf, &kbuflen)
        PyBytes_AsStringAndSize(bvalue, &vbuf, &vbuflen)

        knbytes = _writevarnum(knumbuf, <uint64_t>kbuflen)
        vnbytes = _writevarnum(vnumbuf, <uint64_t>vbuflen)
        data.write(knumbuf[:knbytes])
        data.write(vnumbuf[:vnbytes])
        data.write(bkey)
        data.write(bvalue)

    return data.getvalue()


# Serialization method compatible with KyotoTycoon's lua "mapload" function.
cdef dict _deserialize_dict(bytes data, bint deserialize):
    cdef:
        Py_ssize_t buflen
        bytes bkey, bval
        char *buf
        char *kbuf = <char *>malloc(128 * sizeof(char))
        char *vbuf = <char *>malloc(1024 * sizeof(char))
        dict accum = {}
        size_t kitemsize = 128
        size_t vitemsize = 1024
        size_t kstep, vstep
        uint64_t knum, vnum

    # Get reference to underlying pointer and length of data.
    PyBytes_AsStringAndSize(data, &buf, &buflen)

    while buflen > 0:
        # Read a variable-sized integer from the data buffer. The number of
        # bytes used to encode the number is returned as "kstep", and the
        # number itself is stored in "knum".
        kstep = _readvarnum(<unsigned char *>buf, buflen, &knum)

        if buflen < kstep + knum:
            free(kbuf); free(vbuf)
            raise ValueError('corrupt key, refusing to process')

        # Move the data pointer forward to the start of the value size.
        buf += kstep
        buflen -= kstep

        vstep = _readvarnum(<unsigned char *>buf, buflen, &vnum)

        if buflen < vstep + vnum:
            free(kbuf); free(vbuf)
            raise ValueError('corrupt value, refusing to process')

        # Move to start of key data.
        buf += vstep
        buflen -= vstep

        # Can we reuse our item buffer?
        if knum > kitemsize:
            free(kbuf)
            kbuf = <char *>malloc(knum * sizeof(unsigned char))

        memcpy(kbuf, buf, knum)
        bkey = kbuf[:knum]

        # Move to start of value.
        buf += knum
        buflen -= knum

        if vnum > vitemsize:
            free(vbuf)
            vbuf = <char *>malloc(vnum * sizeof(unsigned char))

        memcpy(vbuf, buf, vnum)
        bval = vbuf[:vnum]

        # Move to end of value.
        buf += vnum
        buflen -= vnum

        if deserialize:
            accum[_decode(bkey)] = _decode(bval)
        else:
            accum[_decode(bkey)] = bval

    if kbuf:
        free(kbuf)
    if vbuf:
        free(vbuf)

    return accum


# Serialization method compatible with KyotoTycoon's lua "arraydump" function.
cdef bytes _serialize_list(l):
    cdef:
        bytes bnum, bvalue
        char *buf
        int nbytes
        Py_ssize_t buflen
        unsigned char numbuf[8]

    data = io.BytesIO()

    for i in range(len(l)):
        bvalue = _encode(l[i])
        PyBytes_AsStringAndSize(bvalue, &buf, &buflen)
        nbytes = _writevarnum(numbuf, <uint64_t>buflen)
        data.write(numbuf[:nbytes])
        data.write(bvalue)

    return data.getvalue()


# Serialization method compatible with KyotoTycoon's lua "arrayload" function.
cdef list _deserialize_list(bytes data, bint deserialize):
    cdef:
        Py_ssize_t buflen
        bytes bitem
        char *buf
        char *item = <char *>malloc(1024 * sizeof(char))
        list accum = []
        size_t itemsize = 1024
        size_t step
        uint64_t num

    # Get reference to underlying pointer and length of data.
    PyBytes_AsStringAndSize(data, &buf, &buflen)

    while buflen > 0:
        # Read a variable-sized integer from the data buffer. The number of
        # bytes used to encode the number is returned as "step", and the number
        # itself is stored in "num".
        step = _readvarnum(<unsigned char *>buf, buflen, &num)

        if buflen < step + num:
            free(item)
            raise ValueError('corrupt array item, refusing to process')

        # Move the data pointer forward to the start of the data.
        buf += step
        buflen -= step

        # Can we reuse our item buffer?
        if num > itemsize:
            free(item)
            item = <char *>malloc(num * sizeof(unsigned char))

        memcpy(item, buf, num)
        bitem = item[:num]
        if deserialize:
            accum.append(_decode(bitem))
        else:
            accum.append(bitem)
        buf += num
        buflen -= num

    if item:
        free(item)

    return accum


cdef inline int _writevarnum(unsigned char *buf, uint64_t num):
    if num < (1 << 7):
        buf[0] = <unsigned char>num
        return 1
    elif num < (1 << 14):
        buf[0] = <unsigned char>((num >> 7) | 0x80)
        buf[1] = <unsigned char>(num & 0x7f)
        return 2
    elif num < (1 << 21):
        buf[0] = <unsigned char>((num >> 14) | 0x80)
        buf[1] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(num & 0x7f)
        return 3
    elif num < (1 << 28):
        buf[0] = <unsigned char>((num >> 21) | 0x80)
        buf[1] = <unsigned char>(((num >> 14) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[3] = <unsigned char>(num & 0x7f)
        return 4
    elif num < (1 << 35):
        buf[0] = <unsigned char>((num >> 28) | 0x80)
        buf[1] = <unsigned char>(((num >> 21) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(((num >> 14) & 0x7f) | 0x80)
        buf[3] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[4] = <unsigned char>(num & 0x7f)
        return 5
    elif num < (1 << 42):
        buf[0] = <unsigned char>((num >> 35) | 0x80)
        buf[1] = <unsigned char>(((num >> 28) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(((num >> 21) & 0x7f) | 0x80)
        buf[3] = <unsigned char>(((num >> 14) & 0x7f) | 0x80)
        buf[4] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[5] = <unsigned char>(num & 0x7f)
        return 6
    elif num < (1 << 49):
        buf[0] = <unsigned char>((num >> 42) | 0x80)
        buf[1] = <unsigned char>(((num >> 35) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(((num >> 28) & 0x7f) | 0x80)
        buf[3] = <unsigned char>(((num >> 21) & 0x7f) | 0x80)
        buf[4] = <unsigned char>(((num >> 14) & 0x7f) | 0x80)
        buf[5] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[6] = <unsigned char>(num & 0x7f)
        return 7
    elif num < (1 << 56):
        buf[0] = <unsigned char>((num >> 49) | 0x80)
        buf[1] = <unsigned char>(((num >> 42) & 0x7f) | 0x80)
        buf[2] = <unsigned char>(((num >> 35) & 0x7f) | 0x80)
        buf[3] = <unsigned char>(((num >> 28) & 0x7f) | 0x80)
        buf[4] = <unsigned char>(((num >> 21) & 0x7f) | 0x80)
        buf[5] = <unsigned char>(((num >> 14) & 0x7f) | 0x80)
        buf[6] = <unsigned char>(((num >> 7) & 0x7f) | 0x80)
        buf[7] = <unsigned char>(num & 0x7f)
        return 8
    return 0


cdef inline size_t _readvarnum(unsigned char *buf, size_t size, uint64_t *np):
    cdef:
        unsigned char *rp = buf
        unsigned char *ep = rp + size
        uint64_t num = 0
        uint32_t c

    while rp < ep:
        if rp >= ep:
            np[0] = 0
            return 0
        c = rp[0]
        num = (num << 7) + (c & 0x7f)
        rp += 1
        if c < 0x80:
            break
    np[0] = num
    return rp - <unsigned char *>buf
