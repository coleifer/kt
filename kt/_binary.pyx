from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdint cimport int64_t

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


cdef class _Socket(object):
    cdef:
        readonly bint is_closed
        _socket

    def __init__(self, s):
        self._socket = s
        self.is_closed = False

    def __del__(self):
        if not self.is_closed:
            self._socket.close()

    cdef recv(self, int n):
        cdef:
            bytearray result = bytearray(n)  # Allocate buffer of size n.
            int l = 0

        buf = memoryview(result)  # Obtain "pointer" to head of buffer.
        while n:
            l = self._socket.recv_into(buf, n)
            if not l:
                self.close()
                raise ServerConnectionError('server went away')
            n -= l
            buf = buf[l:]  # Advance pointer by number of bytes read.
        return bytes(result)  # Return bytes.

    cdef send(self, bytes data):
        try:
            self._socket.sendall(data)
        except IOError:
            self.close()
            raise ServerConnectionError('server went away')

    cdef bint close(self):
        if self.is_closed:
            return False

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


cdef class BaseResponseHandler(object):
    cdef:
        object key_decode
        object value_decode
        public _Socket _socket

    def __init__(self, sock, key_decode, value_decode):
        self._socket = sock
        self.key_decode = key_decode
        self.value_decode = value_decode

    cdef bytes read(self, int n):
        cdef bytes value = b''
        if n > 0:
            value = self._socket.recv(n)
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
        return self.key_decode(self.read_bytes())

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

    cdef tuple read_key_value(self):
        cdef:
            int klen, vlen
        klen, vlen = s_unpack('>II', self.read(8))
        return (self.key_decode(self.read(klen)),
                self.value_decode(self.read(vlen)))

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

        _, klen, vlen, _ = s_unpack('>HIIq', self.read(18))
        return (self.key_decode(self.read(klen)),
                self.value_decode(self.read(vlen)))

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
        readonly str _host
        readonly int _port
        readonly decode_key
        readonly encode_value
        readonly decode_value
        readonly SocketPool _socket_pool

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

    cdef RequestBuffer request(self):
        return RequestBuffer(
            self._socket_pool.checkout(),
            self.encode_value)


cdef class KTBinaryProtocol(BinaryProtocol):
    cdef KTResponseHandler response(self):
        return KTResponseHandler(
            self._socket_pool.checkout(),
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

        if encode_values:
            return response.read_keys_values()
        else:
            # Handle reading "raw" with noop-decoder.
            response = KTResponseHandler(self._socket, self.decode_key,
                                         noop_decode)
            return response.read_keys_values()


cdef class TTBinaryProtocol(BinaryProtocol):
    cdef TTResponseHandler response(self):
        return TTResponseHandler(
            self._socket_pool.checkout(),
            self.decode_key,
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

    def out(self, key):
        cdef RequestBuffer request = self.request()

        (request
         .write_magic(b'\xc8\x20')
         .write_key(key)
         .send())
        return 0 if self.response().check_error() else 1

    def get(self, key):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        request.write_magic(b'\xc8\x30').write_key(key).send()
        response = self.response()
        if not response.check_error():
            return response.read_value()

    def mget(self, keys):
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
               lock_all=False):
        cdef:
            bytes bname = _encode(name)
            bytes bkey = _encode(key or '')
            bytes bval = self.encode_value(value or '')
            int opts = 0
            RequestBuffer request = self.request()
            TTResponseHandler response

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

        return response.read_bytes()

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
