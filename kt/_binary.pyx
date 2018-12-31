cimport cython
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION
from libc.stdint cimport int32_t
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
DEF KT_ERROR = b'\xbf'
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


cdef int READSIZE = 16 * 1024


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
        readonly int port
        readonly str host
        readonly timeout
        mutex

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.in_use = {}
        self.free = []
        self.mutex = threading.Lock()

    def stats(self):
        return len(self.in_use), len(self.free)

    cdef _Socket create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self.timeout:
            sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))
        return _Socket(sock)

    cdef _Socket checkout(self):
        cdef:
            float now = time.time()
            float ts
            tid = get_ident()
            _Socket sock

        with self.mutex:
            if tid in self.in_use:
                sock = self.in_use[tid]
                if sock.is_closed:
                    del self.in_use[tid]
                else:
                    return sock

            while self.free:
                ts, sock = heapq.heappop(self.free)
                self.in_use[tid] = sock
                return sock

            sock = self.create_socket()
            self.in_use[tid] = sock
            return sock

    cdef checkin(self):
        cdef:
            tid = get_ident()
            _Socket sock

        if tid in self.in_use:
            sock = self.in_use.pop(tid)
            if not sock.is_closed:
                heapq.heappush(self.free, (time.time(), sock))

    cdef close(self):
        cdef:
            tid = get_ident()
            _Socket sock

        sock = self.in_use.pop(tid, None)
        if sock and not sock.is_closed:
            sock.close()

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


struct_h = struct.Struct('>H')
struct_hi = struct.Struct('>HI')
struct_i = struct.Struct('>I')
struct_ii = struct.Struct('>II')
struct_iii = struct.Struct('>III')
struct_l = struct.Struct('>q')
struct_q = struct.Struct('>Q')
struct_qq = struct.Struct('>QQ')
struct_dbkvxt = struct.Struct('>HIIq')


@cython.freelist(32)
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
        self.buf.write(struct_i.pack(i))
        return self

    cdef RequestBuffer write_ints(self, ints):
        fmt = 'I' * len(ints)
        self.buf.write(s_pack('>%s' % fmt, *ints))
        return self

    cdef RequestBuffer write_ii(self, i1, i2):
        self.buf.write(struct_ii.pack(i1, i2))
        return self

    cdef RequestBuffer write_short(self, s):
        self.buf.write(struct_h.pack(s))
        return self

    cdef RequestBuffer write_long(self, l):
        self.buf.write(struct_l.pack(l))
        return self

    cdef RequestBuffer write_double(self, d):
        m, i = math.modf(d)
        m, i = int(m * 1e12), int(i)
        self.buf.write(struct_qq.pack(i, m))
        return self

    cdef RequestBuffer write_bytes(self, bytes data, write_length):
        if write_length:
            self.buf.write(struct_i.pack(len(data)))
        self.buf.write(data)
        return self

    cdef RequestBuffer write_key_list_with_db(self, keys, db):
        # [k0, k1, k2], db
        cdef bytes bkey
        self.write_int(len(keys))
        for key in keys:
            bkey = _encode(key)
            self.buf.write(struct_hi.pack(db, len(bkey)))
            self.buf.write(bkey)
        return self

    cdef RequestBuffer write_db_key_list(self, data):
        # [(db0, k0), (db1, k1)...]
        cdef bytes bkey
        self.write_int(len(data))
        for db, key in data:
            bkey = _encode(key)
            self.buf.write(struct_hi.pack(db, len(bkey)))
            self.buf.write(bkey)
        return self

    cdef RequestBuffer write_key_value_list_with_db_expire(self, data, db, xt,
                                                           encode_values):
        # [(k0, v0), (k1, v1)...], db, xt
        cdef bytes bkey, bval

        self.buf.write(struct_i.pack(len(data)))
        for key, value in data.items():
            bkey = _encode(key)
            if encode_values:
                bval = self.value_encode(value)
            else:
                bval = _encode(value)
            self.buf.write(struct_dbkvxt.pack(db, len(bkey), len(bval), xt))
            self.buf.write(bkey)
            self.buf.write(bval)
        return self

    cdef RequestBuffer write_db_key_value_expire_list(self, data,
                                                      encode_values):
        # [(db0, k0, v0, xt0), (db1, k1, v1, xt1)...]
        cdef bytes bkey, bval

        self.buf.write(struct_i.pack(len(data)))
        for db, key, value, xt in data:
            bkey = _encode(key)
            if encode_values:
                bval = self.value_encode(value)
            else:
                bval = _encode(value)
            if xt is None:
                xt = EXPIRE
            self.buf.write(struct_dbkvxt.pack(db, len(bkey), len(bval), xt))
            self.buf.write(bkey)
            self.buf.write(bval)
        return self

    cdef RequestBuffer write_key(self, key):
        cdef bytes bkey = _encode(key)
        self.buf.write(struct_i.pack(len(bkey)))
        self.buf.write(bkey)
        return self

    cdef RequestBuffer write_key_list(self, keys):
        cdef bytes bkey
        self.buf.write(struct_i.pack(len(keys)))
        for key in keys:
            bkey = _encode(key)
            self.buf.write(struct_i.pack(len(bkey)))
            self.buf.write(bkey)
        return self

    cdef RequestBuffer write_key_value(self, key, value, encode_value):
        cdef:
            bytes bkey = _encode(key)
            bytes bval

        if encode_value:
            bval = self.value_encode(value)
        else:
            bval = _encode(value)

        self.buf.write(struct_ii.pack(len(bkey), len(bval)))
        self.buf.write(bkey)
        self.buf.write(bval)
        return self

    cdef send_simple(self, data):
        self._socket.send(data)

    cdef send(self):
        self._socket.send(self.buf.getvalue())
        self.buf = io.BytesIO()


@cython.freelist(32)
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

    cdef inline int32_t read_int(self):
        cdef bytes bdata = self._socket.recv(4)
        cdef unsigned char *data = <unsigned char *>bdata
        return (data[0]<<24) + (data[1]<<16) + (data[2]<<8) + data[3]

    cdef inline int64_t read_long(self):
        return struct_q.unpack(self._socket.recv(8))[0]

    cdef double read_double(self):
        cdef long i, m
        i, m = struct_qq.unpack(self._socket.recv(16))
        return i + (m * 1e-12)

    cdef inline bytes read_bytes(self):
        cdef int32_t n = self.read_int()
        if n > 0:
            return self._socket.recv(n)
        return b''

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

        if self._decode_keys:
            for i in range(n):
                accum.append(_decode(self.read_bytes()))
        else:
            for i in range(n):
                accum.append(self.read_bytes())

        return accum

    cdef list read_values(self, decode_values):
        cdef:
            int i
            int n = self.read_int()
            list accum = []

        for i in range(n):
            value = self.read_bytes()
            if decode_values:
                value = self.value_decode(value)
            accum.append(value)

        return accum

    cdef list read_keys_values(self, decode_values):
        cdef:
            list accum = []
            int i, klen, vlen, n_items

        n_items = self.read_int()
        for i in range(n_items):
            klen = self.read_int()
            vlen = self.read_int()
            key = self._socket.recv(klen)
            value = self._socket.recv(vlen)
            if self._decode_keys:
                key = _decode(key)
            if decode_values:
                value = self.value_decode(value)
            accum.append((key, value))
        return accum

    cdef dict read_keys_values_dict(self, decode_values):
        return dict(self.read_keys_values(decode_values))

    cdef list read_keys_values_with_db_expire(self, decode_values):
        cdef:
            int i, klen, vlen, n_items
            list accum = []

        n_items = self.read_int()
        for i in range(n_items):
            db, klen, vlen, xt = struct_dbkvxt.unpack(self._socket.recv(18))
            key = self._socket.recv(klen)
            value = self._socket.recv(vlen)
            if self._decode_keys:
                key = _decode(key)
            if decode_values:
                value = self.value_decode(value)
            accum.append((db, key, value, xt))

        return accum

    cdef dict read_keys_values_with_db_expire_dict(self, decode_values):
        cdef:
            int klen, vlen
            dict accum = {}
            int i, n_items

        n_items = self.read_int()
        for i in range(n_items):
            _, klen, vlen, _ = struct_dbkvxt.unpack(self._socket.recv(18))
            key = self._socket.recv(klen)
            value = self._socket.recv(vlen)
            if self._decode_keys:
                key = _decode(key)
            if decode_values:
                value = self.value_decode(value)
            accum[key] = value

        return accum


cdef class KTResponseHandler(BaseResponseHandler):
    cdef inline int check_error(self, magic) except -1:
        cdef bytes bmagic = self._socket.recv(1)
        if bmagic == magic:
            return 0
        elif bmagic == KT_ERROR:
            raise ProtocolError('Internal server error processing request.')
        else:
            raise ServerError('Unexpected server response: %r' % bmagic)


cdef class TTResponseHandler(BaseResponseHandler):
    cdef inline int check_error(self) except -1:
        cdef bytes bmagic = self._socket.recv(1)
        if bmagic == b'\x00':
            return 0
        elif bmagic == b'\x01':
            return 1
        else:
            raise ServerError('Unexpected server response: %r' % bmagic)


class _ConnectionState(object):
    def __init__(self, **kwargs):
        super(_ConnectionState, self).__init__(**kwargs)
        self.reset()
    def reset(self): self.conn = None
    def set_connection(self, conn): self.conn = conn

class _ConnectionLocal(_ConnectionState, threading.local): pass


cdef class BinaryProtocol(object):
    cdef:
        readonly bint _decode_keys
        readonly str _host
        readonly int _port
        readonly encode_value
        readonly decode_value
        readonly object _timeout
        readonly SocketPool _pool
        public object _state

    def __init__(self, host='127.0.0.1', port=1978, decode_keys=True,
                 encode_value=None, decode_value=None, timeout=None,
                 connection_pool=False):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._decode_keys = decode_keys
        self.encode_value = encode_value or encode
        self.decode_value = decode_value or decode
        self._state = _ConnectionLocal()
        if connection_pool:
            self._pool = SocketPool(host, port, timeout)
        else:
            self._pool = None

    def __del__(self):
        if self._pool is not None:
            self._pool.close_all()

    def is_closed(self):
        return self._state.conn is None

    def close(self, allow_reuse=True):
        if self._state.conn is None: return False

        cdef _Socket conn
        if self._pool is not None:
            if allow_reuse:
                self._pool.checkin()
            else:
                self._pool.close()
        else:
            conn = self._state.conn
            conn.close()

        self._state.reset()
        return True

    def close_all(self):
        if self._pool is None:
            raise ValueError('connection pool is not enabled')

        self.close()
        return self._pool.close_all()

    def close_idle(self, cutoff=60):
        if self._pool is None:
            raise ValueError('connection pool is not enabled')

        n = self._pool.close_idle(cutoff)
        if self._state.conn is None:
            return n

        cdef _Socket conn = self._state.conn
        if conn.is_closed:
            self._state.reset()
        return n

    cpdef connect(self):
        if self._state.conn is not None: return False

        if self._pool is not None:
            self._state.conn = self._pool.checkout()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self._timeout:
                sock.settimeout(self._timeout)
            sock.connect((self._host, self._port))
            self._state.conn = _Socket(sock)
        return True

    cdef _Socket _connection(self):
        self.connect()
        return <_Socket>(self._state.conn)

    def serialize_list(self, data):
        return _serialize_list(data)

    def deserialize_list(self, data, decode_values=True):
        return _deserialize_list(data, decode_values)

    def serialize_dict(self, data):
        return _serialize_dict(data)

    def deserialize_dict(self, data, decode_values=True):
        return _deserialize_dict(data, decode_values)

    cdef RequestBuffer request(self):
        if self._state.conn is None:
            self.connect()
        return RequestBuffer(<_Socket>(self._state.conn), self.encode_value)


cdef class KTBinaryProtocol(BinaryProtocol):
    cdef:
        public int default_db

    def __init__(self, *args, **kwargs):
        self.default_db = kwargs.pop('default_db', 0) or 0
        super(KTBinaryProtocol, self).__init__(*args, **kwargs)

    def set_database(self, db):
        self.default_db = db

    cdef KTResponseHandler response(self):
        if self._state.conn is None:
            self.connect()
        return KTResponseHandler(
            <_Socket>(self._state.conn),
            self._decode_keys,
            self.decode_value)

    cdef KTResponseHandler _get_bulk(self, keys, db, flat):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response

        if db is None:
            db = self.default_db

        request.write_magic(KT_GET_BULK).write_int(0)
        if flat:
            request.write_key_list_with_db(keys, db)
        else:
            request.write_db_key_list(keys)
        request.send()

        response = self.response()
        response.check_error(KT_GET_BULK)
        return response

    def get_bulk(self, keys, db=None, decode_values=True):
        """
        Get multiple key/value pairs in a single request.

        :param list keys: a flat list of keys
        :param int db: db index
        :param bint decode_values: deserialize values after reading
        :return: a dict of key, value for matching records
        """
        cdef KTResponseHandler resp = self._get_bulk(keys, db, True)
        return resp.read_keys_values_with_db_expire_dict(decode_values)

    def get_bulk_details(self, keys, db=None, decode_values=True):
        """
        Get multiple key/value pairs in a single request.

        :param list keys: a flat list of keys
        :param int db: db index
        :param bint decode_values: deserialize values after reading
        :return: a list of (db, key, value, expire_time) tuples.
        """
        cdef KTResponseHandler resp = self._get_bulk(keys, db, True)
        return resp.read_keys_values_with_db_expire(decode_values)

    def get_bulk_raw(self, db_key_list, decode_values=True):
        """
        Get multiple key/value pairs in a single request.

        :param list db_key_list: a list of (db, key) tuples
        :param int db: db index
        :param bint decode_values: deserialize values after reading
        :return: a dict of key, value for matching records
        """
        cdef KTResponseHandler resp = self._get_bulk(db_key_list, None, False)
        return resp.read_keys_values_with_db_expire_dict(decode_values)

    def get_bulk_raw_details(self, db_key_list, decode_values=True):
        """
        Get multiple key/value pairs in a single request.

        :param list db_key_list: a list of (db, key) tuples
        :param int db: db index
        :param bint decode_values: decode values
        :return: a list of (db, key, value, expire_time) tuples.
        """
        cdef KTResponseHandler resp = self._get_bulk(db_key_list, None, False)
        return resp.read_keys_values_with_db_expire(decode_values)

    def get(self, key, db=None, decode_value=True):
        """
        Get the value associated with the given key.

        :param key: key to retrieve
        :param int db: db index
        :param bint decode_value: deserialize values after reading
        :return: value or None if not found
        """
        result = self.get_bulk_details((key,), db, decode_value)
        if result:
            return result[0][2]  # [(db, key, VALUE, xt)].

    cdef _set_bulk(self, data, db, expire_time, no_reply, encode_values,
                   as_dict):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response
            int flags = KT_NOREPLY if no_reply else 0

        if db is None:
            db = self.default_db

        request.write_magic(KT_SET_BULK).write_int(flags)

        if as_dict:
            # data is {k0: v0, k1: v1...}
            request.write_key_value_list_with_db_expire(
                data,
                db,
                expire_time or EXPIRE,
                encode_values)
        else:
            # data is [(db0, k0, v0, xt0), (db1, k1, v1, xt1)...]
            request.write_db_key_value_expire_list(data, encode_values)

        request.send()
        if not no_reply:
            response = self.response()
            response.check_error(KT_SET_BULK)
            return response.read_int()

    def set_bulk(self, data, db, expire_time, no_reply=False,
                 encode_values=True):
        """
        Set multiple key, value pairs in a single request.

        :param dict data: mapping of key to value
        :param int db: db index
        :param long expire_time: expire time in seconds from now
        :param bint no_reply: ignore reply
        :param bint encode_values: serialize values before writing
        :return: number of records written
        """
        return self._set_bulk(data, db, expire_time, no_reply, encode_values,
                              True)

    def set_bulk_raw(self, data, no_reply, encode_values):
        """
        Set multiple key, value pairs in a single request.

        :param list data: a list of (db, key, value, expire_time) tuples
        :param bint no_reply: ignore reply
        :param bint encode_values: serialize values before writing
        :return: number of records written
        """
        return self._set_bulk(data, None, None, no_reply, encode_values, False)

    def set(self, key, value, db, expire_time, no_reply=False,
            encode_value=True):
        """
        Store value at the given key.

        :param key: key to write
        :param value: data to store at key
        :param int db: db index
        :param long expire_time: expire time in seconds from now
        :param bint no_reply: ignore reply
        :param bint encode_value: serialize value before writing
        :return: number of records written (1)
        """
        return self._set_bulk({key: value}, db, expire_time, no_reply,
                              encode_value, True)

    cdef _remove_bulk(self, keys, db, no_reply, flat):
        cdef:
            RequestBuffer request = self.request()
            KTResponseHandler response
            int flags = KT_NOREPLY if no_reply else 0

        if db is None:
            db = self.default_db

        request.write_magic(KT_REMOVE_BULK).write_int(flags)
        if flat:
            # [k0, k1...]
            request.write_key_list_with_db(keys, db)
        else:
            # [(db0, k0), (db1, k1)...]
            request.write_db_key_list(keys)
        request.send()

        if not no_reply:
            response = self.response()
            response.check_error(KT_REMOVE_BULK)
            return response.read_int()

    def remove_bulk(self, keys, db, no_reply=False):
        """
        Remove multiple keys in a single request.

        :param list keys: list of keys
        :param int db: db index
        :param bint no_reply: ignore reply
        :return: number of records removed
        """
        return self._remove_bulk(keys, db, no_reply, True)

    def remove_bulk_raw(self, db_key_list, no_reply=False):
        """
        Remove multiple keys in a single request.

        :param list db_key_list: list of (db, key) tuples
        :param bint no_reply: ignore reply
        :return: number of records removed
        """
        return self._remove_bulk(db_key_list, None, no_reply, False)

    def remove(self, key, db, no_reply=False):
        """
        Remove a single key from the database.

        :param key: key to remove
        :param int db: db index
        :param bint no_reply: ignore reply
        :return: number of records removed
        """
        return self._remove_bulk((key,), db, no_reply, True)

    def script(self, name, data=None, no_reply=False, encode_values=True,
               decode_values=True):
        """
        Evaluate a lua script.

        :param name: script function name
        :param dict data: dictionary of key, value pairs, passed as arguments
        :param bint no_reply: ignore reply
        :param bint encode_values: serialize values before sending to db
        :param bint decode_values: deserialize values after reading result
        :return: dictionary of key, value pairs returned by function
        """
        cdef:
            bytes bname = _encode(name)
            bytes bkey, bval
            int flags = KT_NOREPLY if no_reply else 0
            RequestBuffer request = self.request()
            KTResponseHandler response

        data = data or {}
        (request
         .write_magic(KT_PLAY_SCRIPT)
         .write_bytes(struct_iii.pack(flags, len(bname), len(data)), False)
         .write_bytes(bname, False))

        for key in data:
            bkey = _encode(key)
            if encode_values:
                bval = self.encode_value(data[key])
            else:
                bval = _encode(data[key])
            (request
             .write_ii(len(bkey), len(bval))
             .write_bytes(bkey, False)
             .write_bytes(bval, False))

        request.send()

        if flags & KT_NOREPLY:
            return

        response = self.response()
        response.check_error(KT_PLAY_SCRIPT)
        return response.read_keys_values_dict(decode_values)


struct_2si = struct.Struct('>2sI')
struct_2sii = struct.Struct('>2sII')
struct_2siii = struct.Struct('>2sIII')
struct_2siiii = struct.Struct('>2sIIII')


cdef class TTBinaryProtocol(BinaryProtocol):
    cdef TTResponseHandler response(self):
        if self._state.conn is None:
            self.connect()
        return TTResponseHandler(
            <_Socket>(self._state.conn),
            self._decode_keys,
            self.decode_value)

    cdef _key_value_cmd(self, key, value, bytes bmagic, encode_value):
        cdef:
            bytes bkey, bval
            RequestBuffer request = self.request()

        bkey = _encode(key)
        if encode_value:
            bval = self.encode_value(value)
        else:
            bval = _encode(value)

        request.send_simple(struct_2sii.pack(bmagic, len(bkey), len(bval)) +
                            bkey + bval)
        return self.response().check_error()

    def put(self, key, value, encode_value=True):
        return self._key_value_cmd(key, value, b'\xc8\x10', encode_value) == 0

    def putkeep(self, key, value, encode_value=True):
        return self._key_value_cmd(key, value, b'\xc8\x11', encode_value) == 0

    def putcat(self, key, value, encode_value=True):
        return self._key_value_cmd(key, value, b'\xc8\x12', encode_value) == 0

    def putshl(self, key, value, width, encode_value=True):
        cdef:
            bytes bkey = _encode(key)
            bytes bval
            RequestBuffer request = self.request()

        if encode_value:
            bval = self.encode_value(value)
        else:
            bval = _encode(value)

        request.send_simple(
            struct_2siii.pack(b'\xc8\x13', len(bkey), len(bval), width) +
            bkey + bval)
        return self.response().check_error() == 0

    def putnr(self, key, value, encode_value=True):
        cdef:
            bytes bkey = _encode(key)
            bytes bval
            RequestBuffer request = self.request()

        if encode_value:
            bval = self.encode_value(value)
        else:
            bval = _encode(value)

        request.send_simple(struct_2sii.pack(b'\xc8\x18', len(bkey), len(bval))
                            + bkey + bval)

    def putnr_bulk(self, data, encode_values=True):
        cdef RequestBuffer request = self.request()
        for key, value in data.items():
            (request
             .write_magic(b'\xc8\x18')
             .write_key_value(key, value, encode_values))
        request.send()

    cdef _simple_key_command(self, bytes bmagic, key):
        cdef:
            bytes bkey = _encode(key)
            RequestBuffer request = self.request()
        request.send_simple(struct_2si.pack(bmagic, len(bkey)) + bkey)

    def seize(self, key, decode_value=True):
        cdef:
            bytes bkey = _encode(key)
            RequestBuffer request = self.request()
            TTResponseHandler response

        (request
         .write_magic(b'\xc8\x30')  # Get.
         .write_bytes(bkey, True)
         .write_magic(b'\xc8\x20')  # Out.
         .write_bytes(bkey, True)
         .send())

        response = self.response()
        value = None
        if not response.check_error():
            value = response.read_bytes()
            if decode_value:
                value = self.decode_value(value)
        if not response.check_error():
            return value

    def out(self, key):
        self._simple_key_command(b'\xc8\x20', key)
        return 0 if self.response().check_error() else 1

    def get(self, key, decode_value=True):
        cdef TTResponseHandler response

        self._simple_key_command(b'\xc8\x30', key)
        response = self.response()
        if not response.check_error():
            bdata = response.read_bytes()
            if decode_value:
                return self.decode_value(bdata)
            else:
                return bdata

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
            return response.read_keys_values_dict(decode_values)

    def vsiz(self, key):
        cdef TTResponseHandler response
        self._simple_key_command(b'\xc8\x38', key)
        response = self.response()
        if not response.check_error():
            return response.read_int()

    cdef _simple_command(self, bytes bmagic):
        cdef RequestBuffer request = self.request()
        request.send_simple(bmagic)
        return self.response().check_error() == 0

    def iterinit(self):
        return self._simple_command(b'\xc8\x50')

    def iternext(self):
        cdef:
            RequestBuffer request = self.request()
            TTResponseHandler response

        request.send_simple(b'\xc8\x51')
        response = self.response()
        if not response.check_error():
            return response.read_key()

    def fwmkeys(self, prefix, max_keys=None):
        cdef:
            bytes bprefix = _encode(prefix)
            RequestBuffer request = self.request()
            TTResponseHandler response

        if max_keys is None:
            max_keys = (1 << 32) - 1

        # fwmkeys method.
        (request
         .write_magic(b'\xc8\x58')
         .write_ii(len(bprefix), max_keys)
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
         .write_ii(len(bkey), value)
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
            lock_all=False, encode_value=True, decode_value=False):
        cdef:
            bytes bname = _encode(name)
            bytes bkey = _encode(key or b'')
            bytes bval
            int opts = 0
            RequestBuffer request = self.request()
            TTResponseHandler response

        if encode_value:
            bval = self.encode_value(value or '')
        else:
            bval = _encode(value or b'')

        if lock_records and lock_all:
            raise ValueError('cannot specify both record and global locking.')

        if lock_records:
            opts = 1
        elif lock_all:
            opts = 2

        (request
         .write_bytes(struct_2siiii.pack(b'\xc8\x68', len(bname), opts,
                                         len(bkey), len(bval)), False)
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
        if decode_value:
            return self.decode_value(resp)
        return resp

    def sync(self):
        return self._simple_command(b'\xc8\x70')

    def optimize(self, options):
        self._simple_key_command(b'\xc8\x71', options)
        return self.response().check_error() == 0

    def vanish(self):
        return self._simple_command(b'\xc8\x72')

    def copy(self, path):
        self._simple_key_command(b'\xc8\x73', path)
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
         .write_ii(len(bhost), port)
         .write_long(timestamp)
         .write_int(opts)
         .write_bytes(bhost, False)
         .send())
        return self.response().check_error() == 0

    def _long_cmd(self, bytes bmagic):
        cdef TTResponseHandler response
        self.request().send_simple(bmagic)
        response = self.response()
        if not response.check_error():
            return response.read_long()

    def rnum(self):
        return self._long_cmd(b'\xc8\x80')

    def size(self):
        return self._long_cmd(b'\xc8\x81')

    def stat(self):
        cdef TTResponseHandler response
        self.request().send_simple(b'\xc8\x88')
        response = self.response()
        if not response.check_error():
            return response.read_bytes()

    cpdef misc(self, proc, args, update_log, decode_values=False):
        cdef:
            bytes bprocname = _encode(proc)
            int opts = 1 if update_log else 0
            RequestBuffer request = self.request()

        if args is None:
            args = ()

        pfx = struct_2siii.pack(b'\xc8\x90', len(bprocname), opts, len(args))
        request.write_bytes(pfx, False).write_bytes(bprocname, False)
        for arg in args:
            request.write_bytes(_encode(arg), True)

        request.send()
        response = self.response()
        success = self.check_error() == 0
        return success, response.read_values(decode_values)

    cdef _misc_kv(self, cmd, key, value, update_log, encode_value):
        cdef:
            bytes bkey = _encode(key)
            bytes bval
        if encode_value:
            bval = self.encode_value(value)
        else:
            bval = _encode(value)
        ok, _ = self.misc(cmd, (bkey, bval), update_log)
        return ok

    def misc_put(self, key, value, update_log=True, encode_value=True):
        return self._misc_kv('put', key, value, update_log, encode_value)

    def misc_putkeep(self, key, value, update_log=True, encode_value=True):
        return self._misc_kv('putkeep', key, value, update_log, encode_value)

    def misc_putcat(self, key, value, update_log=True, encode_value=True):
        return self._misc_kv('putcat', key, value, update_log, encode_value)

    def misc_putdup(self, key, value, update_log=True, encode_value=True):
        return self._misc_kv('putdup', key, value, update_log, encode_value)

    def misc_putdupback(self, key, value, update_log=True, encode_value=True):
        return self._misc_kv('putdupback', key, value, update_log,
                             encode_value)

    def misc_out(self, key, update_log=True):
        ok, _ = self._misc('out', [_encode(key)], update_log)
        return ok

    def misc_get(self, key, decode_value=True):
        ok, data = self.misc('get', [_encode(key)], True, decode_value)
        if ok and data:
            return data[0]

    def misc_putlist(self, data, update_log=True, encode_values=True):
        cdef list accum = []
        for key, value in data.items():
            accum.append(_encode(key))
            if encode_values:
                accum.append(self.encode_value(value))
            else:
                accum.append(_encode(value))
        ok, _ = self.misc('putlist', accum, update_log)
        return ok

    def _misc_key_list(self, cmd, keys, update_log):
        cdef list accum = [_encode(key) for key in keys]
        return self.misc(cmd, accum, update_log)

    def misc_outlist(self, keys, update_log=True):
        ok, _ = self._misc_key_list('outlist', keys, update_log)
        return ok

    def _misc_list_of_items(self, list data, decode_values):
        if not data: return []

        cdef:
            list accum = []
            int i = 0, l = len(data)

        while i < l:
            key = data[i]
            value = data[i + 1]
            if self._decode_keys:
                key = _decode(key)
            if decode_values:
                value = self.decode_value(value)
            accum.append((key, value))
            i += 2
        return accum

    cdef _misc_list_to_dict(self, list data, decode_values):
        if not data: return {}

        cdef:
            dict accum = {}
            int i = 0, l = len(data)

        while i < l:
            key = data[i]
            value = data[i + 1]
            if self._decode_keys:
                key = _decode(key)
            if decode_values:
                value = self.decode_value(value)
            accum[key] = value
            i += 2
        return accum

    def misc_getlist(self, keys, decode_values=True):
        ok, data = self._misc_key_list('getlist', keys, False, False)
        return self._misc_list_to_dict(data, decode_values)

    def misc_getpart(self, key, start=0, length=None, decode_value=True):
        args = [key, str(start)]
        if length is not None:
            args.append(str(length))
        ok, result = self.misc('getpart', args, False, decode_value)
        if ok and result:
            return result[0]

    def misc_iterinit(self, key=None):
        ok, _ = self._misc('iterinit', [key] if key else [], False)
        return ok

    def misc_iternext(self, decode_value=True):
        ok, data = self.misc('iternext', [], False, False)
        if ok and data:
            key, value = data
            if self._decode_keys:
                key = _decode(key)
            if decode_value:
                value = self.decode_value(value)
            return (key, value)

    def misc_sync(self):
        return self.misc('sync', [], True, False)[0]

    def misc_optimize(self, opts, update_log=True):
        ok, _ = self.misc('optimize', [opts], update_log, False)
        return ok

    def misc_vanish(self, update_log=True):
        return self.misc('vanish', [], update_log, False)[0]

    def misc_error(self):
        ok, data = self.misc('error', [], False, False)
        if ok and data:
            return _decode(data[0])

    def misc_cacheclear(self):
        return self.misc('cacheclear', [], False, False)[0]

    def misc_defragment(self, nsteps=None, update_log=True):
        args = [str(nsteps)] if nsteps is not None else []
        return self.misc('defrag', args, update_log, False)[0]

    def misc_regex(self, regex, max_records=None, decode_values=True):
        args = [regex]
        if max_records is not None:
            args.append(str(max_records))
        ok, data = self.misc('regex', args, False, False)
        return self._misc_list_to_dict(data, decode_values)

    def misc_regexlist(self, regex, max_records=None, decode_values=True):
        args = [regex]
        if max_records is not None:
            args.append(str(max_records))
        ok, data = self.misc('regex', args, False, False)
        return self._misc_list_of_lists(data, decode_values)

    def misc_range(self, start, stop=None, max_records=0, decode_values=True):
        args = [start, str(max_records)]
        if stop is not None:
            args.append(stop)
        ok, data = self.misc('range', args, False, False)
        return self._misc_list_to_dict(data, decode_values)

    def misc_rangelist(self, start, stop=None, max_records=0,
                       decode_values=True):
        args = [start, str(max_records)]
        if stop is not None:
            args.append(stop)
        ok, data = self.misc('range', args, False, False)
        return self._misc_list_of_lists(data)

    def misc_setindex(self, column, index_type, update_log=True):
        args = [column, str(index_type)]
        return self.misc('setindex', args, update_log, False)[0]

    def misc_search(self, conditions, cmd=None, update_log=True):
        if cmd is not None:
            conditions.append(_encode(cmd))

        ok, data = self.misc('search', conditions, update_log, False)
        if cmd is None:
            return [_decode(key) for key in data]
        elif cmd == 'get':
            accum = []
            for item in data:
                key, rest = item[1:].split(b'\x00', 1)
                accum.append((_decode(key), rest))
            return accum
        elif cmd == 'count':
            return int(data[0])
        elif len(data) == 0:
            return ok
        else:
            raise ProtocolError('Unexpected results for search cmd=%s' % cmd)

    def misc_genuid(self, update_log=True):
        ok, data = self.misc('genuid', [], update_log)
        return int(data[0])

    def keys(self):
        cdef RequestBuffer request = self.request()
        cdef TTResponseHandler response

        # iterinit method.
        request.send_simple(b'\xc8\x50')
        response = self.response()
        if response.check_error():
            return []

        while True:
            # iternext method.
            request.send_simple(b'\xc8\x51')
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
        buf.write(_encode(key))
        buf.write(b'\x00')
        buf.write(_encode(value))
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
