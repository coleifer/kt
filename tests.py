import functools
import os
import sys
import threading
import unittest
import warnings

try:
    import msgpack
except ImportError:
    msgpack = None

from kt import EmbeddedServer
from kt import EmbeddedTokyoTyrantServer
from kt import KT_BINARY
from kt import KT_MSGPACK
from kt import KT_JSON
from kt import KT_NONE
from kt import KT_PICKLE
from kt import KyotoTycoon
from kt import QueryBuilder
from kt import TokyoTyrant
from kt import TT_TABLE
from kt import constants


class BaseTestCase(unittest.TestCase):
    _server = None
    db = None
    server = None
    server_kwargs = None

    @classmethod
    def setUpClass(cls):
        if cls.server is None:
            return

        if sys.version_info[0] > 2:
            warnings.filterwarnings(action='ignore', message='unclosed',
                                    category=ResourceWarning)

        kwargs = {'quiet': True}
        if cls.server_kwargs:
            kwargs.update(cls.server_kwargs)
        cls._server = cls.server(**kwargs)
        cls._server.run()
        cls.db = cls._server.client

    @classmethod
    def tearDownClass(cls):
        if cls._server is not None:
            cls._server.stop()
            cls.db.close()
            cls.db = None

    def tearDown(self):
        if self.db is not None:
            self.db.clear()

    @classmethod
    def get_embedded_server(cls):
        if self.server is None:
            raise NotImplementedError


class KyotoTycoonTests(object):
    def test_basic_operations(self):
        """
        Test operations of the KyotoTycoon client.

        This class wraps two protocol handlers - binary and HTTP protocols. The
        interface exposes a super-set of the methods available, preferring the
        bulk/binary APIs where possible.

        Note: protocols are also tested individually.
        """
        self.assertEqual(len(self.db), 0)

        # Test basic set and get.
        self.db.set('k1', 'v1')
        self.assertEqual(self.db.get('k1'), 'v1')
        self.assertTrue(self.db.get('kx') is None)

        # Test setting bulk data returns records set.
        nkeys = self.db.set_bulk({'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(nkeys, 3)

        # Test getting bulk data returns dict of just existing keys.
        self.assertEqual(self.db.get_bulk(['k1', 'k2', 'k3', 'kx']),
                         {'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})

        # Test removing a record returns number of rows removed.
        self.assertEqual(self.db.remove('k1'), 1)
        self.assertEqual(self.db.remove('k1'), 0)

        self.db['k1'] = 'v1'
        self.assertEqual(self.db.remove_bulk(['k1', 'k3', 'kx']), 2)
        self.assertEqual(self.db.remove_bulk([]), 0)
        self.assertEqual(self.db.remove_bulk(['k2']), 1)

        self.db.append('key', 'abc')
        self.db.append('key', 'def')
        self.assertEqual(self.db['key'], 'abcdef')

        # Test atomic replace and pop.
        self.assertTrue(self.db.replace('key', 'xyz'))
        self.assertEqual(self.db.seize('key'), 'xyz')
        self.assertFalse(self.db.seize('key'))
        self.assertFalse(self.db.replace('key', 'abc'))
        self.assertTrue(self.db.add('key', 'foo'))
        self.assertFalse(self.db.add('key', 'bar'))
        self.assertEqual(self.db['key'], 'foo')

        # Test compare-and-swap.
        self.assertTrue(self.db.cas('key', 'foo', 'baz'))
        self.assertFalse(self.db.cas('key', 'foo', 'bar'))
        self.assertEqual(self.db['key'], 'baz')

        # Test dict interface.
        self.assertTrue('key' in self.db)
        self.assertFalse('other' in self.db)
        self.assertEqual(len(self.db), 1)
        self.assertEqual(self.db.count(), 1)
        self.db['k1'] = 'v1'
        self.db.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(self.db.pop('k1'), 'v1')
        self.assertTrue(self.db.pop('k1') is None)
        self.assertEqual(sorted(list(self.db)), ['k2', 'k3', 'key'])
        del self.db['k3']
        self.assertEqual(sorted(list(self.db.keys())), ['k2', 'key'])
        self.assertEqual(sorted(list(self.db.values())), ['baz', 'v2'])
        self.assertEqual(sorted(list(self.db.items())),
                         [('k2', 'v2'), ('key', 'baz')])

        # Test matching.
        self.assertEqual(sorted(self.db.match_prefix('k')), ['k2', 'key'])
        self.assertEqual(self.db.match_regex('k[0-9]'), ['k2'])
        self.assertEqual(self.db.match_regex('x\d'), [])
        self.assertEqual(self.db.match_similar('k'), ['k2'])
        self.assertEqual(sorted(self.db.match_similar('k', 2)), ['k2', 'key'])

        # Test numeric operations.
        self.assertEqual(self.db.incr('n'), 1)
        self.assertEqual(self.db.incr('n', 3), 4)
        self.assertEqual(self.db.incr_double('nd'), 1.)
        self.assertEqual(self.db.incr_double('nd', 2.5), 3.5)

    def test_get_raw(self):
        self.db['k1'] = b'v1'
        self.db['k2'] = b'\xff\x00\xff'
        self.assertEqual(self.db.get_raw('k1'), b'v1')
        self.assertEqual(self.db.get_raw('k2'), b'\xff\x00\xff')
        self.assertEqual(self.db.get_bulk_raw(['k1', 'k2']), {
            'k1': b'v1', 'k2': b'\xff\x00\xff'})

    def test_large_read_write(self):
        long_str = 'a' * (1024 * 1024 * 32)  # 32MB string.
        self.db['key'] = long_str
        self.assertEqual(self.db['key'], long_str)
        del self.db['key']
        self.assertEqual(len(self.db), 0)

    def test_protocol_binary(self):
        self._test_protocol(self.db._protocol)

    def test_protocol_http(self):
        self._test_protocol(self.db._protocol_http)

    def _test_protocol(self, p):
        # Both protocols support some basic methods, which we will test (namely
        # get/set/remove and their bulk equivalents).
        self.assertEqual(self.db.count(), 0)

        # Test basic set and get.
        p.set('k1', 'v1', 0, None)
        self.assertEqual(p.get('k1', 0), 'v1')
        self.assertTrue(p.get('kx', 0) is None)

        # Test setting bulk data returns records set.
        nkeys = p.set_bulk({'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'}, 0, None)
        self.assertEqual(nkeys, 3)

        # Test getting bulk data returns dict of just existing keys.
        self.assertEqual(p.get_bulk(['k1', 'k2', 'k3', 'kx'], 0),
                         {'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})

        # Test removing a record returns number of rows removed.
        self.assertEqual(p.remove('k1', 0), 1)
        self.assertEqual(p.remove('k1', 0), 0)

        p.set('k1', 'v1', 0, None)
        self.assertEqual(p.remove_bulk(['k1', 'k3', 'kx'], 0), 2)
        self.assertEqual(p.remove_bulk([], 0), 0)
        self.assertEqual(p.remove_bulk(['k2'], 0), 1)

    def test_http_protocol_special(self):
        p = self.db._protocol_http
        p.append('key', 'abc', 0, None)
        p.append('key', 'def', 0, None)
        self.assertEqual(p.get('key', 0), 'abcdef')

        # Test atomic replace and pop.
        self.assertTrue(p.replace('key', 'xyz', 0, None))
        self.assertEqual(p.seize('key', 0), 'xyz')
        self.assertFalse(p.seize('key', 0))
        self.assertFalse(p.replace('key', 'abc', 0, None))
        self.assertTrue(p.add('key', 'foo', 0, None))
        self.assertFalse(p.add('key', 'bar', 0, None))
        self.assertEqual(p.get('key', 0), 'foo')

        # Test compare-and-swap.
        self.assertTrue(p.cas('key', 'foo', 'baz', 0, None))
        self.assertFalse(p.cas('key', 'foo', 'bar', 0, None))
        self.assertEqual(p.get('key', 0), 'baz')

        self.assertTrue(p.check('key', 0))
        self.assertFalse(p.check('other', 0))
        self.assertEqual(p.count(), 1)

        # Test numeric operations.
        self.assertEqual(p.increment('n'), 1)
        self.assertEqual(p.increment('n', 3, 0, None), 4)
        self.assertEqual(p.increment_double('nd'), 1.)
        self.assertEqual(p.increment_double('nd', 2.5, 0, None), 3.5)

        # Flush db.
        p.clear()

        # Test bulk operations with and without atomic.
        for atomic in (False, True):
            accum = {}
            keys = []
            for i in range(100):
                accum['k%064d' % i] = '%01024d' % i
                keys.append('k%064d' % i)

            self.assertEqual(p.set_bulk(accum, 0, None, atomic=atomic), 100)
            resp = p.get_bulk(keys, 0, atomic=atomic)
            self.assertEqual(resp, accum)
            self.assertEqual(p.remove_bulk(keys, 0, atomic=atomic), 100)

        # Set some data for matching tests.
        p.set_bulk(dict(('k%04d' % i, 'v%01024d' % i) for i in range(100)), 0)
        keys = ['k%04d' % i for i in range(100)]

        # Test matching.
        self.assertEqual(sorted(p.match_prefix('k')), keys)
        self.assertEqual(sorted(p.match_regex('k00[25]3')), ['k0023', 'k0053'])
        self.assertEqual(p.match_regex('x\d'), [])
        self.assertEqual(p.match_similar('k0022'), [
            'k0022',  # Exact match is always first, regardless of storage.
            'k0002', 'k0012',
            'k0020', 'k0021', 'k0023', 'k0024', 'k0025', 'k0026', 'k0027',
            'k0028', 'k0029', 'k0032', 'k0042', 'k0052', 'k0062', 'k0072',
            'k0082', 'k0092'])


class TestKyotoTycoonHash(KyotoTycoonTests, BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '*'}


class TestKyotoTycoonBTree(KyotoTycoonTests, BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '%'}


class TestKyotoTycoonCursor(BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '%'}

    def setUp(self):
        super(TestKyotoTycoonCursor, self).setUp()
        self.db.update({'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4'})

    def test_multiple_cursors(self):
        c1 = self.db.cursor()
        c2 = self.db.cursor()
        c3 = self.db.cursor()
        self.assertTrue(c1.jump('k1'))
        self.assertTrue(c2.jump('k2'))
        self.assertTrue(c3.jump('k3'))
        self.assertEqual(c1.get(), ('k1', 'v1'))
        self.assertEqual(c2.get(), ('k2', 'v2'))
        self.assertEqual(c3.get(), ('k3', 'v3'))

        self.assertTrue(c1.step())
        self.assertEqual(c1.get(), ('k2', 'v2'))
        self.assertEqual(c1.seize(), ('k2', 'v2'))
        self.assertEqual(c2.get(), ('k3', 'v3'))
        self.assertEqual(c2.seize(), ('k3', 'v3'))
        for c in (c1, c2, c3):
            self.assertEqual(c.get(), ('k4', 'v4'))
        self.assertTrue(c3.remove())
        for c in (c1, c2, c3):
            self.assertTrue(c.get() is None)

        c1.jump()
        self.assertEqual(c1.get(), ('k1', 'v1'))
        self.assertTrue(c1.remove())
        self.assertFalse(c2.jump())

    def test_cursor_movement(self):
        cursor = self.db.cursor()
        self.assertEqual(list(cursor), [('k1', 'v1'), ('k2', 'v2'),
                                        ('k3', 'v3'), ('k4', 'v4')])

        # Jumping in-between moves to closest without going under.
        self.assertTrue(cursor.jump('k1x'))
        self.assertEqual(cursor.key(), 'k2')
        self.assertEqual(cursor.value(), 'v2')

        # Jumping backwards in-between moves to closest while going over.
        self.assertTrue(cursor.jump_back('k2x'))
        self.assertEqual(cursor.key(), 'k2')
        self.assertEqual(cursor.value(), 'v2')

        # We cannot jump past the last record, but we can jump below the first.
        # Similarly, we can't step_back prior to the first record.
        self.assertFalse(cursor.jump('k5'))
        self.assertTrue(cursor.jump('k0'))
        self.assertEqual(cursor.key(), 'k1')
        self.assertFalse(cursor.step_back())

        # We cannot jump_back prior to the first record, but we can jump_back
        # from after the last. Similarly, we can't step past the last record.
        self.assertFalse(cursor.jump_back('k0'))
        self.assertTrue(cursor.jump_back('k5'))
        self.assertEqual(cursor.key(), 'k4')
        self.assertFalse(cursor.step())

    def test_cursor_write(self):
        cursor = self.db.cursor()
        cursor.jump('k2')

        self.assertTrue(cursor.set_value('v2-x'))
        self.assertEqual(cursor.get(), ('k2', 'v2-x'))
        self.assertEqual(self.db['k2'], 'v2-x')
        self.assertTrue(cursor.remove())
        self.assertEqual(cursor.get(), ('k3', 'v3'))
        self.assertFalse('k2' in self.db)
        self.assertTrue(cursor.step_back())
        self.assertEqual(cursor.get(), ('k1', 'v1'))

        self.assertEqual(cursor.seize(), ('k1', 'v1'))
        self.assertTrue(cursor.seize() is None)
        self.assertFalse(cursor.is_valid())

        self.assertTrue(cursor.jump())
        self.assertEqual(cursor.seize(), ('k3', 'v3'))
        self.assertTrue(cursor.jump_back())
        self.assertEqual(cursor.get(), ('k4', 'v4'))

        self.assertEqual(list(cursor), [('k4', 'v4')])
        self.assertEqual(list(cursor), [('k4', 'v4')])
        self.assertTrue(cursor.jump_back())
        self.assertTrue(cursor.remove())
        self.assertEqual(list(cursor), [])

    def test_implicit_cursor_operations(self):
        self.assertEqual(list(self.db.keys()), ['k1', 'k2', 'k3', 'k4'])
        self.assertEqual(list(self.db.values()), ['v1', 'v2', 'v3', 'v4'])
        self.assertEqual(list(self.db.items()), [
            ('k1', 'v1'),
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k4', 'v4')])

        # Nonlazy.
        self.assertEqual(self.db.keys_nonlazy(), ['k1', 'k2', 'k3', 'k4'])


class TestKyotoTycoonSerializers(BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '*'}

    def get_client(self, serializer):
        return KyotoTycoon(self._server._host, self._server._port, serializer)

    def test_serializer_binary(self):
        db = self.get_client(KT_BINARY)
        db.set('k1', 'v1')
        db.set('k2', b'\xe1\x80\x80')
        self.assertEqual(db.get('k1'), 'v1')
        self.assertEqual(db.get('k2'), u'\u1000')
        self.assertEqual(db.get_bulk(['k1', 'k2']),
                         {'k1': 'v1', 'k2': u'\u1000'})

    def _test_serializer_object(self, serializer):
        db = self.get_client(serializer)

        obj = {'w': {'wk': 'wv'}, 'x': 0, 'y': ['aa', 'bb'], 'z': None}
        db.set('k1', obj)
        self.assertEqual(db.get('k1'), obj)

        db.set('k2', '')
        self.assertEqual(db.get('k2'), '')

        self.assertEqual(db.get_bulk(['k1', 'k2']), {'k1': obj, 'k2': ''})

    def test_serializer_json(self):
        self._test_serializer_object(KT_JSON)

    def test_serializer_pickle(self):
        self._test_serializer_object(KT_PICKLE)

    def test_serializer_none(self):
        db = self.get_client(KT_NONE)
        db.set('k1', b'v1')
        self.assertEqual(self.db.get('k1'), 'v1')

        db[b'k2'] = b'v2'
        self.assertEqual(self.db.get_bulk([b'k1', b'k2']),
                         {'k1': 'v1', 'k2': 'v2'})

    @unittest.skipIf(msgpack is None, 'msgpack-python not installed')
    def test_serializer_msgpack(self):
        db = self.get_client(KT_MSGPACK)

        obj = {'w': {'wk': 'wv'}, 'x': 0, 'y': ['aa', 'bb'], 'z': None}
        db.set('k1', obj)
        self.assertEqual(db.get('k1'), {b'w': {b'wk': b'wv'}, b'x': 0,
                                        b'y': [b'aa', b'bb'], b'z': None})

        db.set('k2', '')
        self.assertEqual(db.get('k2'), b'')


class TestKyotoTycoonScripting(BaseTestCase):
    lua_script = os.path.join(os.path.dirname(__file__), 'kt/scripts/kt.lua')
    server = EmbeddedServer
    server_kwargs = {
        'database': '%',
        'server_args': ['-scr', lua_script]}

    def test_script_set(self):
        L = self.db.lua

        # Test adding a single item.
        self.assertEqual(L.sadd(key='s1', value='foo'), {'num': '1'})
        self.assertEqual(L.sadd(key='s1', value='foo'), {'num': '0'})

        # Test adding multiple items.
        items = b'\x01'.join([b'bar', b'baz', b'nug'])
        self.assertEqual(L.sadd(key='s1', value=items), {'num': '3'})

        # Test get cardinality.
        self.assertEqual(L.scard(key='s1'), {'num': '4'})

        # Test membership.
        self.assertEqual(L.sismember(key='s1', value='bar'), {'num': '1'})
        self.assertEqual(L.sismember(key='s1', value='baze'), {'num': '0'})

        keys = ['bar', 'baz', 'foo', 'nug']

        # Test get members.
        self.assertEqual(L.smembers(key='s1'), dict((k, '1') for k in keys))

        # Test pop.
        res = L.spop(key='s1')
        self.assertEqual(res['num'], '1')
        self.assertTrue(res['value'] in keys)

        # Restore all keys.
        L.sadd(key='s1', value=b'\x01'.join(k.encode() for k in keys))
        self.assertEqual(L.srem(key='s1', value='nug'), {'num': '1'})
        self.assertEqual(L.srem(key='s1', value='nug'), {'num': '0'})

        # Create another set, s2 {baze, foo, zai}.
        L.sadd(key='s2', value=b'\x01'.join([b'baze', b'foo', b'zai']))

        # Test multiple set operations, {bar, baz, foo} | {baze, foo, zai}.
        self.assertEqual(L.sinter(key1='s1', key2='s2'), {'foo': '1'})
        res = L.sunion(key1='s1', key2='s2')
        self.assertEqual(res, dict((k, '1') for k in
                                   ('bar', 'baz', 'baze', 'foo', 'zai')))

        res = L.sdiff(key1='s1', key2='s2')
        self.assertEqual(res, {'bar': '1', 'baz': '1'})
        res = L.sdiff(key1='s2', key2='s1')
        self.assertEqual(res, {'baze': '1', 'zai': '1'})

        res = L.sdiff(key1='s1', key2='s2', dest='s3')
        self.assertEqual(res, {'bar': '1', 'baz': '1'})
        res = L.smembers(key='s3')
        self.assertEqual(res, {'bar': '1', 'baz': '1'})

    def test_script_list(self):
        L = self.db.lua

        self.assertEqual(L.lrpush(key='l1', value='i0'), {})
        # Test appending items to list.
        for i in range(1, 5):
            L.lrpush(key='l1', value='i%s' % i)

        # Test accessing items by index.
        for i in range(5):
            self.assertEqual(L.lindex(key='l1', index=i), {'value': 'i%s' % i})

        # Invalid index returns empty result set.
        self.assertEqual(L.lindex(key='l1', index=6), {})
        self.assertEqual(L.lindex(key='l1', index=-1), {'value': 'i4'})

        # Get length of list, pop last item, verify length change.
        self.assertEqual(L.llen(key='l1'), {'num': '5'})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'i4'})
        self.assertEqual(L.llen(key='l1'), {'num': '4'})

        # Verify setting indices.
        self.assertEqual(L.lset(key='l1', index=2, value='i2-x'), {})
        self.assertEqual(L.lindex(key='l1', index=2), {'value': 'i2-x'})

        self.assertEqual(L.lrpop(key='l1'), {'value': 'i3'})
        self.assertEqual(L.llpop(key='l1'), {'value': 'i0'})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'i2-x'})
        self.assertEqual(L.llpop(key='l1'), {'value': 'i1'})

        self.assertEqual(L.llen(key='l1'), {'num': '0'})
        self.assertEqual(L.llpop(key='l1'), {})
        self.assertEqual(L.lrpop(key='l1'), {})

    def test_list_insert(self):
        # Test getting ranges.
        L = self.db.lua
        for i in range(5):
            L.lrpush(key='l1', value='i%s' % i)

        R = functools.partial(L.lrange, key='l1')
        L.linsert(key='l1', index=1, value='i0.5')
        self.assertEqual(R(start=0, stop=3), {'0': 'i0', '1': 'i0.5',
                                              '2': 'i1'})

        L.linsert(key='l1', index=-1, value='i3.5')
        self.assertEqual(R(), {'0': 'i0', '1': 'i0.5', '2': 'i1', '3': 'i2',
                               '4': 'i3', '5': 'i3.5', '6': 'i4'})

    def test_script_list_ranges(self):
        # Test getting ranges.
        L = self.db.lua
        for i in range(5):
            L.lrpush(key='l1', value='i%s' % i)

        R = functools.partial(L.lrange, key='l1')
        all_items = dict((str(i), 'i%s' % i) for i in range(5))
        self.assertEqual(R(), all_items)
        self.assertEqual(R(start=0), all_items)
        self.assertEqual(R(start=-5), all_items)
        self.assertEqual(R(stop=5), all_items)

        # Within bounds.
        self.assertEqual(R(start=1, stop=4), {'1': 'i1', '2': 'i2', '3': 'i3'})
        self.assertEqual(R(start=0, stop=1), {'0': 'i0'})
        self.assertEqual(R(start=3), {'3': 'i3', '4': 'i4'})
        self.assertEqual(R(stop=-3), {'0': 'i0', '1': 'i1'})
        self.assertEqual(R(start=1, stop=-3), {'1': 'i1'})
        self.assertEqual(R(start=3, stop=-1), {'3': 'i3'})
        self.assertEqual(R(start=-1), {'4': 'i4'})
        self.assertEqual(R(start=-2), {'3': 'i3', '4': 'i4'})

        # Out-of-bounds or out-of-order.
        self.assertEqual(R(start=5), {})
        self.assertEqual(R(start=-6), {})
        self.assertEqual(R(start=0, stop=0), {})
        self.assertEqual(R(start=-1, stop=3), {})
        self.assertEqual(R(start=3, stop=2), {})
        self.assertEqual(R(start=1, stop=1), {})

    def test_python_list_integration(self):
        L = self.db.lua
        data = ['foo', 'a' * 1024, '', 'b' * 1024 * 32, 'c']

        self.db['l1'] = self.db._protocol.serialize_list(data)
        self.assertEqual(L.llen(key='l1'), {'num': '5'})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'c'})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'b' * 1024 * 32})
        self.assertEqual(L.lrpop(key='l1'), {'value': ''})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'a' * 1024})
        self.assertEqual(L.lrpop(key='l1'), {'value': 'foo'})

        for item in data:
            L.lrpush(key='l1', value=item)

        raw_data = self.db.get_raw('l1')
        self.assertEqual(self.db._protocol.deserialize_list(raw_data), data)
        self.assertEqual(L.lrange(key='l1'), dict((str(i), data[i])
                                                  for i in range(len(data))))

    def test_python_dict_integration(self):
        L = self.db.lua
        data = {'a' * 64: 'b' * 128, 'c' * 1024: 'd' * 1024 * 32,
                'e' * 256: 'f' * 1024 * 1024, 'g': ''}

        self.db['h1'] = self.db._protocol.serialize_dict(data)
        self.assertEqual(L.hgetall(table_key='h1'), data)
        self.assertEqual(L.hget(table_key='h1', key='e' * 256),
                         {'value': 'f' * 1024 * 1024})
        self.assertTrue(L.hcontains(table_key='h1', key='a' * 64))
        del self.db['h1']

        L.hmset(table_key='h1', **data)
        raw_data = self.db.get_raw('h1')
        self.assertEqual(self.db._protocol.deserialize_dict(raw_data), data)
        self.assertEqual(L.hgetall(table_key='h1'), data)

    def test_script_hash(self):
        L = self.db.lua

        # Set multiple items, returns number set.
        res = L.hmset(table_key='h1', k1='v1', k2='v2', k3='v3')
        self.assertEqual(res['num'], '3')

        # Set individual item using key=..., value=...
        res = L.hset(table_key='h1', key='k1', value='v1-x')
        self.assertEqual(res['num'], '1')

        # Retrieve an individual item.
        self.assertEqual(L.hget(table_key='h1', key='k1'), {'value': 'v1-x'})

        # Missing key returns empty response.
        self.assertEqual(L.hget(table_key='h1', key='kx'), {})

        # Retrieve multiple items. Missing keys are omitted.
        res = L.hmget(table_key='h1', k1='', k2='', kx='')
        self.assertEqual(res, {'k1': 'v1-x', 'k2': 'v2'})

        # Retrieve all key/values in hash.
        res = L.hgetall(table_key='h1')
        self.assertEqual(res, {'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})

        # Delete individual key, returns number deleted.
        self.assertEqual(L.hdel(table_key='h1', key='k2'), {'num': '1'})
        self.assertEqual(L.hdel(table_key='h1', key='k2'), {'num': '0'})

        # Delete multiple keys, returns number deleted.
        self.assertEqual(L.hmdel(table_key='h1', k1='', k3=''), {'num': '2'})
        self.assertEqual(L.hgetall(table_key='h1'), {})

        # We can conditionally set a key (if it does not exist). Returns 1 if
        # successful.
        res = L.hsetnx(table_key='h1', key='k1', value='v1-y')
        self.assertEqual(res, {'num': '1'})

        res = L.hsetnx(table_key='h1', key='k1', value='v1-z')
        self.assertEqual(res, {'num': '0'})

        # Set an additional key and verify hash contents for subsequent checks.
        L.hsetnx(table_key='h1', key='k2', value='v2')
        self.assertEqual(L.hgetall(table_key='h1'), {'k1': 'v1-y', 'k2': 'v2'})

        self.assertEqual(L.hlen(table_key='h1'), {'num': '2'})
        self.assertEqual(L.hcontains(table_key='h1', key='k1'), {'num': '1'})
        self.assertEqual(L.hcontains(table_key='h1', key='kx'), {'num': '0'})

        # Getting values from a non-existent hash returns empty response.
        self.assertEqual(L.hgetall(table_key='h2'), {})

    def test_script_list_items(self):
        self.assertEqual(self.db.script('list'), {})

        self.db.update(k1='v1', k2='v2', k3='v3')
        self.assertEqual(self.db.script('list'),
                         {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})


class TestKyotoTycoonMultiDatabase(BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '%', 'server_args': ['*']}

    def test_multiple_databases_present(self):
        report = self.db.report()
        self.assertTrue('db_0' in report)
        self.assertTrue('db_1' in report)
        self.assertTrue(report['db_0'].endswith(b'path=*'))
        self.assertTrue(report['db_1'].endswith(b'path=%'))

    def test_multiple_databases(self):
        k0 = KyotoTycoon(self._server._host, self._server._port, default_db=0)
        k1 = KyotoTycoon(self._server._host, self._server._port, default_db=1)

        k0.set('k1', 'v1-0')
        k0.set('k2', 'v2-0')
        self.assertEqual(len(k0), 2)
        self.assertEqual(len(k1), 0)

        k1.set('k1', 'v1-1')
        k1.set('k2', 'v2-1')
        self.assertEqual(len(k0), 2)
        self.assertEqual(len(k1), 2)

        self.assertEqual(k0.get('k1'), 'v1-0')
        k0.remove('k1')
        self.assertTrue(k0.get('k1') is None)

        self.assertEqual(k1.get('k1'), 'v1-1')
        k1.remove('k1')
        self.assertTrue(k1.get('k1') is None)

        k0.set_bulk({'k1': 'v1-0', 'k3': 'v3-0'})
        k1.set_bulk({'k1': 'v1-1', 'k3': 'v3-1'})

        self.assertEqual(k0.get_bulk(['k1', 'k2', 'k3']),
                         {'k1': 'v1-0', 'k2': 'v2-0', 'k3': 'v3-0'})
        self.assertEqual(k1.get_bulk(['k1', 'k2', 'k3']),
                         {'k1': 'v1-1', 'k2': 'v2-1', 'k3': 'v3-1'})

        self.assertEqual(k0.remove_bulk(['k3', 'k2']), 2)
        self.assertEqual(k0.remove_bulk(['k3', 'k2']), 0)
        self.assertEqual(k1.remove_bulk(['k3', 'k2']), 2)
        self.assertEqual(k1.remove_bulk(['k3', 'k2']), 0)

        self.assertTrue(k0.add('k2', 'v2-0'))
        self.assertFalse(k0.add('k2', 'v2-x'))

        self.assertTrue(k1.add('k2', 'v2-1'))
        self.assertFalse(k1.add('k2', 'v2-x'))

        self.assertEqual(k0['k2'], 'v2-0')
        self.assertEqual(k1['k2'], 'v2-1')

        self.assertTrue(k0.replace('k2', 'v2-0x'))
        self.assertFalse(k0.replace('k3', 'v3-0'))
        self.assertTrue(k1.replace('k2', 'v2-1x'))
        self.assertFalse(k1.replace('k3', 'v3-1'))

        self.assertEqual(k0['k2'], 'v2-0x')
        self.assertEqual(k1['k2'], 'v2-1x')

        self.assertTrue(k0.append('k3', 'v3-0'))
        self.assertTrue(k0.append('k3', 'x'))
        self.assertTrue(k1.append('k3', 'v3-1'))
        self.assertTrue(k1.append('k3', 'x'))

        self.assertEqual(k0['k3'], 'v3-0x')
        self.assertEqual(k1['k3'], 'v3-1x')

        for k in (k0, k1):
            self.assertTrue(k.exists('k3'))
            self.assertEqual(k.remove('k3'), 1)
            self.assertFalse(k.exists('k3'))

        self.assertEqual(k0.seize('k2'), 'v2-0x')
        self.assertEqual(k1.seize('k2'), 'v2-1x')

        self.assertTrue(k0.cas('k1', 'v1-0', 'v1-0x'))
        self.assertFalse(k0.cas('k1', 'v1-0', 'v1-0z'))

        self.assertTrue(k1.cas('k1', 'v1-1', 'v1-1x'))
        self.assertFalse(k1.cas('k1', 'v1-1', 'v1-1z'))

        self.assertEqual(k0['k1'], 'v1-0x')
        self.assertEqual(k1['k1'], 'v1-1x')

        for k in (k0, k1):
            k.remove_bulk(['i', 'j'])
            self.assertEqual(k.incr('i'), 1)
            self.assertEqual(k.incr('i'), 2)

            self.assertEqual(k.incr_double('j'), 1.)
            self.assertEqual(k.incr_double('j'), 2.)

        self.assertEqual(k0['k1'], 'v1-0x')
        self.assertEqual(k0['k1', 1], 'v1-1x')
        self.assertEqual(k1['k1'], 'v1-1x')
        self.assertEqual(k1['k1', 0], 'v1-0x')

        k0['k2'] = 'v2-0y'
        k0['k2', 1] = 'v2-1y'
        self.assertEqual(k0.get('k2'), 'v2-0y')
        self.assertEqual(k1.get('k2'), 'v2-1y')
        k1['k2'] = 'v2-1z'
        k1['k2', 0] = 'v2-0z'
        self.assertEqual(k0.get('k2'), 'v2-0z')
        self.assertEqual(k1.get('k2'), 'v2-1z')

        del k0['k1']
        del k0['k1', 1]
        self.assertTrue(k0['k1'] is None)
        self.assertTrue(k1['k1'] is None)
        del k1['k2']
        del k1['k2', 0]
        self.assertTrue(k0['k2'] is None)
        self.assertTrue(k1['k2'] is None)

        k0['k3'] = 'v3-0'
        k0['k03'] = 'v03'
        k1['k3'] = 'v3-1'
        k1['k13'] = 'v13'
        self.assertTrue('k3' in k0)
        self.assertTrue('k03' in k0)
        self.assertTrue('k13' not in k0)
        self.assertTrue('k3' in k1)
        self.assertTrue('k13' in k1)
        self.assertTrue('k03' not in k1)

        self.assertEqual(sorted(k0.match_prefix('k')), ['k03', 'k3'])
        self.assertEqual(sorted(k0.match_prefix('k', db=1)), ['k13', 'k3'])
        self.assertEqual(sorted(k1.match_prefix('k')), ['k13', 'k3'])
        self.assertEqual(sorted(k1.match_prefix('k', db=0)), ['k03', 'k3'])

        self.assertEqual(sorted(k0.match_regex('k')), ['k03', 'k3'])
        self.assertEqual(sorted(k0.match_regex('k', db=1)), ['k13', 'k3'])
        self.assertEqual(sorted(k1.match_regex('k')), ['k13', 'k3'])
        self.assertEqual(sorted(k1.match_regex('k', db=0)), ['k03', 'k3'])

        self.assertEqual(sorted(k0.keys()), ['i', 'j', 'k03', 'k3'])
        self.assertEqual(sorted(k0.keys(1)), ['i', 'j', 'k13', 'k3'])
        self.assertEqual(sorted(k1.keys()), ['i', 'j', 'k13', 'k3'])
        self.assertEqual(sorted(k1.keys(0)), ['i', 'j', 'k03', 'k3'])

        k0.clear()
        self.assertTrue('k3' not in k0)
        self.assertTrue('k3' in k1)
        k1.clear()
        self.assertTrue('k3' not in k1)


class TestMultipleThreads(BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '*'}

    def test_multiple_threads(self):
        def write_and_read(n, s):
            for i in range(s, n + s):
                self.db.set('k%s' % i, 'v%s' % i)

            keys = ['k%s' % i for i in range(s, n + s)]
            result = self.db.get_bulk(keys)
            self.assertEqual(result, dict(('k%s' % i, 'v%s' % i)
                                          for i in range(s, n + s)))
            self.db.close()

        threads = [threading.Thread(target=write_and_read,
                                    args=(100, 100 * i)) for i in range(10)]
        for t in threads:
            t.daemon = True
            t.start()
        [t.join() for t in threads]


class TestArrayMapSerialization(unittest.TestCase):
    def setUp(self):
        db = KyotoTycoon()
        self.p = db._protocol

    def assertSerializeDict(self, dictobj):
        dictstr = self.p.serialize_dict(dictobj)
        self.assertEqual(self.p.deserialize_dict(dictstr), dictobj)

    def assertSerializeList(self, listobj):
        liststr = self.p.serialize_list(listobj)
        self.assertEqual(self.p.deserialize_list(liststr), listobj)

    def test_dict_serialize_deserialize(self):
        self.assertSerializeDict({'k1': 'v1', 'k2': 'v2'})
        self.assertSerializeDict({'k1': '', '': 'v2'})
        self.assertSerializeDict({'': ''})
        self.assertSerializeDict({'a' * 128: 'b' * 1024,
                                  'c' * 1024: 'd' * 1024 * 16,
                                  'e' * 1024 * 16: 'f' * 1024 * 1024,
                                  'g': 'g' * 128})
        self.assertSerializeDict({})

    def test_dict_serialization(self):
        serialize, deserialize = self.p.serialize_dict, self.p.deserialize_dict

        data = {'foo': 'baze'}
        dictstr = serialize(data)
        self.assertEqual(dictstr, b'\x03\x04foobaze')
        self.assertEqual(deserialize(dictstr), data)

        dictobj = deserialize(dictstr, decode_values=False)
        self.assertEqual(dictobj, {'foo': b'baze'})

        # Test edge cases.
        data = {'': ''}
        self.assertEqual(serialize(data), b'\x00\x00')

        self.assertEqual(serialize({}), b'')
        self.assertEqual(deserialize(b''), {})

    def test_list_serialize_deserialize(self):
        self.assertSerializeList(['foo', 'bar', 'nugget', 'baze'])
        self.assertSerializeList(['', 'zaizee', ''])
        self.assertSerializeList(['', '', ''])
        self.assertSerializeList(['a' * 128, 'b' * 1024 * 16,
                                  'c' * 1024 * 1024, 'd' * 1024])
        self.assertSerializeList([])

    def test_list_serialization(self):
        serialize, deserialize = self.p.serialize_list, self.p.deserialize_list
        # Simple tests.
        data = ['foo', 'baze', 'nugget', 'bar']
        liststr = serialize(data)
        self.assertEqual(liststr, b'\x03foo\x04baze\x06nugget\x03bar')
        self.assertEqual(deserialize(liststr), data)

        listobj = deserialize(liststr, decode_values=False)
        self.assertEqual(listobj, [b'foo', b'baze', b'nugget', b'bar'])

        # Test edge cases.
        data = ['', 'foo', '']
        self.assertEqual(serialize(data), b'\x00\x03foo\x00')

        self.assertEqual(serialize([]), b'')
        self.assertEqual(deserialize(b''), [])


class TokyoTyrantTests(object):
    def test_basic_operations(self):
        self.assertEqual(len(self.db), 0)

        # Test basic set and get.
        self.db.set('k1', 'v1')
        self.assertEqual(self.db.get('k1'), 'v1')
        self.assertTrue(self.db.get('kx') is None)

        # Test setting bulk data returns records set.
        success = self.db.set_bulk({'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})
        self.assertTrue(success)

        # Test getting bulk data returns dict of just existing keys.
        self.assertEqual(self.db.get_bulk(['k1', 'k2', 'k3', 'kx']),
                         {'k1': 'v1-x', 'k2': 'v2', 'k3': 'v3'})

        # Test removing a record returns number of rows removed.
        self.assertTrue(self.db.remove('k1'))
        self.assertFalse(self.db.remove('k1'))

        self.db['k1'] = 'v1'
        self.assertTrue(self.db.remove_bulk(['k1', 'k3', 'kx']))
        self.assertTrue(self.db.remove_bulk([]))
        self.assertTrue(self.db.remove_bulk(['k2']))

        self.db.append('key', 'abc')
        self.db.append('key', 'def')
        self.assertEqual(self.db['key'], 'abcdef')
        self.assertEqual(self.db.length('key'), 6)
        self.assertTrue(self.db.length('other') is None)

        self.assertEqual(self.db.get_part('key', 2, 2), 'cd')
        self.assertEqual(self.db.get_part('key', 3, 2), 'de')

        del self.db['key']
        self.assertTrue(self.db.add('key', 'foo'))
        self.assertFalse(self.db.add('key', 'bar'))

        # Test dict interface.
        self.assertTrue('key' in self.db)
        self.assertFalse('other' in self.db)
        self.assertEqual(len(self.db), 1)
        self.db['k1'] = 'v1'
        self.db.update({'k2': 'v2', 'k3': 'v3'})
        del self.db['k1']
        self.assertEqual(sorted(list(self.db)), ['k2', 'k3', 'key'])
        del self.db['k3']
        self.assertEqual(sorted(list(self.db.keys())), ['k2', 'key'])
        self.assertEqual(sorted(list(self.db.items())), [
            ('k2', 'v2'), ('key', 'foo')])

        self.db.setnr('k1', 'v1x')
        self.assertEqual(self.db['k1'], 'v1x')
        del self.db['k1']

        data = {'x1': 'y1', 'x2': 'y2', 'x3': 'y3'}
        self.db.setnr_bulk(data)
        self.assertEqual(sorted(list(self.db.items())), [
            ('k2', 'v2'), ('key', 'foo'), ('x1', 'y1'), ('x2', 'y2'),
            ('x3', 'y3')])
        self.db.remove_bulk(['x1', 'x2', 'x3'])

        # Test matching.
        self.assertEqual(sorted(self.db.match_prefix('k')), ['k2', 'key'])
        self.assertEqual(self.db.match_regex('k[0-9]'), {'k2': 'v2'})
        self.assertEqual(self.db.match_regex('x\d'), {})

        # Test numeric operations.
        self.assertEqual(self.db.incr('n'), 1)
        self.assertEqual(self.db.incr('n', 3), 4)
        self.assertEqual(self.db.incr_double('nd'), 1.)
        self.assertEqual(self.db.incr_double('nd', 2.5), 3.5)

    def test_get_raw(self):
        self.db['k1'] = b'v1'
        self.db['k2'] = b'\xff\x00\xff'
        self.assertEqual(self.db.get_raw('k1'), b'v1')
        self.assertEqual(self.db.get_raw('k2'), b'\xff\x00\xff')
        self.assertEqual(self.db.get_bulk_raw(['k1', 'k2']), {
            'k1': b'v1', 'k2': b'\xff\x00\xff'})

    def test_large_read_write(self):
        long_str = 'a' * (1024 * 1024 * 32)  # 32MB string.
        self.db['key'] = long_str
        self.assertEqual(self.db['key'], long_str)
        del self.db['key']
        self.assertEqual(len(self.db), 0)

    def test_misc_commands(self):
        self.db.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
        p = self.db._protocol
        self.assertEqual(p.misc_get('k1'), 'v1')
        self.assertEqual(p.misc_get('k3'), 'v3')
        self.assertTrue(p.misc_get('kx') is None)

        self.assertTrue(p.misc_out('k1'))
        self.assertFalse(p.misc_out('k1'))

        self.assertTrue(p.misc_put('k1', 'v1-x'))
        self.assertEqual(p.misc_get('k1'), 'v1-x')
        self.assertTrue(p.misc_put('k1', 'v1-y'))
        self.assertTrue(p.misc_putlist({'aa': 'AA', 'bb': 'BB'}))

        self.assertEqual(p.misc_get('k1'), 'v1-y')
        self.assertEqual(p.misc_getlist(['k1', 'k2', 'aa', 'bb']), {
            'k1': 'v1-y',
            'k2': 'v2',
            'aa': 'AA',
            'bb': 'BB'})

        self.assertTrue(p.misc_out('k1'))
        self.assertFalse(p.misc_out('k1'))
        self.assertTrue(p.misc_get('k1') is None)

        self.assertTrue(p.misc_putlist({
            'k1': 'v1-x',
            'k2': 'v2-x',
            'k3': 'v3-x'}))
        self.assertEqual(
            p.misc_getlist(['k1', 'k2', 'k3', 'k4', 'k5']),
            {'k1': 'v1-x', 'k2': 'v2-x', 'k3': 'v3-x'})
        self.assertEqual(p.misc_getlist(['k9', 'xz9']), {})
        self.assertEqual(p.misc_getlist([]), {})

        self.assertTrue(p.misc_outlist(['k1', 'k2', 'k3']))
        self.assertTrue(p.misc_outlist(['k1', 'k3']))  # Always true.
        self.assertFalse(p.misc_out('k1'))  # Returns true/false.
        self.assertTrue(p.misc_putlist({}))  # Always true.

        self.assertTrue(p.misc_vanish())
        p.misc_put('k1', 'v1')
        p.misc_putcat('k1', '-x')
        p.misc_putcat('k2', 'v2-y')
        p.misc_putkeep('k2', 'v2-z')
        p.misc_putkeep('k3', 'v3-z')
        self.assertEqual(p.misc_getlist(['k1', 'k2', 'k3', 'kx']), {
            'k1': 'v1-x',
            'k2': 'v2-y',
            'k3': 'v3-z'})

    def test_misc_noulog(self):
        self.db.misc('putlist', [b'k1', b'v1', b'k2', b'v2'], False)
        self.assertEqual(self.db.misc('get', [b'k1'], False), [b'v1'])
        self.assertEqual(self.db.misc('get', [b'k2'], False), [b'v2'])
        self.assertTrue(self.db.misc('get', [b'k3'], False) is None)


class TestTokyoTyrantHash(TokyoTyrantTests, BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '*'}


class TestTokyoTyrantBTree(TokyoTyrantTests, BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '+'}

    def test_ranges(self):
        data = dict(('k%02d' % i, 'v%s' % i) for i in range(20))
        self.db.update(data)
        self.assertEqual(list(self.db), sorted(data))
        self.assertEqual(len(self.db), 20)

        self.assertEqual(self.db.get_range('k09', 'k12'), {
            'k09': 'v9', 'k10': 'v10', 'k11': 'v11'})
        self.assertEqual(self.db.get_range('k09', 'k121'), {
            'k09': 'v9', 'k10': 'v10', 'k11': 'v11', 'k12': 'v12'})
        self.assertEqual(self.db.get_range('aa', 'bb'), {})
        self.assertEqual(self.db.get_range('', 'k03'),
                         {'k00': 'v0', 'k01': 'v1', 'k02': 'v2'})
        self.assertEqual(self.db.get_range('k18', ''), {})
        self.assertEqual(self.db.get_range('k18'),
                         {'k18': 'v18', 'k19': 'v19'})
        self.assertEqual(self.db.get_range('k18', b'\xff'),
                         {'k18': 'v18', 'k19': 'v19'})

        self.db.remove_bulk(['k02', 'k03', 'k04', 'k05', 'k06', 'k07', 'k08'])
        self.assertEqual(self.db.match_prefix('k0'), ['k00', 'k01', 'k09'])

        self.assertEqual(self.db.iter_from('k16'), {
            'k16': 'v16', 'k17': 'v17', 'k18': 'v18', 'k19': 'v19'})
        self.assertEqual(self.db.iter_from('kx'), {})

    def test_duplicates(self):
        # When using an in-memory B-Tree duplicates are not stored.
        self.db.set('k1', 'v1')
        self.db.set('k1', 'v2')
        self.assertEqual(list(self.db.items()), [('k1', 'v2')])

        # To trigger the storage of duplicates with an on-disk B-Tree, we first
        # use the set_bulk API. See the following test-case (BTreeOnDisk) for
        # an example of how this behavior differs.
        self.db.set_bulk({'k1': 'vx'})
        self.db.set('k1', 'vy')
        self.assertEqual(list(self.db.items()), [('k1', 'vy')])

        # What about using the setdup and setdupback APIs? Apparently they have
        # no effect and return failure.
        self.assertFalse(self.db.setdup('k1', 'vz0'))
        self.assertFalse(self.db.setdupback('k1', 'vz1'))
        self.assertEqual(list(self.db.items()), [('k1', 'vy')])


class TestTokyoTyrantBTreeOnDisk(BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '/tmp/tt-btree.tcb'}

    @classmethod
    def tearDownClass(cls):
        super(TestTokyoTyrantBTreeOnDisk, cls).tearDownClass()
        if os.path.exists('/tmp/tt-btree.tcb'):
            os.unlink('/tmp/tt-btree.tcb')

    def test_duplicates(self):
        def assertItems(expected):
            self.assertEqual([v for _, v in self.db.items()], expected)

        # When using the ordinary "set" API, we will not store duplicates.
        self.db.set('k1', 'v1')
        self.db.set('k1', 'v2')
        assertItems(['v2'])

        # When we use "set_bulk()" it will result in the storage of duplicates.
        self.db.set_bulk({'k1': 'vx'})
        self.db.set('k1', 'vy')  # Dup goes to the front.
        assertItems(['vy', 'vx'])

        # Subsequent set_bulk will store dupes.
        self.db.set_bulk({'k1': 'vz', 'k2': 'v2'})  # Dupe goes to the back?
        assertItems(['vy', 'vx', 'vz', 'v2'])

        # WTF, the dupes now replace just the first item?
        self.db.set('k1', 'v0')
        self.db.set('k1', 'vw')
        assertItems(['vw', 'vx', 'vz', 'v2'])

        # Just documenting behavior... the "back" ones are put at the front,
        # while the "setdup" calls go to the back.
        self.assertTrue(self.db.setdup('k1', 'vy-1'))
        self.assertTrue(self.db.setdupback('k1', 'vy-2'))
        self.assertTrue(self.db.setdup('k1', 'v0-1'))
        self.assertTrue(self.db.setdupback('k1', 'v0-2'))
        assertItems(['v0-2', 'vy-2', 'vw', 'vx', 'vz', 'vy-1', 'v0-1', 'v2'])

        # Again, replaces just the first.
        self.db.set('k1', 'zz')
        assertItems(['zz', 'vy-2', 'vw', 'vx', 'vz', 'vy-1', 'v0-1', 'v2'])

        # What happens when doing append?
        self.db.append('k1', 'foo')
        assertItems(['zzfoo', 'vy-2', 'vw', 'vx', 'vz', 'vy-1', 'v0-1', 'v2'])

        # Add calls will not succeed.
        self.assertFalse(self.db.add('k1', 'yy'))

        # Remove calls just remove the first.
        self.assertTrue(self.db.remove('k1'))
        self.assertTrue(self.db.remove('k1'))
        self.assertTrue(self.db.set('k1', 'vwx'))
        self.assertTrue(self.db.setdup('k1', 'v3'))
        assertItems(['vwx', 'vx', 'vz', 'vy-1', 'v0-1', 'v3', 'v2'])

        # We remove the first item, vwx, add v2-z which replaces v2, and
        # replace v2-z with v2-y.
        self.assertTrue(self.db.remove('k1'))
        self.assertTrue(self.db.set('k2', 'v2-z'))
        self.assertTrue(self.db.set('k2', 'v2-y'))
        assertItems(['vx', 'vz', 'vy-1', 'v0-1', 'v3', 'v2-y'])

        self.assertTrue(self.db.setdup('k2', 'v2-z'))
        assertItems(['vx', 'vz', 'vy-1', 'v0-1', 'v3', 'v2-y', 'v2-z'])

        self.db.remove_bulk(['k1'])
        self.db.set('k1', 'v1-1')
        self.db.set('k1', 'v1-0')
        self.db.set('k1', 'v1-2')
        assertItems(['v1-2', 'v2-y', 'v2-z'])
        self.db.clear()

        # If we first set_bulk, then subsequently call set multiple times, no
        # duplicates are stored.
        self.db.set_bulk({'k1': 'v1-b'})
        self.db.set('k1', 'v1-c')
        self.db.set('k1', 'v1-a')
        assertItems(['v1-a'])

        # If we first set, then call set_bulk, then call set again, duplicates
        # will be stored, as if triggered by the set_bulk() call.
        self.db.set('k2', 'v2-a')
        self.db.set_bulk({'k2': 'v2-b'})
        self.db.set('k2', 'v2-c')  # This stores a duplicate.
        self.db.set('k2', 'v2-d')  # This overwrites the duplicate (???).
        assertItems(['v1-a', 'v2-d', 'v2-b'])

        # Wtf, who knows?
        self.assertTrue(self.db.clear())


class TestTokyoTyrantScripting(BaseTestCase):
    lua_script = os.path.join(os.path.dirname(__file__), 'kt/scripts/tt.lua')
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {
        'database': '+',
        'server_args': ['-ext', lua_script]}

    def test_script_method(self):
        self.db.update(k1='v1', k2='v2', k3='v3')
        self.assertTrue('k2' in self.db)
        self.assertEqual(self.db.script('seize', 'k2'), b'v2')
        self.assertFalse('k2' in self.db)
        self.assertEqual(self.db.script('seize', 'k2'), b'')

    def test_script_match(self):
        def decode_match(r):
            accum = {}
            for line in r.decode('utf-8').strip().splitlines():
                key, value = line.split('\t')
                accum[key] = value
            return accum

        self.db.update(key='value', key_a='a', key_b='bbb', ky_a='aa')
        results = self.db.script('match_pattern', 'key*')
        self.assertEqual(decode_match(results), {'key': 'value', 'key_a': 'a',
                                                 'key_b': 'bbb'})

        results = self.db.script('match_pattern', 'key_%a')
        self.assertEqual(decode_match(results), {'key_a': 'a', 'key_b': 'bbb'})

        # No matches returns empty result.
        results = self.db.script('match_pattern', '%d+')
        self.assertEqual(decode_match(results), {})

        results = self.db.script('match_similar', 'key', 1)
        self.assertEqual(decode_match(results), {'key': 'value'})

        results = self.db.script('match_similar', 'key', 2)
        self.assertEqual(decode_match(results), {'key': 'value', 'key_a': 'a',
                                                 'key_b': 'bbb'})

        results = self.db.script('match_similar_value', 'a', 1)
        self.assertEqual(decode_match(results), {'key_a': 'a', 'ky_a': 'aa'})

    def test_script_queue(self):
        for i in range(5):
            self.db.script('enqueue', 'testqueue', 'item-%s' % i)

        self.assertEqual(self.db.script('queuesize', 'testqueue'), b'5')

        # By default one item is dequeued.
        item = self.db.script('dequeue', 'testqueue')
        self.assertEqual(item, b'item-0\n')
        self.assertEqual(self.db.script('queuesize', 'testqueue'), b'4')

        # We can dequeue multiple items, which are newline-separated.
        items = self.db.script('dequeue', 'testqueue', 3)
        self.assertEqual(items, b'item-1\nitem-2\nitem-3\n')

        # It's OK if fewer items exist.
        items = self.db.script('dequeue', 'testqueue', 3)
        self.assertEqual(items, b'item-4\n')

        # No items -> empty string and zero count.
        self.assertEqual(self.db.script('dequeue', 'testqueue'), b'')
        self.assertEqual(self.db.script('queuesize', 'testqueue'), b'0')


class TestTokyoTyrantScriptingTable(BaseTestCase):
    lua_script = os.path.join(os.path.dirname(__file__), 'kt/scripts/tt.lua')
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {
        'database': '/tmp/kt_tt.tct',
        'serializer': TT_TABLE,
        'server_args': ['-ext', lua_script]}

    def test_script_with_table(self):
        self.db['t1'] = {'k1': 'v1', 'k2': 'v2'}
        self.db['t2'] = {'k2': 'v2', 'k3': 'v3'}
        res = self.db.script('seize', 't2', encode_value=False,
                             decode_result=True)
        self.assertEqual(res, {'k2': 'v2', 'k3': 'v3'})

        res = self.db.script('seize', 't2', encode_value=False,
                             decode_result=True)
        self.assertEqual(res, {})

        res = self.db.script('table_get', 't1', 'k2', encode_value=False)
        self.assertEqual(res, b'v2')

        res = self.db.script('table_get', 't1', 'kx', encode_value=False)
        self.assertEqual(res, b'')

        res = self.db.script('table_pop', 't1', 'k2', encode_value=False)
        self.assertEqual(res, b'v2')
        res = self.db.script('table_pop', 't1', 'k2', encode_value=False)
        self.assertEqual(res, b'')
        self.assertEqual(self.db['t1'], {'k1': 'v1'})

        self.db['t1'] = {'k1': 'v1', 'k2': 'v2'}
        res = self.db.script('table_update', 't1', {'k1': 'v1-x', 'k4': 'v4'})
        self.assertTrue(res)
        self.assertEqual(self.db['t1'], {'k1': 'v1-x', 'k2': 'v2', 'k4': 'v4'})

        res = self.db.script('table_update', 't3', {'k1': 'v1z', 'k2': 'v2y'})
        self.assertTrue(res)
        self.assertEqual(self.db['t3'], {'k1': 'v1z', 'k2': 'v2y'})


class TestTokyoTyrantSerializers(TestKyotoTycoonSerializers):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '*'}

    def get_client(self, serializer):
        return TokyoTyrant(self._server._host, self._server._port, serializer)


class TestTokyoTyrantTableDB(BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '/tmp/kt_tt.tct', 'serializer': TT_TABLE}

    @classmethod
    def tearDownClass(cls):
        super(TestTokyoTyrantTableDB, cls).tearDownClass()
        if os.path.exists(cls.server_kwargs['database']):
            os.unlink(cls.server_kwargs['database'])

    def test_table_database(self):
        self.db['t1'] = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        self.assertEqual(self.db['t1'], {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        self.db['t2'] = {}
        self.assertEqual(self.db['t2'], {})

        self.db.set_bulk({
            't1': {'k1': 'v1', 'k2': 'v2'},
            't2': {'x1': 'y1'},
            't3': {}})
        self.assertEqual(self.db.get_bulk(['t1', 't2', 't3', 'tx']), {
            't1': {'k1': 'v1', 'k2': 'v2'},
            't2': {'x1': 'y1'},
            't3': {}})


class TestTokyoTyrantSearch(BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '/tmp/kt_tt.tct', 'serializer': TT_TABLE}

    @classmethod
    def tearDownClass(cls):
        super(TestTokyoTyrantSearch, cls).tearDownClass()
        if os.path.exists(cls.server_kwargs['database']):
            os.unlink(cls.server_kwargs['database'])

    def setUp(self):
        super(TestTokyoTyrantSearch, self).setUp()
        data = [
            {'name': 'huey', 'type': 'cat', 'eyes': 'blue', 'age': '7'},
            {'name': 'mickey', 'type': 'dog', 'eyes': 'blue', 'age': '9'},
            {'name': 'zaizee', 'type': 'cat', 'eyes': 'blue', 'age': '5'},
            {'name': 'charlie', 'type': 'human', 'eyes': 'brown', 'age': '35'},
            {'name': 'leslie', 'type': 'human', 'eyes': 'blue', 'age': '34'},
            {'name': 'connor', 'type': 'human', 'eyes': 'brown', 'age': '3'}]
        for item in data:
            self.db[item['name']] = item

    def test_search(self):
        query = (QueryBuilder()
                 .filter('type', constants.OP_STR_EQ, 'cat')
                 .order_by('name', constants.ORDER_STR_DESC))
        self.assertEqual(query.execute(self.db), ['zaizee', 'huey'])

        query = (QueryBuilder()
                 .filter('age', constants.OP_NUM_GE, '7')
                 .filter('type', constants.OP_STR_ANY, 'human,cat')
                 .order_by('age', constants.ORDER_NUM_DESC))
        self.assertEqual(query.execute(self.db),
                         ['charlie', 'leslie', 'huey'])

        query = (QueryBuilder()
                 .order_by('name', constants.ORDER_STR_DESC)
                 .limit(3)
                 .offset(1))
        self.assertEqual(query.execute(self.db), ['mickey', 'leslie', 'huey'])

    def test_search_get(self):
        query = (QueryBuilder()
                 .filter('type', constants.OP_STR_EQ, 'cat')
                 .order_by('name', constants.ORDER_STR_DESC))
        self.assertEqual(query.get(self.db), [
            ('zaizee', {'name': 'zaizee', 'type': 'cat', 'age': '5',
                        'eyes': 'blue'}),
            ('huey', {'name': 'huey', 'type': 'cat', 'age': '7',
                      'eyes': 'blue'})])

    def test_indexing(self):
        self.assertTrue(self.db.set_index('name', constants.INDEX_STR))
        self.assertTrue(self.db.set_index('age', constants.INDEX_NUM))

        # Check if index exists first -- returns False.
        self.assertFalse(self.db.set_index('name', constants.INDEX_STR, True))
        self.assertTrue(self.db.optimize_index('age'))

        # Perform a query.
        query = (QueryBuilder()
                 .filter('age', constants.OP_NUM_LT, '10')
                 .order_by('name', constants.ORDER_STR_DESC)
                 .limit(3)
                 .offset(1))
        self.assertEqual(query.execute(self.db), ['mickey', 'huey', 'connor'])

        # Verify we can delete an index.
        self.assertTrue(self.db.delete_index('name'))


from kt.models import *


class BaseModelTestCase(BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '/tmp/kt_tt.tct', 'serializer': KT_NONE}

    def setUp(self):
        super(BaseModelTestCase, self).setUp()
        class Base(Model):
            __database__ = self.db
        self.Base = Base

    @classmethod
    def tearDownClass(cls):
        super(BaseModelTestCase, cls).tearDownClass()
        if os.path.exists(cls.server_kwargs['database']):
            os.unlink(cls.server_kwargs['database'])


class TestTokyoTyrantModels(BaseModelTestCase):
    def test_basic_crud_apis(self):
        class User(self.Base):
            name = TextField()
            dob = TextField()
            status = IntegerField()

        User.create('u1', name='charlie', dob='1983-01-01', status=1)
        User.create('u2', name='huey', dob='2011-08-01', status=2)
        User.create('u3', name='mickey', dob='2009-05-01', status=3)

        u = User['u1']
        self.assertEqual(u.name, 'charlie')
        self.assertEqual(u.dob, '1983-01-01')
        self.assertEqual(u.status, 1)

        u = User['u3']
        u.status = 4
        self.assertTrue(u.save())
        u = User['u3']
        self.assertEqual(u.key, 'u3')
        self.assertEqual(u.name, 'mickey')
        self.assertEqual(u.dob, '2009-05-01')
        self.assertEqual(u.status, 4)

        u4 = User(key='u4', name='zaizee', dob='2012-01-01')
        u4.save()
        u4_db = User['u4']
        self.assertEqual(u4_db.key, 'u4')
        self.assertEqual(u4_db.name, 'zaizee')
        self.assertEqual(u4_db.dob, '2012-01-01')
        self.assertTrue(u4_db.status is None)

        u4_db.delete()
        self.assertRaises(KeyError, lambda: User['u4'])

    def test_model_apis(self):
        class KV(self.Base):
            value = TextField()
            status = IntegerField()

        data = [('k1', 'v1', 1),
                ('k2', 'v2', 2),
                ('k3', 'v3', 3),
                ('k4', 'v4', 4)]

        # Test creation via setitem with dict.
        for key, value, status in data:
            KV[key] = {'value': value, 'status': status}

        def assertModel(model, num):
            self.assertEqual(model.key, 'k%s' % num)
            self.assertEqual(model.value, 'v%s' % num)
            self.assertEqual(model.status, num)

        # Test data was stored correctly, retrieving using get and getitem.
        assertModel(KV.get('k2'), 2)
        assertModel(KV['k3'], 3)

        # Test bulk-get.
        data = KV.get_list(['k1', 'xx', 'k4', 'kx', 'k3'])
        self.assertEqual(len(data), 3)
        assertModel(data[0], 1)
        assertModel(data[1], 4)
        assertModel(data[2], 3)

        data = KV['k4', 'xx', 'k2']
        self.assertEqual(len(data), 2)
        assertModel(data[0], 4)
        assertModel(data[1], 2)

        # Test bulk-delete.
        self.assertTrue(KV.delete_list(['k1', 'kx', 'k4', 'k3']))
        self.assertEqual(len(self.db), 1)

        data = KV.all()
        self.assertEqual(len(data), 1)
        assertModel(data[0], 2)

        del KV['k2']
        self.assertRaises(KeyError, lambda: KV['k2'])

    def test_model_field_types(self):
        class T(self.Base):
            bytes_field = BytesField()
            text_field = TextField()
            int_field = IntegerField()
            float_field = FloatField()
            dt_field = DateTimeField()
            d_field = DateField()
            ts_field = TimestampField()
            tk_field = TokenField()
            fts_field = SearchField()

        # All values are stored/retrieved correctly.
        dt = datetime.datetime(2018, 1, 2, 3, 4, 5, 6789)
        d = datetime.date(2018, 12, 25)

        T.create(key='t1', bytes_field=b'\xfftest\xff', text_field='test',
                 int_field=7, float_field=3.14, dt_field=dt, d_field=d,
                 ts_field=dt, tk_field='foo bar baz', fts_field='huey zaizee')
        t1 = T['t1']
        self.assertEqual(t1.key, 't1')
        self.assertEqual(t1.bytes_field, b'\xfftest\xff')
        self.assertEqual(t1.text_field, 'test')
        self.assertEqual(t1.int_field, 7)
        self.assertEqual(t1.float_field, 3.14)
        self.assertEqual(t1.dt_field, dt)
        self.assertEqual(t1.d_field, d)
        self.assertEqual(t1.ts_field, dt)
        self.assertEqual(t1.tk_field, 'foo bar baz')
        self.assertEqual(t1.fts_field, 'huey zaizee')

        # All blank fields works correctly.
        T.create(key='t2')
        t2 = T['t2']
        self.assertEqual(t2.key, 't2')
        self.assertTrue(t2.bytes_field is None)
        self.assertTrue(t2.text_field is None)
        self.assertTrue(t2.int_field is None)
        self.assertTrue(t2.float_field is None)
        self.assertTrue(t2.dt_field is None)
        self.assertTrue(t2.d_field is None)
        self.assertTrue(t2.ts_field is None)
        self.assertTrue(t2.tk_field is None)
        self.assertTrue(t2.fts_field is None)


class TestTokyoTyrantQuery(BaseModelTestCase):
    def test_query(self):
        class User(self.Base):
            username = TextField()
            status = IntegerField(index=True)
            tags = TextField(index=True)

        data = [
            ('huey', 1, 'cat white'),
            ('mickey', 2, 'dog black'),
            ('zaizee', 1, 'cat gray black'),
            ('scout', 2, 'dog black white'),
            ('pipey', 3, 'bird red')]
        for username, status, tags in data:
            User.create(username, username=username, status=status, tags=tags)

        # Verify we can create indexes.
        User.create_indexes()

        # Simple multi-term filter and order-by.
        query = User.query().filter(
            (User.status < 3),
            (User.tags.contains_any('white'))).order_by(User.status.desc())
        self.assertEqual(query.execute(), ['scout', 'huey'])

        query = User.query().filter(User.status == 1).order_by(User.username)
        self.assertEqual(query.execute(), ['huey', 'zaizee'])

    def test_query_apis(self):
        class KV(self.Base):
            value = TextField()
            status = IntegerField(index=True)

        KV.create_list([KV(key='k%s' % i, value='v%s' % i, status=i)
                        for i in range(20)])
        KV.create_indexes()

        query = KV.query().filter(KV.value.endswith('5')).order_by(KV.status)
        self.assertEqual(query.execute(), ['k5', 'k15'])
        self.assertEqual(query.count(), 2)

        query = KV.query().filter(
            KV.value.startswith('v1'),
            KV.status < 13).order_by(KV.value)
        self.assertEqual(query.execute(), ['k1', 'k10', 'k11', 'k12'])
        self.assertEqual(query.count(), 4)

        query = KV.query().filter(KV.value.endswith('9')).delete()
        self.assertRaises(KeyError, lambda: KV['k9'])
        self.assertRaises(KeyError, lambda: KV['k19'])

        query = KV.query()
        self.assertEqual(query.count(), 18)

        query = query.filter(KV.value.contains_any_exact('v4', 'v1', 'v3'))
        self.assertEqual(query.order_by(KV.status.desc()).execute(),
                         ['k4', 'k3', 'k1'])

        query = (KV.query()
                 .filter(KV.status.between(12, 15))
                 .order_by(KV.value))
        self.assertEqual(query.execute(), ['k12', 'k13', 'k14', 'k15'])

        query = (KV.query()
                 .filter(KV.status.matches_any(8, 10, 11, 12))
                 .filter(KV.value != 'v11')
                 .filter(KV.status != 10)
                 .order_by(KV.value))
        self.assertEqual(query.execute(), ['k12', 'k8'])

    def test_query_all(self):
        class KV(self.Base):
            value = IntegerField()

        KV.create_list([KV(key='k%s' % i, value=i) for i in range(4)])

        self.assertEqual(sorted((k.key, k.value) for k in KV.all()), [
            ('k0', 0),
            ('k1', 1),
            ('k2', 2),
            ('k3', 3)])

        del KV['k2', 'k1']
        self.assertEqual(sorted(k.value for k in KV.all()), [0, 3])

    def test_query_get(self):
        class KV(self.Base):
            value = TextField()
            status = IntegerField(index=True)

        KV.create_list([KV(key='k%s' % i, value='v%s' % i, status=i)
                        for i in range(5)])
        KV.create_indexes()

        query = (KV.query()
                 .filter(KV.status < 3)
                 .order_by(KV.status.desc()))
        self.assertEqual([(k.key, k.value, k.status) for k in query.get()], [
            ('k2', 'v2', 2),
            ('k1', 'v1', 1),
            ('k0', 'v0', 0)])

        query = (KV.query()
                 .filter(KV.value.contains_any('v4', 'v1', 'v3'))
                 .order_by(KV.value))
        self.assertEqual([(k.key, k.value, k.status) for k in query.get()], [
            ('k1', 'v1', 1),
            ('k3', 'v3', 3),
            ('k4', 'v4', 4)])

    def test_special_string_ops(self):
        class KV(self.Base):
            value = TextField()

        KV['k1'] = {'value': 'baz zoo bar'}
        KV['k2'] = {'value': 'foo bar baz'}
        KV['k3'] = {'value': 'nug baze zoo'}

        def assertQuery(expression, expected):
            query = KV.query().filter(expression).order_by(KV.value)
            self.assertEqual(query.execute(), expected)

        assertQuery(KV.value.regex('^baz'), ['k1'])
        assertQuery(KV.value.regex('zoo$'), ['k3'])
        assertQuery(KV.value.regex('zoo'), ['k1', 'k3'])
        assertQuery(KV.value.regex('baze?'), ['k1', 'k2', 'k3'])
        assertQuery(KV.value.regex('[bf]o{2}'), ['k2'])
        assertQuery(KV.value.regex('ba.e'), ['k3'])

        assertQuery(KV.value.contains('zoo'), ['k1', 'k3'])
        assertQuery(KV.value.contains('baze'), ['k3'])
        assertQuery(KV.value.contains('nugget'), [])

        assertQuery(KV.value.startswith('ba'), ['k1'])
        assertQuery(KV.value.startswith('foox'), [])
        assertQuery(KV.value.endswith('oo'), ['k3'])

        assertQuery(KV.value.contains_all('bar', 'baz'), ['k1', 'k2'])
        assertQuery(KV.value.contains_all('zoo', 'baz'), ['k1'])
        assertQuery(KV.value.contains_all('o', 'bar'), [])

        assertQuery(KV.value.contains_any('bar', 'baz'), ['k1', 'k2'])
        assertQuery(KV.value.contains_any('zoo', 'baz'), ['k1', 'k2', 'k3'])
        assertQuery(KV.value.contains_any_exact('bar', 'baz'), [])

    def test_int_ops(self):
        class KV(self.Base):
            value = IntegerField()

        for i in range(20):
            KV['k%s' % i] = {'value': i}

        def assertQuery(expression, expected):
            query = KV.query().filter(expression).order_by(KV.value)
            self.assertEqual(query.execute(), expected)

        assertQuery(KV.value == 9, ['k9'])
        assertQuery(KV.value < 2, ['k0', 'k1'])
        assertQuery(KV.value <= 2, ['k0', 'k1', 'k2'])
        assertQuery(KV.value > 17, ['k18', 'k19'])
        assertQuery(KV.value >= 17, ['k17', 'k18', 'k19'])
        assertQuery(KV.value < 0, [])
        assertQuery(KV.value > 19, [])
        assertQuery(KV.value.between(8, 11), ['k8', 'k9', 'k10', 'k11'])
        assertQuery(KV.value.between(18, 999), ['k18', 'k19'])
        assertQuery(KV.value.between(-10, 2), ['k0', 'k1', 'k2'])
        assertQuery(KV.value.matches_any(10, 0, 99, 18), ['k0', 'k10', 'k18'])
        assertQuery(KV.value.matches_any(100, -1, -0), ['k0'])

    def test_query_dates_times(self):
        class Event(self.Base):
            dt = DateTimeField(index=True)
            ts = TimestampField(index=True)

        data = [
            (datetime.datetime(2018, 1, 2, 3, 4, 5, 6789), 'e1'),
            (datetime.datetime(2018, 1, 3, 2, 0, 0, 0), 'e2'),
            (datetime.datetime(2018, 2, 1, 0, 0, 0, 0), 'e3'),
            (datetime.datetime(2018, 12, 1, 0, 0, 0, 0), 'e4')]
        for dt, key in data:
            Event.create(key=key, dt=dt, ts=dt)

        def assertMessages(filter_condition, expected):
            query = Event.query().filter(filter_condition).order_by(Event.dt)
            self.assertEqual(query.execute(), expected)

        assertMessages(Event.dt.startswith('2018-01'), ['e1', 'e2'])
        assertMessages(Event.dt.endswith('000000'), ['e2', 'e3', 'e4'])
        assertMessages(Event.dt.contains('01-02'), ['e1'])

        def D(y=2018, m=1, d=1, H=0, M=0, S=0, f=0):
            return datetime.datetime(y, m, d, H, M, S, f)
        assertMessages(Event.ts < D(m=2), ['e1', 'e2'])
        assertMessages(Event.ts > D(m=2), ['e4'])
        assertMessages(Event.ts == D(m=2), ['e3'])
        assertMessages(Event.ts != D(m=2), ['e1', 'e2', 'e4'])
        assertMessages(Event.ts.between(D(d=3), D(m=3)), ['e2', 'e3'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
