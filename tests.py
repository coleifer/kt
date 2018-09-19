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
        self.db['k1'] = 'v1'
        self.db.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(self.db.pop('k1'), 'v1')
        self.assertTrue(self.db.pop('k1') is None)
        self.assertEqual(sorted(list(self.db)), ['k2', 'k3', 'key'])
        del self.db['k3']
        self.assertEqual(sorted(list(self.db.keys())), ['k2', 'key'])

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

    def test_large_read_write(self):
        long_str = 'a' * (1024 * 1024 * 32)  # 32MB string.
        self.db['key'] = long_str
        self.assertEqual(self.db['key'], long_str)
        del self.db['key']
        self.assertEqual(len(self.db), 0)


class TestKyotoTycoonHash(KyotoTycoonTests, BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '*'}


class TestKyotoTycoonBTree(KyotoTycoonTests, BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '%'}


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

        # Test matching.
        self.assertEqual(sorted(self.db.match_prefix('k')), ['k2', 'key'])
        self.assertEqual(self.db.match_regex('k[0-9]'), {'k2': 'v2'})
        self.assertEqual(self.db.match_regex('x\d'), {})

        # Test numeric operations.
        self.assertEqual(self.db.incr('n'), 1)
        self.assertEqual(self.db.incr('n', 3), 4)
        self.assertEqual(self.db.incr_double('nd'), 1.)
        self.assertEqual(self.db.incr_double('nd', 2.5), 3.5)

    def test_large_read_write(self):
        long_str = 'a' * (1024 * 1024 * 32)  # 32MB string.
        self.db['key'] = long_str
        self.assertEqual(self.db['key'], long_str)
        del self.db['key']
        self.assertEqual(len(self.db), 0)

    def test_misc_commands(self):
        self.db.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(self.db.misc('get', 'k1'), 'v1')
        self.assertEqual(self.db.misc('get', 'k3'), 'v3')
        self.assertTrue(self.db.misc('get', 'kx') is False)
        self.assertTrue(self.db.misc('out', 'k1'))
        self.assertFalse(self.db.misc('out', 'k1'))
        self.assertTrue(self.db.misc('put', data={'k1': 'v1-x'}))
        self.assertEqual(self.db.misc('get', 'k1'), 'v1-x')
        self.assertTrue(self.db.misc('put', data={'k1': 'v1-y'}))
        self.assertTrue(self.db.misc('putlist', data={'a': 'A', 'b': 'B'}))
        self.assertEqual(self.db.misc('get', 'k1'), 'v1-y')
        self.assertTrue(self.db.misc('out', 'k1'))
        self.assertFalse(self.db.misc('out', 'k1'))
        self.assertFalse(self.db.misc('get', 'k1'))
        self.assertTrue(self.db.misc('put', data={'k1': 'v1-z'}))
        self.assertTrue(self.db.misc('putlist', data={
            'k1': 'v1-x',
            'k2': 'v2-x',
            'k3': 'v3-x'}))
        self.assertEqual(
            self.db.misc('getlist', ['k1', 'k2', 'k3', 'k4', 'k5']),
            {'k1': 'v1-x', 'k2': 'v2-x', 'k3': 'v3-x'})
        self.assertEqual(self.db.misc('getlist', ['k9', 'xz9']), {})
        self.assertEqual(self.db.misc('getlist', []), {})
        self.assertTrue(self.db.misc('outlist', ['k1', 'k2', 'k3']))
        self.assertTrue(self.db.misc('outlist', ['k1', 'k3']))  # Always true.
        self.assertFalse(self.db.misc('out', ['k1']))  # Returns true/false.
        self.assertTrue(self.db.misc('putlist', data={}))  # Always true.
        self.assertFalse(self.db.misc('put', data={}))  # Returns true/false.


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
        self.assertEqual(query.search(self.db), ['zaizee', 'huey'])

        query = (QueryBuilder()
                 .filter('age', constants.OP_NUM_GE, '7')
                 .filter('type', constants.OP_STR_ANY, 'human,cat')
                 .order_by('age', constants.ORDER_NUM_DESC))
        self.assertEqual(query.search(self.db),
                         ['charlie', 'leslie', 'huey'])

        query = (QueryBuilder()
                 .order_by('name', constants.ORDER_STR_DESC)
                 .limit(3)
                 .offset(1))
        self.assertEqual(query.search(self.db), ['mickey', 'leslie', 'huey'])

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
        self.assertEqual(query.search(self.db), ['mickey', 'huey', 'connor'])

        # Verify we can delete an index.
        self.assertTrue(self.db.delete_index('name'))


from kt.models import BytesField
from kt.models import FloatField
from kt.models import IntegerField
from kt.models import Model
from kt.models import TextField


class TestTokyoTyrantModels(BaseTestCase):
    server = EmbeddedTokyoTyrantServer
    server_kwargs = {'database': '/tmp/kt_tt.tct', 'serializer': KT_NONE}

    @classmethod
    def tearDownClass(cls):
        super(TestTokyoTyrantModels, cls).tearDownClass()
        if os.path.exists(cls.server_kwargs['database']):
            os.unlink(cls.server_kwargs['database'])

    def test_basic_crud_apis(self):
        class User(Model):
            __database__ = self.db
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

    def test_model_field_types(self):
        class T(Model):
            __database__ = self.db
            bytes_field = BytesField()
            text_field = TextField()
            int_field = IntegerField()
            float_field = FloatField()

        # All values are stored/retrieved correctly.
        T.create(key='t1', bytes_field=b'\xfftest\xff', text_field='test',
                 int_field=7, float_field=3.14)
        t1 = T['t1']
        self.assertEqual(t1.key, 't1')
        self.assertEqual(t1.bytes_field, b'\xfftest\xff')
        self.assertEqual(t1.text_field, 'test')
        self.assertEqual(t1.int_field, 7)
        self.assertEqual(t1.float_field, 3.14)

        # All blank fields works correctly.
        T.create(key='t2')
        t2 = T['t2']
        self.assertEqual(t2.key, 't2')
        self.assertTrue(t2.bytes_field is None)
        self.assertTrue(t2.text_field is None)
        self.assertTrue(t2.int_field is None)
        self.assertTrue(t2.float_field is None)

    def test_query(self):
        class User(Model):
            __database__ = self.db
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


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
