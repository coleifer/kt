import os
import sys
import unittest

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
from kt import TokyoTyrant
from kt import TT_TABLE


class BaseTestCase(unittest.TestCase):
    _server = None
    db = None
    server = None
    server_kwargs = None

    @classmethod
    def setUpClass(cls):
        if cls.server is None:
            return

        cls._server = cls.server(**(cls.server_kwargs or {}))
        cls._server.run()
        cls.db = cls._server.client

    @classmethod
    def tearDownClass(cls):
        if cls._server is not None:
            cls._server.stop()
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

    @unittest.skipIf(msgpack is None, 'msgpack-python not installed')
    def test_serializer_msgpack(self):
        self._test_serializer_object(KT_MSGPACK)

    def test_serializer_pickle(self):
        self._test_serializer_object(KT_PICKLE)

    def test_serializer_none(self):
        db = self.get_client(KT_NONE)
        db.set('k1', 'v1')
        self.assertEqual(self.db.get('k1'), b'v1')

        db[b'k2'] = b'v2'
        self.assertEqual(self.db.get_bulk([b'k1', b'k2']),
                         {'k1': b'v1', 'k2': b'v2'})


class TestKyotoTycoonMultiDatabase(BaseTestCase):
    server = EmbeddedServer
    server_kwargs = {'database': '%', 'server_args': ['*']}

    def test_multiple_databases_present(self):
        report = self.db.report()
        self.assertTrue('db_0' in report)
        self.assertTrue('db_1' in report)
        self.assertTrue(report['db_0'].endswith('path=*'))
        self.assertTrue(report['db_1'].endswith('path=%'))

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


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
