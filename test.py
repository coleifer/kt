from kt import KyotoTycoon


kt = KyotoTycoon()

kt.set('foo', 'bar')
assert kt.get('foo') == 'bar'

assert kt.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}) == 3

ret = kt.get_bulk(['k1', 'k2', 'k3', 'k4'])
assert ret == {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}

assert kt.remove('foo') == 1
assert kt.remove('foo') == 0

assert kt.remove_bulk(['k1', 'k3', 'kx']) == 2
assert kt.remove_bulk([]) == 0
assert kt.remove_bulk(['k2']) == 1

kt.append('foo', 'nug')
kt.append('foo', 'nuggz')
assert kt['foo'] == 'nugnuggz'

assert kt.replace('foo', 'baze')
assert kt.seize('foo') == 'baze'
assert not kt.seize('foo')
assert not kt.replace('foo', 'nug')
assert kt.add('foo', 'pug')
assert not kt.add('foo', 'rug')
assert kt.get('foo') == 'pug'

assert kt.cas('foo', 'pug', 'zug')
assert not kt.cas('foo', 'pug', 'fug')
assert kt['foo'] == 'zug'

assert 'foo' in kt
assert 'bar' not in kt
assert len(kt) == 1

assert kt.clear()
assert len(kt) == 0
