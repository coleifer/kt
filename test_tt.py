from kt.client import TokyoTyrant

tt = TokyoTyrant(port=9871)

tt.clear()
assert tt.set('foo', 'bar') == 1
assert tt.get('foo') == 'bar'
assert tt.append('foo', 'nuggie')
assert tt.get('foo') == 'barnuggie'
assert tt.exists('foo') == 9
assert not tt.add('foo', 'baze')
assert tt.get('foo') == 'barnuggie'
assert tt.get('x') is None
assert tt.add('x', 'y')
assert tt.get_bulk(['foo', 'bar', 'x']) == {'foo': 'barnuggie', 'x': 'y'}
assert tt.incr('z', 1) == 1
assert tt.incr('z', 3) == 4
assert tt.incr_double('d', 3.14) == 3.14
assert round(tt.incr_double('d', 3.15), 2) == 6.29
assert len(tt) == 4
tt.clear()
tt.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
assert sorted(tt.match_prefix('k')) == ['k1', 'k2', 'k3']
assert sorted(tt) == ['k1', 'k2', 'k3']
assert sorted(tt.match_regex('k?')) == ['k1', 'k2', 'k3']
assert tt.misc('get', 'k1') == 'v1'
assert tt.misc('get', 'k3') == 'v3'
assert tt.misc('get', 'kx') is False
assert tt.misc('out', 'k1')
assert not tt.misc('out', 'k1')
assert tt.misc('put', data={'k1': 'v1-x'})
assert tt.misc('get', 'k1') == 'v1-x'
assert tt.misc('put', data={'k1': 'v1-y'})
assert tt.misc('putlist', data={'a': 'A', 'b': 'B'})
assert tt.misc('get', 'k1') == 'v1-y'
assert tt.misc('out', 'k1')
assert tt.misc('out', 'k1') is False
assert tt.misc('get', 'k1') is False
assert tt.misc('put', data={'k1': 'v1-z'})
assert tt.misc('putlist', data={'k1': 'v1-x', 'k2': 'v2-x', 'k3': 'v3-x'})
assert tt.misc('getlist', ['k1', 'k2', 'k3', 'k4', 'k5']) == {'k1': 'v1-x', 'k2': 'v2-x', 'k3': 'v3-x'}
assert tt.misc('getlist', ['k9', 'xz9']) == {}
assert tt.misc('getlist', []) == {}
assert tt.misc('outlist', ['k1', 'k2', 'k3'])
assert tt.misc('outlist', ['k1', 'k2', 'k3'])  # outlist always returns True
assert tt.misc('out', ['k1']) is False  # but out does not.
assert tt.misc('putlist', data={})  # putlist always returns True
assert tt.misc('put', data={}) is False  # but put does not
