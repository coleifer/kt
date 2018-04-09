from kt.client import TokyoTyrant

tt = TokyoTyrant(port=9871)

tt.clear()
assert tt.set('foo', 'bar') == 1
assert tt.get('foo') == 'bar'
assert tt.append('foo', 'nuggie')
assert tt.get('foo') == 'barnuggie'
assert tt.check('foo') == 9
assert not tt.add('foo', 'baze')
assert tt.get('foo') == 'barnuggie'
assert tt.get('x') is None
assert tt.add('x', 'y')
assert tt.get_bulk(['foo', 'bar', 'x']) == {'foo': 'barnuggie', 'x': 'y'}
assert tt.incr('z', 1) == 1
assert tt.incr('z', 3) == 4
assert len(tt) == 3
tt.clear()
