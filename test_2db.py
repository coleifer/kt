from kt import *

k0 = KyotoTycoon(default_db=0)
k1 = KyotoTycoon(default_db=1)

k0.clear()
k1.clear()

# Test basic operations using the binary protocols.

k0.set('k1', 'v1-0')
k0.set('k2', 'v2-0')
assert len(k0) == 2
assert len(k1) == 0
k1.set('k1', 'v1-1')
k1.set('k2', 'v2-1')
assert len(k0) == 2
assert len(k1) == 2

assert k0.get('k1') == 'v1-0'
k0.remove('k1')
assert k0.get('k1') is None

assert k1.get('k1') == 'v1-1'
k1.remove('k1')
assert k1.get('k1') is None

k0.set_bulk({'k1': 'v1-0', 'k3': 'v3-0'})
k1.set_bulk({'k1': 'v1-1', 'k3': 'v3-1'})

assert k0.get_bulk(['k1', 'k2', 'k3']) == {'k1': 'v1-0', 'k2': 'v2-0', 'k3': 'v3-0'}
assert k1.get_bulk(['k1', 'k2', 'k3']) == {'k1': 'v1-1', 'k2': 'v2-1', 'k3': 'v3-1'}

assert k0.remove_bulk(['k3', 'k2']) == 2
assert k0.remove_bulk(['k3', 'k2']) == 0
assert k1.remove_bulk(['k3', 'k2']) == 2
assert k1.remove_bulk(['k3', 'k2']) == 0

assert k0.add('k2', 'v2-0')
assert not k0.add('k2', 'v2-0')

assert k1.add('k2', 'v2-1')
assert not k1.add('k2', 'v2-1')

assert k0['k2'] == 'v2-0'
assert k1['k2'] == 'v2-1'

assert k0.replace('k2', 'v2-0x')
assert not k0.replace('k3', 'v3-0')
assert k1.replace('k2', 'v2-1x')
assert not k1.replace('k3', 'v3-1')

assert k0['k2'] == 'v2-0x'
assert k1['k2'] == 'v2-1x'

assert k0.append('k3', 'v3-0')
assert k0.append('k3', 'x')
assert k1.append('k3', 'v3-1')
assert k1.append('k3', 'x')

assert k0['k3'] == 'v3-0x'
assert k1['k3'] == 'v3-1x'

assert k0.exists('k3')
assert k0.remove('k3') == 1
assert not k0.exists('k3')

assert k1.exists('k3')
assert k1.remove('k3') == 1
assert not k1.exists('k3')

assert k0.seize('k2') == 'v2-0x'
assert k1.seize('k2') == 'v2-1x'

assert k0.cas('k1', 'v1-0', 'v1-0x')
assert not k0.cas('k1', 'v1-0', 'v1-0z')

assert k1.cas('k1', 'v1-1', 'v1-1x')
assert not k1.cas('k1', 'v1-1', 'v1-1z')

assert k0['k1'] == 'v1-0x'
assert k1['k1'] == 'v1-1x'

k0.remove_bulk(['i', 'j'])
k1.remove_bulk(['i', 'j'])
assert k0.incr('i') == 1
assert k0.incr('i') == 2

assert k1.incr('i') == 1
assert k1.incr('i') == 2

assert k0.incr_double('j') == 1.
assert k0.incr_double('j') == 2.

assert k1.incr_double('j') == 1.
assert k1.incr_double('j') == 2.

assert k0['k1'] == 'v1-0x'
assert k0['k1', 1] == 'v1-1x'
assert k1['k1'] == 'v1-1x'
assert k1['k1', 0] == 'v1-0x'

k0['k2'] = 'v2-0y'
k0['k2', 1] = 'v2-1y'
assert k0.get('k2') == 'v2-0y'
assert k1.get('k2') == 'v2-1y'
k1['k2'] = 'v2-1z'
k1['k2', 0] = 'v2-0z'
assert k0.get('k2') == 'v2-0z'
assert k1.get('k2') == 'v2-1z'

del k0['k1']
del k0['k1', 1]
assert k0['k1'] is None
assert k1['k1'] is None
del k1['k2']
del k1['k2', 0]
assert k0['k2'] is None
assert k1['k2'] is None

k0['k3'] = 'v3-0'
k0['k03'] = 'v03'
k1['k3'] = 'v3-1'
k1['k13'] = 'v13'
assert 'k3' in k0
assert 'k03' in k0
assert 'k13' not in k0
assert 'k3' in k1
assert 'k13' in k1
assert 'k03' not in k1

assert sorted(k0.match_prefix('k')) == ['k03', 'k3']
assert sorted(k0.match_prefix('k', db=1)) == ['k13', 'k3']
assert sorted(k1.match_prefix('k')) == ['k13', 'k3']
assert sorted(k1.match_prefix('k', db=0)) == ['k03', 'k3']

assert sorted(k0.match_regex('k')) == ['k03', 'k3']
assert sorted(k0.match_regex('k', db=1)) == ['k13', 'k3']
assert sorted(k1.match_regex('k')) == ['k13', 'k3']
assert sorted(k1.match_regex('k', db=0)) == ['k03', 'k3']

assert sorted(k0.keys()) == ['i', 'j', 'k03', 'k3']
assert sorted(k0.keys(1)) == ['i', 'j', 'k13', 'k3']
assert sorted(k1.keys()) == ['i', 'j', 'k13', 'k3']
assert sorted(k1.keys(0)) == ['i', 'j', 'k03', 'k3']

k0.clear()
assert 'k3' not in k0
assert 'k3' in k1
k1.clear()
assert 'k3' not in k1
