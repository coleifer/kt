### kt

Fast bindings to kyototycoon and tokyotyrant.

[documentation hosted at readthedocs](http://kt-lib.readthedocs.io/en/latest/)

#### installing

```console

$ pip install kt
```

#### usage

```pycon

>>> from kt import KyotoTycoon
>>> client = KyotoTycoon()
>>> client.set('k1', 'v1')
1
>>> client.get('k1')
'v1'
>>> client.remove('k1')
1

>>> client.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
3
>>> client.get_bulk(['k1', 'xx, 'k3'])
{'k1': 'v1', 'k3': 'v3'}
>>> client.remove_bulk(['k1', 'xx', 'k3'])
2
```
