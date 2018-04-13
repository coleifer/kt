![](http://media.charlesleifer.com/blog/photos/logo.png)

Fast bindings to kyototycoon and tokyotyrant.

* Binary APIs implemented as C extension.
* Thread-safe and greenlet-safe.
* Simple APIs.
* Full-featured implementation of protocol.

View the [documentation](http://kt-lib.readthedocs.io/en/latest/) for more
info.

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
