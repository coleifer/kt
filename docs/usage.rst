.. _usage:

Usage
=====

This document describes how to use *kt* with Kyoto Tycoon and Tokyo Tyrant.

Common Features
---------------

This section describes features and APIs that are common to both the
:py:class:`KyotoTycoon` client and the :py:class:`TokyoTyrant` client. For
simplicity, we'll use the :py:class:`EmbeddedServer`, which sets up the
database server in a subprocess and makes it easy to develop.

.. code-block:: pycon

    >>> from kt import EmbeddedServer
    >>> server = EmbeddedServer()
    >>> server.run()  # Starts "ktserver" in a subprocess.
    True
    >>> client = server.client  # Get a client for use with our embedded server.

As you would expect for a key/value database, the client implements
:py:meth:`~KyotoTycoon.get`, :py:meth:`~KyotoTycoon.set` and
:py:meth:`~KyotoTycoon.remove`:

.. code-block:: pycon

    >>> client.set('k1', 'v1')
    1
    >>> client.get('k1')
    'v1'
    >>> client.remove('k1')
    1

It is not an error to try to get or delete a key that doesn't exist:

.. code-block:: pycon

    >>> client.get('not-here')  # Returns None
    >>> client.remove('not-here')
    0

To check whether a key exists we can use :py:meth:`~KyotoTycoon.exists`:

.. code-block:: pycon

    >>> client.set('k1', 'v1')
    >>> client.exists('k1')
    True
    >>> client.exists('not-here')
    False

In addition, there are also efficient methods for bulk operations:
:py:meth:`~KyotoTycoon.get_bulk`, :py:meth:`~KyotoTycoon.set_bulk` and
:py:meth:`~KyotoTycoon.remove_bulk`:

.. code-block:: pycon

    >>> client.set_bulk({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
    3
    >>> client.get_bulk(['k1', 'k2', 'k3', 'not-here'])
    {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
    >>> client.remove_bulk(['k1', 'k2', 'k3', 'not-here'])
    3

The client libraries also support a dict-like interface:

.. code-block:: pycon

    >>> client['k1'] = 'v1'
    >>> print(client['k1'])
    v1
    >>> del client['k1']
    >>> client.update({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
    3
    >>> client.pop('k1')
    'v1'
    >>> client.pop('k1')  # Returns None
    >>> 'k1' in client
    False
    >>> len(client)
    2

To remove all records, you can use the :py:meth:`~KyotoTycoon.clear` method:

.. code-block:: pycon

    >>> client.clear()
    True

Serialization
^^^^^^^^^^^^^

By default the client will assume that keys and values should be encoded as
UTF-8 byte-strings and decoded to unicode upon retrieval. You can set the
``serializer`` parameter when creating your client to use a different value
serialization. *kt* provides the following:

* ``KT_BINARY`` - **default**, treat values as unicode and serialize as UTF-8.
* ``KT_JSON`` - use JSON to serialize values.
* ``KT_MSGPACK`` - use msgpack to serialize values.
* ``KT_PICKLE`` - use pickle to serialize values.
* ``KT_NONE`` - no serialization, values must be bytestrings.

For example, to use the pickle serializer:

.. code-block:: pycon

    >>> from kt import KT_PICKLE, KyotoTycoon
    >>> client = KyotoTycoon(serializer=KT_PICKLE)
    >>> client.set('k1', {'this': 'is', 'a': ['python object']})
    1
    >>> client.get('k1')
    {'this': 'is', 'a': ['python object']}

Kyoto Tycoon
------------

The Kyoto Tycoon section continues from the previous section, and assumes that
you are running an :py:class:`EmbeddedServer` and accessing it through it's
:py:attr:`~EmbeddedServer.client` property.

Database filenames
^^^^^^^^^^^^^^^^^^

Kyoto Tycoon determines the database type by looking at the filename of the
database(s) specified when ``ktserver`` is executed. Additionally, for
in-memory databases, you use special symbols instead of filenames.

* ``hash_table.kch`` - on-disk hash table ("kch").
* ``btree.kct`` - on-disk b-tree ("kct").
* ``dirhash.kcd`` - directory hash ("kcd").
* ``dirtree.kcf`` - directory b-tree ("kcf").
* ``*`` - cache-hash, in-memory hash-table with LRU deletion.
* ``%`` - cache-tree, in-memory b-tree (ordered cache).
* ``:`` - stash db, in-memory database with lower memory usage.
* ``-`` - prototype hash, simple in-memory hash using ``std::unordered_map``.
* ``+`` - prototype tree, simple in-memory hash using ``std::map`` (ordered).

Generally:

* For unordered collections, use either the cache-hash (``*``) or the
  file-hash (``.kch``).
* For ordered collections or indexes, use either the cache-tree (``%``) or the
  file b-tree (``.kct``).
* I avoid the prototype hash and btree as the entire data-structure is locked
  during writes (as opposed to an individual record or page).

For more information about the above database types, their algorithmic
complexity, and the unit of locking, see `kyotocabinet db chart <http://fallabs.com/kyotocabinet/spex.html#tutorial_dbchart>`_.

Key Expiration
^^^^^^^^^^^^^^

Kyoto Tycoon servers feature a built-in expiration mechanism, allowing you to
use it as a cache. Whenever setting a value or otherwise writing to the
database, you can also specify an expiration time (in seconds):

.. code-block:: pycon

    >>> client.set('k1', 'v1', expire_time=5)
    >>> client.get('k1')
    'v1'
    >>> time.sleep(5)
    >>> client.get('k1')  # Returns None

Multiple Databases
^^^^^^^^^^^^^^^^^^

Kyoto Tycoon can also be used as the front-end for multiple databases. For
example, to start ``ktserver`` with an in-memory hash-table and an in-memory
b-tree, you would run:

.. code-block:: bash

    $ ktserver \* \%

By default, the :py:class:`KyotoTycoon` client assumes you are working with the
first database (starting from zero, our hash-table would be ``0`` and the
b-tree would be ``1``).

The client can be initialized to use a different database by default:

.. code-block:: pycon

    >>> client = KyotoTycoon(default_db=1)

To change the default database at run-time, you can call the
:py:meth:`~KyotoTycoon.set_database` method:

.. code-block:: pycon

    >>> client = KyotoTycoon()
    >>> client.set_database(1)

Lastly, to perform a one-off operation against a specific database, all methods
accept a ``db`` parameter which you can use to specify the database:

.. code-block:: pycon

    >>> client.set('k1', 'v1', db=1)
    >>> client.get('k1', db=0)  # Returns None
    >>> client.get('k1', db=1)
    'v1'

Similarly, if a ``tuple`` is passed into the dictionary APIs, it is assumed
that the key consists of ``(key, db)`` and the value of ``(value, expire)``:

.. code-block:: pycon

    >>> client['k1', 1] = 'v1'  # Set k1=v1 in db1.
    >>> client['k1', 1]
    'v1'
    >>> client['k2'] = ('v2', 10)  # Set k2=v2 in default db with 10s expiration.
    >>> client['k2', 0] = ('v2', 20)  # Set k2=v2 in db0 with 20s expiration.
    >>> del client['k1', 1]  # Delete 'k1' in db1.

Lua Scripts
^^^^^^^^^^^

Kyoto Tycoon can be scripted using `lua <http://fallabs.com/kyototycoon/luadoc/index.html>`_.
To run a Lua script from the client, you can use the
:py:meth:`~KyotoTycoon.script` method. In Kyoto Tycoon, a script may receive
arbitrary key/value-pairs as parameters, and may return arbitrary key/value
pairs:

.. code-block:: pycon

    >>> client.script('myfunction', {'key': 'some-key', 'data': 'etc'})
    {'data': 'returned', 'by': 'user-script'}

To simplify script execution, you can also use the :py:meth:`~KyotoTycoon.lua`
helper, which provides a slightly more Pythonic API:

.. code-block:: pycon

    >>> lua = client.lua
    >>> lua.myfunction(key='some-key', data='etc')
    {'data': 'returned', 'by': 'user-script'}
    >>> lua.another_function(key='another-key')
    {}

Learn more about scripting Kyoto Tycoon by reading the `lua doc <http://fallabs.com/kyototycoon/luadoc/index.html>`_.

Tokyo Tyrant
------------

To experiment with Tokyo Tyrant, an easy way to get started is to use the
:py:class:`EmbeddedTokyoTyrantServer`, which sets up the database server in a
subprocess and makes it easy to develop.

.. code-block:: pycon

    >>> from kt import EmbeddedTokyoTyrantServer
    >>> server = EmbeddedTokyoTyrantServer()
    >>> server.run()
    True
    >>> client = server.client

.. note::
    Unlike Kyoto Tycoon, the Tokyo Tyrant server process can only embed a
    single database, and does not support expiration.

Database filenames
^^^^^^^^^^^^^^^^^^

Tokyo Tyrant determines the database type by looking at the filename of the
database(s) specified when ``ttserver`` is executed. Additionally, for
in-memory databases, you use special symbols instead of filenames.

* ``hash_table.tch`` - on-disk hash table ("tch").
* ``btree.tcb`` - on-disk b-tree ("tcb").
* ``*`` - in-memory hash-table.
* ``+`` - in-memory tree (ordered).

There are two additional database-types, but their usage is beyond the scope of
this document:

* ``table.tct`` - on-disk table database ("tct").
* ``table.tcf`` - fixed-length database ("tcf").

The tree database is neat, as it you can store another layer of key/value pairs
in the value field. These key/value pairs are serialized using ``0x0`` as the
delimiter, and by default :py:class:`TokyoTyrant` does not do any special
handling for these values.

For more information about the above database types, their algorithmic
complexity, and the unit of locking, see `ttserver documentation <http://fallabs.com/tokyotyrant/spex.html#serverprog>`_.

Lua Scripts
^^^^^^^^^^^

Tokyo Tyrant can be scripted using `lua <http://fallabs.com/tokyotyrant/spex.html#luaext>`_.
To run a Lua script from the client, you can use the
:py:meth:`~TokyoTyrant.script` method. In Tokyo Tyrant, a script may receive
a key and a value parameter, and will return a byte-string as a result:

.. code-block:: pycon

    >>> client.script('incr', key='counter', value='1')
    '1'
    >>> client.script('incr', 'counter', '4')
    '5'

To simplify script execution, you can also use the :py:meth:`~TokyoTyrant.lua`
helper, which provides a slightly more Pythonic API:

.. code-block:: pycon

    >>> lua = client.lua
    >>> lua.incr(key='counter', value='2')
    '7'
    >>> lua.incr('counter', '1')
    '8'

Learn more about scripting Tokyo Tyrant by reading the `lua docs <http://fallabs.com/tokyotyrant/spex.html#luaext>`_.
