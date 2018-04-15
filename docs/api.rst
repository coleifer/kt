.. _api:

API
===

Serializers
-----------

.. py:data:: KT_BINARY

    Default value serialization. Serializes values as UTF-8 byte-strings and
    deserializes to unicode.

.. py:data:: KT_JSON

    Serialize values as JSON (encoded as UTF-8).

.. py:data:: KT_MSGPACK

    Uses ``msgpack`` to serialize and deserialize values.

.. py:data:: KT_NONE

    No serialization or deserialization. Values must be byte-strings.

.. py:data:: KT_PICKLE

    Serialize and deserialize using Python's pickle module.

Kyoto Tycoon client
-------------------

.. py:class:: KyotoTycoon(host='127.0.0.1', port=1978, serializer=KT_BINARY, decode_keys=True, timeout=None, default_db=0)

    :param str host: server host.
    :param int port: server port.
    :param serializer: serialization method to use for storing/retrieving values.
        Accepts ``KT_BINARY``, ``KT_JSON``, ``KT_MSGPACK``, ``KT_NONE`` or ``KT_PICKLE``.
    :param bool decode_keys: allow unicode keys, encoded as UTF-8.
    :param int timeout: socket timeout (optional).
    :param int default_db: default database to operate on.

    Client for interacting with Kyoto Tycoon database.

    .. py:method:: checkin()

        Return the communication socket to the pool for re-use.

    .. py:method:: close()

        Close the connection to the server.

    .. py:method:: get(key, db=None)

        :param str key: key to look-up
        :param db: database index
        :type db: int or None
        :return: deserialized value or ``None`` if key does not exist.

    .. py:method:: set(key, value, db=None, expire_time=None)

        :param str key: key to set
        :param value: value to store (will be serialized using serializer)
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: number of rows set (1)

    .. py:method:: remove(key, db=None)

        :param str key: key to remove
        :param db: database index
        :type db: int or None
        :return: number of rows removed

    .. py:method:: get_bulk(keys, db=None)

        :param list keys: list of keys to look-up
        :param db: database index
        :type db: int or None
        :return: dictionary of all key/value pairs that were found
        :rtype: dict

    .. py:method:: set_bulk(__data=None, db=None, expire_time=None, **kwargs)

        :param dict __data: mapping of key/value pairs to set.
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :param kwargs: mapping of key/value pairs to set, expressed as keyword arguments
        :return: number of keys that were set

    .. py:method:: remove_bulk(keys, db=None)

        :param list keys: list of keys to remove
        :param db: database index
        :type db: int or None
        :return: number of keys that were removed

    .. py:method:: script(name, __data=None, encode_values=True, **kwargs)

        :param str name: name of lua function to call
        :param dict __data: mapping of key/value pairs to pass to lua function.
        :param bool encode_values: serialize values passed to lua function.
        :param kwargs: mapping of key/value pairs to pass to lua function, expressed as keyword arguments
        :return: dictionary of key/value pairs returned by function
        :rtype: dict

        Execute a lua function. Kyoto Tycoon lua extensions accept arbitrary
        key/value pairs as input, and return a result dictionary. If
        ``encode_values`` is ``True``, the input values will be serialized and
        the result values will be deserialized using the client's serializer.

    .. py:method:: clear(db=None)

        :param db: database index
        :type db: int or None
        :return: boolean indicating success

        Remove all keys from the database.

    .. py:method:: status(db=None)

        :param db: database index
        :type db: int or None
        :return: status fields and values
        :rtype: dict

        Obtain status information from the server about the selected database.

    .. py:method:: report()

        :return: status fields and values
        :rtype: dict

        Obtain report on overall status of server, including all databases.

    .. py:method:: add(key, value, db=None, expire_time=None)

        :param str key: key to add
        :param value: value to store (will be serialized using serializer)
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: boolean indicating if key could be added or not
        :rtype: bool

        Add a key/value pair to the database. This operation will only succeed
        if the key does not already exist in the database.

    .. py:method:: replace(key, value, db=None, expire_time=None)

        :param str key: key to replace
        :param value: value to store (will be serialized using serializer)
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: boolean indicating if key could be replaced or not
        :rtype: bool

        Replace a key/value pair to the database. This operation will only
        succeed if the key alreadys exist in the database.

    .. py:method:: append(key, value, db=None, expire_time=None)

        :param str key: key to append value to
        :param value: data to append (will be serialized using serializer)
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: boolean indicating if value was appended
        :rtype: bool

        Appends data to an existing key/value pair. If the key does not exist,
        this is equivalent to :py:meth:`~KyotoTycoon.set`.

    .. py:method:: exists(key, db=None)

        :param str key: key to test
        :param db: database index
        :type db: int or None
        :return: boolean indicating if key exists
        :rtype: bool

    .. py:method:: seize(key, db=None)

        :param str key: key to remove
        :param db: database index
        :type db: int or None
        :return: value stored at given key or ``None`` if key does not exist.

        Get and remove the data stored in a given key.

    .. py:method:: cas(key, old_val, new_val, db=None, expire_time=None)

        :param str key: key to append value to
        :param old_val: original value to test
        :param old_val: new value to store
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: boolean indicating if compare-and-swap succeeded.
        :rtype: bool

        Compare-and-swap the value stored at a given key.

    .. py:method:: incr(key, n=1, orig=None, db=None, expire_time=None)

        :param str key: key to increment
        :param int n: value to add
        :param int orig: default value if key does not exist
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: new value at key
        :rtype: int

    .. py:method:: incr_double(key, n=1., orig=None, db=None, expire_time=None)

        :param str key: key to increment
        :param float n: value to add
        :param float orig: default value if key does not exist
        :param db: database index
        :type db: int or None
        :param expire_time: expiration time in seconds
        :type expire_time: int or None
        :return: new value at key
        :rtype: float

    .. py:method:: __getitem__(key_or_keydb)

        Item-lookup based on either ``key`` or a 2-tuple consisting of
        ``(key, db)``. Follows same semantics as :py:meth:`~KyotoTycoon.get`.

    .. py:method:: __setitem__(key_or_keydb, value_or_valueexpire)

        Item-setting based on either ``key`` or a 2-tuple consisting of
        ``(key, db)``. Value consists of either a ``value`` or a 2-tuple
        consisting of ``(value, expire_time)``. Follows same semantics
        as :py:meth:`~KyotoTycoon.set`.

    .. py:method:: __delitem__(key_or_keydb)

        Item-deletion based on either ``key`` or a 2-tuple consisting of
        ``(key, db)``. Follows same semantics as :py:meth:`~KyotoTycoon.remove`.

    .. py:method:: __contains__(key_or_keydb)

        Check if key exists. Accepts either ``key`` or a 2-tuple consisting of
        ``(key, db)``. Follows same semantics as :py:meth:`~KyotoTycoon.exists`.

    .. py:method:: __len__()

        :return: total number of keys in the default database.
        :rtype: int

    .. py:method:: update(__data=None, db=None, expire_time=None, **kwargs)

        See :py:meth:`KyotoTycoon.set_bulk`.

    .. py:method:: pop(key, db=None)

        See :py:meth:`KyotoTycoon.seize`.

    .. py:method:: match_prefix(prefix, max_keys=None, db=None)

        :param str prefix: key prefix to match
        :param int max_keys: maximum number of results to return (optional)
        :param db: database index
        :type db: int or None
        :return: list of keys that matched the given prefix.
        :rtype: list

    .. py:method:: match_regex(regex, max_keys=None, db=None)

        :param str regex: regular-expression to match
        :param int max_keys: maximum number of results to return (optional)
        :param db: database index
        :type db: int or None
        :return: list of keys that matched the given regular expression.
        :rtype: list

    .. py:method:: match_similar(origin, distance=None, max_keys=None, db=None)

        :param str origin: source string for comparison
        :param int distance: maximum edit-distance for similarity (optional)
        :param int max_keys: maximum number of results to return (optional)
        :param db: database index
        :type db: int or None
        :return: list of keys that were within a certain edit-distance of origin
        :rtype: list

    .. py:method:: keys(db=None)

        :param db: database index
        :type db: int or None
        :return: list of all keys in database
        :rtype: list

    .. py:attribute:: size

        Property which exposes the size information returned by the
        :py:meth:`~KyotoTycoon.status` API, for the default database.

    .. py:attribute:: path

        Property which exposes the filename/path returned by the
        :py:meth:`~KyotoTycoon.status` API, for the default database.

    .. py:method:: set_database(db)

        :param int db: database index

        Specify the default database for the client.

Tokyo Tyrant client
-------------------

.. py:class:: TokyoTyrant(host='127.0.0.1', port=1978, serializer=KT_BINARY, decode_keys=True, timeout=None)

    :param str host: server host.
    :param int port: server port.
    :param serializer: serialization method to use for storing/retrieving values.
        Accepts ``KT_BINARY``, ``KT_JSON``, ``KT_MSGPACK``, ``KT_NONE`` or ``KT_PICKLE``.
    :param bool decode_keys: allow unicode keys, encoded as UTF-8.
    :param int timeout: socket timeout (optional).
    :param int default_db: default database to operate on.

    Client for interacting with Tokyo Tyrant database.

    .. py:method:: checkin()

        Return the communication socket to the pool for re-use.

    .. py:method:: close()

        Close the connection to the server.

    .. py:method:: get(key)

        :param str key: key to look-up
        :return: deserialized value or ``None`` if key does not exist.

    .. py:method:: set(key, value)

        :param str key: key to set
        :param value: value to store (will be serialized using serializer)
        :return: boolean indicating success

    .. py:method:: remove(key)

        :param str key: key to remove
        :return: number of rows removed

    .. py:method:: get_bulk(keys)

        :param list keys: list of keys to look-up
        :return: dictionary of all key/value pairs that were found
        :rtype: dict

    .. py:method:: set_bulk(__data=None, **kwargs)

        :param dict __data: mapping of key/value pairs to set.
        :param kwargs: mapping of key/value pairs to set, expressed as keyword arguments
        :return: boolean indicating success

    .. py:method:: remove_bulk(keys)

        :param list keys: list of keys to remove
        :return: boolean indicating success

    .. py:method:: script(name, key=None, value=None)

        :param str name: name of lua function to call
        :param str key: key to pass to lua function (optional)
        :param str value: value to pass to lua function (optional)
        :return: byte-string returned by function
        :rtype: bytes

        Execute a lua function. Tokyo Tyrant lua extensions accept two
        parameters, a key and a value, and return a result byte-string.

    .. py:method:: clear()

        :return: boolean indicating success

        Remove all keys from the database.

    .. py:method:: status()

        :return: status fields and values
        :rtype: dict

        Obtain status information from the server.

    .. py:method:: add(key, value)

        :param str key: key to add
        :param value: value to store (will be serialized using serializer)
        :return: boolean indicating if key could be added or not
        :rtype: bool

        Add a key/value pair to the database. This operation will only succeed
        if the key does not already exist in the database.

    .. py:method:: append(key, value)

        :param str key: key to append value to
        :param value: data to append (will be serialized using serializer)
        :return: boolean indicating if value was appended
        :rtype: bool

        Appends data to an existing key/value pair. If the key does not exist,
        this is equivalent to :py:meth:`~TokyoTyrant.set`.

    .. py:method:: get_part(key, start=None, end=None)

        :param str key: key to look-up
        :param int start: start offset
        :param int end: number of characters to retrieve (after start).
        :return: the substring portion of value requested or ``False`` if the
            value does not exist or the start index exceeded the value length.

    .. py:method:: exists(key)

        :param str key: key to test
        :return: boolean indicating if key exists
        :rtype: bool

    .. py:method:: incr(key, n=1)

        :param str key: key to increment
        :param int n: value to add
        :return: new value at key
        :rtype: int

    .. py:method:: incr_double(key, n=1.)

        :param str key: key to increment
        :param float n: value to add
        :return: new value at key
        :rtype: float

    .. py:method:: misc(cmd, keys=None, data=None)

        :param str cmd: Command to execute
        :param list keys: List of arguments for command
        :param dict data: Key/value data for command

        Run a miscellaneous command using the "misc" API. Returns different
        values depending on command being executed.

    .. py:method:: __getitem__(key)

        Get value at given ``key``. Identical to :py:meth:`~TokyoTyrant.get`.

        .. note::
            If the database is a tree, a slice of keys can be used to retrieve
            an ordered range of values.

    .. py:method:: __setitem__(key, value)

        Set value at given ``key``. Identical to :py:meth:`~TokyoTyrant.set`.

    .. py:method:: __delitem__(key)

        Remove the given ``key``. Identical to :py:meth:`~TokyoTyrant.remove`.

    .. py:method:: __contains__(key)

        Check if given ``key`` exists. Identical to :py:meth:`~TokyoTyrant.exists`.

    .. py:method:: __len__()

        :return: total number of keys in the database.
        :rtype: int

    .. py:method:: update(__data=None, db=None, expire_time=None, **kwargs)

        See :py:meth:`TokyoTyrant.set_bulk`.

    .. py:attribute:: size

        Property which exposes the size of the database.

    .. py:attribute:: error

        Return the error message for the last error reported by the server.

    .. py:method:: get_range(start, stop=None, max_keys=0)

        :param str start: start-key for range
        :param str stop: stop-key for range (optional)
        :param int max_keys: maximum keys to fetch
        :return: a mapping of key-value pairs falling within the given range.
        :rtype: dict

        .. note:: Only works with tree databases.

    .. py:method:: match_prefix(prefix, max_keys=1024)

        :param str prefix: key prefix to match
        :param int max_keys: maximum number of results to return
        :return: list of keys that matched the given prefix.
        :rtype: list

    .. py:method:: match_regex(regex, max_keys=1024)

        :param str regex: regular-expression to match
        :param int max_keys: maximum number of results to return
        :return: list of keys that matched the given regular expression.
        :rtype: list

    .. py:method:: iter_from(start_key)

        :param start_key: key to start iteration.
        :return: list of key/value pairs obtained by iterating from start-key.
        :rtype: dict

    .. py:method:: keys()

        :return: list of all keys in database
        :rtype: list


Embedded Servers
----------------

.. py:class:: EmbeddedServer(server='ktserver', host='127.0.0.1', port=None, database='*', server_args=None)

    :param str server: path to ktserver executable
    :param str host: host to bind server on
    :param int port: port to use (optional)
    :param str database: database filename, default is in-memory hash table
    :param list server_args: additional command-line arguments for server

    Create a manager for running an embedded (sub-process) Kyoto Tycoon server.
    If the port is not specified, a random high port will be used.

    Example:

    .. code-block:: pycon

        >>> from kt import EmbeddedServer
        >>> server = EmbeddedServer()
        >>> server.run()
        True
        >>> client = server.client
        >>> client.set('k1', 'v1')
        1
        >>> client.get('k1')
        'v1'
        >>> server.stop()
        True

    .. py:method:: run()

        :return: boolean indicating if server successfully started

        Run ``ktserver`` in a sub-process.

    .. py:method:: stop()

        :return: boolean indicating if server was stopped

        Stop the running embedded server.

    .. py:attribute:: client

        :py:class:`KyotoTycoon` client bound to the embedded server.


.. py:class:: EmbeddedTokyoTyrantServer(server='ttserver', host='127.0.0.1', port=None, database='*', server_args=None)

    :param str server: path to ttserver executable
    :param str host: host to bind server on
    :param int port: port to use (optional)
    :param str database: database filename, default is in-memory hash table
    :param list server_args: additional command-line arguments for server

    Create a manager for running an embedded (sub-process) Tokyo Tyrant server.
    If the port is not specified, a random high port will be used.

    Example:

    .. code-block:: pycon

        >>> from kt import EmbeddedTokyoTyrantServer
        >>> server = EmbeddedTokyoTyrantServer()
        >>> server.run()
        True
        >>> client = server.client
        >>> client.set('k1', 'v1')
        True
        >>> client.get('k1')
        'v1'
        >>> server.stop()
        True

    .. py:method:: run()

        :return: boolean indicating if server successfully started

        Run ``ttserver`` in a sub-process.

    .. py:method:: stop()

        :return: boolean indicating if server was stopped

        Stop the running embedded server.

    .. py:attribute:: client

        :py:class:`TokyoTyrant` client bound to the embedded server.
