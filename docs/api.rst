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

.. py:data:: TT_TABLE

    Special serializer for use with TokyoTyrant's remote table database. Values
    are represented as dictionaries.


Kyoto Tycoon client
-------------------

.. py:class:: KyotoTycoon(host='127.0.0.1', port=1978, serializer=KT_BINARY, decode_keys=True, timeout=None, connection_pool=False, default_db=0)

    :param str host: server host.
    :param int port: server port.
    :param serializer: serialization method to use for storing/retrieving values.
        Accepts ``KT_BINARY``, ``KT_JSON``, ``KT_MSGPACK``, ``KT_NONE`` or ``KT_PICKLE``.
    :param bool decode_keys: allow unicode keys, encoded as UTF-8.
    :param int timeout: socket timeout (optional).
    :param bool connection_pool: use a connection pool to manage sockets.
    :param int default_db: default database to operate on.

    Client for interacting with Kyoto Tycoon database.

    .. py:method:: close(allow_reuse=True)

        :param bool allow_reuse: when the connection pool is enabled, this flag
            indicates whether the connection can be reused. For unpooled
            clients this flag has no effect.

        Close the connection to the server.

    .. py:method:: close_all()

        When using the connection pool, this method can close *all* client
        connections.

    .. py:method:: get_bulk(keys, db=None, decode_values=True)

        :param list keys: keys to retrieve
        :param int db: database index
        :param bool decode_values: decode values using the configured
            serialization scheme.
        :return: result dictionary

        Efficiently retrieve multiple key/value pairs from the database. If a
        key does not exist, it will not be present in the result dictionary.

    .. py:method:: get_bulk_details(keys, db=None, decode_values=True)

        :param list keys: keys to retrieve
        :param int db: database index
        :param bool decode_values: decode values using the configured
            serialization scheme.
        :return: List of tuples: ``(db index, key, value, expire time)``

        Like :py:meth:`~KyotoTycoon.get_bulk`, but the return value is a list
        of tuples with additional information for each key.

    .. py:method:: get_bulk_raw(db_key_list, decode_values=True)

        :param db_key_list: a list of 2-tuples to retrieve: ``(db index, key)``
        :param bool decode_values: decode values using the configured
            serialization scheme.
        :return: result dictionary

        Like :py:meth:`~KyotoTycoon.get_bulk`, except it supports fetching
        key/value pairs from multiple databases. The input is a list of
        2-tuples consisting of ``(db, key)`` and the return value is a
        dictionary of ``key: value`` pairs.

    .. py:method:: get_bulk_raw_details(db_key_list, decode_values=True)

        :param db_key_list: a list of 2-tuples to retrieve: ``(db index, key)``
        :param bool decode_values: decode values using the configured
            serialization scheme.
        :return: List of tuples: ``(db index, key, value, expire time)``

        Like :py:meth:`~KyotoTycoon.get_bulk_raw`, but the return value is a
        list of tuples with additional information for each key.

    .. py:method:: get(key, db=None)

        :param str key: key to look-up
        :param int db: database index
        :return: deserialized value or ``None`` if key does not exist.

        Fetch and (optionally) deserialize the value for the given key.

    .. py:method:: get_bytes(key, db=None)

        :param str key: key to look-up
        :param int db: database index
        :return: raw bytestring value or ``None`` if key does not exist.

        Fetch the value for the given key. The resulting value will not
        be deserialized.

    .. py:method:: set_bulk(data, db=None, expire_time=None, no_reply=False, encode_values=True)

        :param dict data: mapping of key/value pairs to set.
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :param bool encode_values: serialize the values using the configured
            serialization scheme (e.g., ``KT_MSGPACK``).
        :return: number of keys that were set, or ``None`` if ``no_reply``.

        Efficiently set multiple key/value pairs. If given, the provided ``db``
        and ``expire_time`` values will be used for all key/value pairs being
        set.

    .. py:method:: set_bulk_raw(data, no_reply=False, encode_values=True)

        :param list data: a list of 4-tuples: ``(db, key, value, expire time)``
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :param bool encode_values: serialize the values using the configured
            serialization scheme (e.g., ``KT_MSGPACK``).
        :return: number of keys that were set, or ``None`` if ``no_reply``.

        Efficiently set multiple key/value pairs. Unlike
        :py:meth:`~KyotoTycoon.set_bulk`, this method can be used to set
        key/value pairs in multiple databases in a single call, and each key
        can specify its own expire time.

    .. py:method:: set(key, value, db=None, expire_time=None, no_reply=False)

        :param str key: key to set
        :param value: value to store (will be serialized using serializer)
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :return: number of rows set (1)

        Set a single key/value pair.

    .. py:method:: set_bytes(key, value, db=None, expire_time=None, no_reply=False)

        :param str key: key to set
        :param value: raw value to store
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :return: number of rows set (1)

        Set a single key/value pair without encoding the value.

    .. py:method:: remove_bulk(keys, db=None, no_reply=False)

        :param list keys: list of keys to remove
        :param int db: database index
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :return: number of keys that were removed

    .. py:method:: remove_bulk_raw(db_key_list, no_reply=False)

        :param db_key_list: a list of 2-tuples to retrieve: ``(db index, key)``
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :return: number of keys that were removed

        Like :py:meth:`~KyotoTycoon.remove_bulk`, but allows keys to be removed
        from multiple databases in a single call.

    .. py:method:: remove(key, db=None, no_reply=False)

        :param str key: key to remove
        :param int db: database index
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :return: number of rows removed

    .. py:method:: script(name, data=None, no_reply=False, encode_values=True, decode_values=True)

        :param str name: name of lua function to call
        :param dict data: mapping of key/value pairs to pass to lua function.
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :param bool encode_values: serialize values passed to lua function.
        :param bool decode_values: deserialize values returned by lua function.
        :return: dictionary of key/value pairs returned by function

        Execute a lua function. Kyoto Tycoon lua extensions accept arbitrary
        key/value pairs as input, and return a result dictionary. If
        ``encode_values`` is ``True``, the input values will be serialized.
        Likewise, if ``decode_values`` is ``True`` the values returned by the
        Lua function will be deserialized using the configured serializer.

    .. py:method:: clear(db=None)

        :param int db: database index
        :return: boolean indicating success

        Remove all keys from the database.

    .. py:method:: status(db=None)

        :param int db: database index
        :return: status fields and values
        :rtype: dict

        Obtain status information from the server about the selected database.

    .. py:method:: report()

        :return: status fields and values
        :rtype: dict

        Obtain report on overall status of server, including all databases.

    .. py:method:: ulog_list()

        :return: a list of 3-tuples describing the files in the update log.

        Returns a list of metadata about the state of the update log. For each
        file in the update log, a 3-tuple is returned. For example:

        .. code-block:: pycon

            >>> kt.ulog_list()
            [('/var/lib/database/ulog/kt/0000000037.ulog',
              '67150706',
              datetime.datetime(2019, 1, 4, 1, 28, 42, 43000)),
             ('/var/lib/database/ulog/kt/0000000038.ulog',
              '14577366',
              datetime.datetime(2019, 1, 4, 1, 41, 7, 245000))]

    .. py:method:: ulog_remove(max_dt)

        :param datetime max_dt: maximum datetime to preserve
        :return: boolean indicating success

        Removes all update-log files older than the given datetime.

    .. py:method:: synchronize(hard=False, command=None, db=None)

        :param bool hard: perform a "hard" synchronization
        :param str command: command to run after synchronization
        :param int db: database index
        :return: boolean indicating success

        Synchronize the database, optionally executing the given command upon
        success. This can be used to create hot backups, for example.

    .. py:method:: vacuum(step=0, db=None)

        :param int step: number of steps, default is 0
        :param int db: database index
        :return: boolean indicating success

    .. py:method:: add(key, value, db=None, expire_time=None, encode_value=True)

        :param str key: key to add
        :param value: value to store
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool encode_value: serialize the value using the configured
            serialization method.
        :return: boolean indicating if key could be added or not
        :rtype: bool

        Add a key/value pair to the database. This operation will only succeed
        if the key does not already exist in the database.

    .. py:method:: replace(key, value, db=None, expire_time=None, encode_value=True)

        :param str key: key to replace
        :param value: value to store
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool encode_value: serialize the value using the configured
            serialization method.
        :return: boolean indicating if key could be replaced or not
        :rtype: bool

        Replace a key/value pair to the database. This operation will only
        succeed if the key alreadys exist in the database.

    .. py:method:: append(key, value, db=None, expire_time=None, encode_value=True)

        :param str key: key to append value to
        :param value: data to append
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool encode_value: serialize the value using the configured
            serialization method.
        :return: boolean indicating if value was appended
        :rtype: bool

        Appends data to an existing key/value pair. If the key does not exist,
        this is equivalent to :py:meth:`~KyotoTycoon.set`.

    .. py:method:: exists(key, db=None)

        :param str key: key to test
        :param int db: database index
        :return: boolean indicating if key exists

        Return whether or not the given key exists in the database.

    .. py:method:: length(key, db=None)

        :param str key: key
        :param int db: database index
        :return: length of the value in bytes, or ``None`` if not found

        Return the length of the raw value stored at the given key. If the key
        does not exist, returns ``None``.

    .. py:method:: seize(key, db=None, decode_value=True)

        :param str key: key to remove
        :param int db: database index
        :param bool decode_value: deserialize the value using the configured
            serialization method.
        :return: value stored at given key or ``None`` if key does not exist.

        Get and remove the data stored in a given key in a single operation.

    .. py:method:: cas(key, old_val, new_val, db=None, expire_time=None, encode_value=True)

        :param str key: key to append value to
        :param old_val: original value to test
        :param new_val: new value to store
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :param bool encode_value: serialize the old and new values using the
            configured serialization method.
        :return: boolean indicating if compare-and-swap succeeded.
        :rtype: bool

        Compare-and-swap the value stored at a given key.

    .. py:method:: incr(key, n=1, orig=None, db=None, expire_time=None)

        :param str key: key to increment
        :param int n: value to add
        :param int orig: default value if key does not exist
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :return: new value at key
        :rtype: int

        Increment the value stored in the given key.

    .. py:method:: incr_double(key, n=1., orig=None, db=None, expire_time=None)

        :param str key: key to increment
        :param float n: value to add
        :param float orig: default value if key does not exist
        :param int db: database index
        :param int expire_time: expiration time in seconds
        :return: new value at key
        :rtype: float

        Increment the floating-point value stored in the given key.

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

    .. py:method:: count(db=None)

        :param db: database index
        :type db: int or None
        :return: total number of keys in the database.
        :rtype: int

        Count total number of keys in the database.

    .. py:method:: update(__data=None, db=None, expire_time=None, no_reply=False, encode_values=True, **kwargs)

        Efficiently set multiple key/value pairs. If given, the provided ``db``
        and ``expire_time`` values will be used for all key/value pairs being
        set.

        See :py:meth:`KyotoTycoon.set_bulk` for details.

    .. py:method:: pop(key, db=None, decode_value=True)

        Get and remove the data stored in a given key in a single operation.

        See :py:meth:`KyotoTycoon.seize`.

    .. py:method:: match_prefix(prefix, max_keys=None, db=None)

        :param str prefix: key prefix to match
        :param int max_keys: maximum number of results to return (optional)
        :param int db: database index
        :return: list of keys that matched the given prefix.
        :rtype: list

    .. py:method:: match_regex(regex, max_keys=None, db=None)

        :param str regex: regular-expression to match
        :param int max_keys: maximum number of results to return (optional)
        :param int db: database index
        :return: list of keys that matched the given regular expression.
        :rtype: list

    .. py:method:: match_similar(origin, distance=None, max_keys=None, db=None)

        :param str origin: source string for comparison
        :param int distance: maximum edit-distance for similarity (optional)
        :param int max_keys: maximum number of results to return (optional)
        :param int db: database index
        :return: list of keys that were within a certain edit-distance of origin
        :rtype: list

    .. py:method:: cursor(db=None, cursor_id=None)

        :param int db: database index
        :param int cursor_id: cursor id (will be automatically created if None)
        :return: :py:class:`Cursor` object

    .. py:method:: keys(db=None)

        :param int db: database index
        :return: all keys in database
        :rtype: generator

        .. warning::
            The :py:meth:`~KyotoCabinet.keys` method uses a cursor and can be
            rather slow.

    .. py:method:: keys_nonlazy(db=None)

        :param int db: database index
        :return: all keys in database
        :rtype: list

        Non-lazy implementation of :py:meth:`~KyotoTycoon.keys`.
        Behind-the-scenes, calls :py:meth:`~KyotoTycoon.match_prefix` with an
        empty string as the prefix.

    .. py:method:: values(db=None)

        :param int db: database index
        :return: all values in database
        :rtype: generator

    .. py:method:: items(db=None)

        :param int db: database index
        :return: all key/value tuples in database
        :rtype: generator

    .. py:attribute:: size

        Property which exposes the size information returned by the
        :py:meth:`~KyotoTycoon.status` API, for the default database.

    .. py:attribute:: path

        Property which exposes the filename/path returned by the
        :py:meth:`~KyotoTycoon.status` API, for the default database.

    .. py:method:: set_database(db)

        :param int db: database index

        Specify the default database index for the client.

Tokyo Tyrant client
-------------------

.. py:class:: TokyoTyrant(host='127.0.0.1', port=1978, serializer=KT_BINARY, decode_keys=True, timeout=None, connection_pool=False)

    :param str host: server host.
    :param int port: server port.
    :param serializer: serialization method to use for storing/retrieving values.
        Accepts ``KT_BINARY``, ``KT_JSON``, ``KT_MSGPACK``, ``KT_NONE``, ``KT_PICKLE``,
        or ``TT_TABLE`` (for use with table databases).
    :param bool decode_keys: automatically decode keys, encoded as UTF-8.
    :param int timeout: socket timeout (optional).
    :param bool connection_pool: use a connection pool to manage sockets.

    Client for interacting with Tokyo Tyrant database.

    .. py:method:: close(allow_reuse=True)

        :param bool allow_reuse: when the connection pool is enabled, this flag
            indicates whether the connection can be reused. For unpooled
            clients this flag has no effect.

        Close the connection to the server.

    .. py:method:: close_all()

        When using the connection pool, this method can close *all* client
        connections.

    .. py:method:: get_bulk(keys, decode_values=True)

        :param list keys: list of keys to retrieve
        :param bool decode_values: decode values using the configured
            serialization scheme.
        :return: dictionary of all key/value pairs that were found

        Efficiently retrieve multiple key/value pairs from the database. If a
        key does not exist, it will not be present in the result dictionary.

    .. py:method:: get(key)

        :param str key: key to look-up
        :return: deserialized value or ``None`` if key does not exist.

        Fetch and (optionally) deserialize the value for the given key.

    .. py:method:: get_bytes(key)

        :param str key: key to look-up
        :return: raw bytestring value or ``None`` if key does not exist.

        Fetch the value for the given key. The resulting value will not
        be deserialized.

    .. py:method:: set_bulk(data, no_reply=False, encode_values=True)

        :param dict data: mapping of key/value pairs to set.
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :param bool encode_values: serialize the values using the configured
            serialization scheme (e.g., ``KT_MSGPACK``).
        :return: boolean indicating success, or ``None`` if ``no_reply``.

        Efficiently set multiple key/value pairs.

    .. py:method:: set(key, value)

        :param str key: key to set
        :param value: value to store (will be serialized using serializer)
        :return: boolean indicating success

        Set a single key/value pair.

    .. py:method:: set_bytes(key, value)

        :param str key: key to set
        :param value: raw value to store
        :return: boolean indicating success

        Set a single key/value pair without encoding the value.

    .. py:method:: remove_bulk(keys)

        :param list keys: list of keys to remove
        :return: boolean indicating success

    .. py:method:: remove(key)

        :param str key: key to remove
        :return: boolean indicating success

    .. py:method:: script(name, key=None, value=None, lock_records=False, lock_all=False, encode_value=True, decode_result=False, as_list=False, as_dict=False, as_int=False)

        :param str name: name of lua function to call
        :param str key: key to pass to lua function (optional)
        :param str value: value to pass to lua function (optional)
        :param bool lock_records: lock records modified during script execution
        :param bool lock_all: lock all records during script execution
        :param bool encode_value: serialize the value before sending to the script
        :param bool decode_value: deserialize the script return value
        :param bool as_list: deserialize newline-separated value into a list
        :param bool as_dict: deserialize list of tab-separated key/value pairs into dict
        :param bool as_int: return value as integer
        :return: byte-string or object returned by function (depending on decode_value)

        Execute a lua function, passing as arguments the given ``key`` and
        ``value`` (if provided). The return value is a bytestring, which can be
        deserialized by specifying ``decode_value=True``. The arguments
        ``as_list``, ``as_dict`` and ``as_int`` can be used to apply specific
        deserialization to the returned value.

    .. py:method:: clear()

        :return: boolean indicating success

        Remove all keys from the database.

    .. py:method:: status()

        :return: status fields and values
        :rtype: dict

        Obtain status information from the server.

    .. py:method:: synchronize()

        :return: boolean indicating success

        Synchronize data to disk.

    .. py:method:: optimize(options)

        :param str options: option format string to use when optimizing database.
        :return: boolean indicating success

    .. py:method:: add(key, value, encode_value=True)

        :param str key: key to add
        :param value: value to store
        :param bool encode_value: serialize the value using the configured
            serialization scheme.
        :return: boolean indicating if key could be added or not

        Add a key/value pair to the database. This operation will only succeed
        if the key does not already exist in the database.

    .. py:method:: append(key, value, encode_value=True)

        :param str key: key to append value to
        :param value: value to append
        :param bool encode_value: serialize the value using the configured
            serialization scheme.
        :return: boolean indicating if value was appended

        Appends data to an existing key/value pair. If the key does not exist,
        this is equivalent to the :py:meth:`~TokyoTyrant.set` method.

    .. py:method:: addshl(key, value, width, encode_value=True)

        :param str key: key to append value to
        :param value: data to append
        :param int width: number of bytes to shift
        :param bool encode_value: serialize the value using the configured
            serialization scheme.
        :return: boolean indicating success

        Concatenate a value at the end of the existing record and shift it to
        the left by *width* bytes.

    .. py:method:: exists(key)

        :param str key: key to test
        :return: boolean indicating if key exists

        Return whether or not the given key exists in the database.

    .. py:method:: length(key)

        :param str key: key
        :param int db: database index
        :return: length of the value in bytes, or ``None`` if not found

        Return the length of the raw value stored at the given key. If the key
        does not exist, returns ``None``.

    .. py:method:: seize(key, decode_value=True)

        :param str key: key to remove
        :param bool decode_value: deserialize the value using the configured
            serialization method.
        :return: value stored at given key or ``None`` if key does not exist.

        Get and remove the data stored in a given key in a single operation.

    .. py:method:: incr(key, n=1)

        :param str key: key to increment
        :param int n: value to add
        :return: incremented result value

    .. py:method:: incr_double(key, n=1.)

        :param str key: key to increment
        :param float n: value to add
        :return: incremented result value

        Increment the floating-point value stored in the given key.

    .. py:method:: count()

        :return: number of key/value pairs in the database
        :rtype: int

        Count the number of key/value pairs in the database

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

        Identical to :py:meth:`~TokyoTyrant.count`.

    .. py:method:: update(__data=None, no_reply=False, encode_values=True, **kwargs)

        :param dict __data: mapping of key/value pairs to set.
        :param bool no_reply: execute the operation without a server
            acknowledgment.
        :param bool encode_values: serialize the values using the configured
            serialization scheme.
        :param kwargs: arbitrary key/value pairs to set.
        :return: boolean indicating success.

        Efficiently set multiple key/value pairs. Data can be provided as a
        dict or as an arbitrary number of keyword arguments.

        See also: :py:meth:`~TokyoTyrant.set_bulk`.

    .. py:method:: setdup(key, value, encode_value=True)

        :param str key: key to set
        :param value: value to store
        :param bool encode_value: serialize the value using the configured
            serialization scheme.
        :return: boolean indicating success

        Set key/value pair. If using a B-Tree and the key already exists, the
        new value will be added to the beginning.

    .. py:method:: setdupback(key, value)

        :param str key: key to set
        :param value: value to store
        :param bool encode_value: serialize the value using the configured
            serialization scheme.
        :return: boolean indicating success

        Set key/value pair. If using a B-Tree and the key already exists, the
        new value will be added to the end.

    .. py:method:: get_part(key, start=None, end=None, decode_value=True)

        :param str key: key to look-up
        :param int start: start offset
        :param int end: number of characters to retrieve (after start).
        :param bool decode_value: deserialize the value using the configured
            serialization scheme.
        :return: the substring portion of value requested or ``False`` if the
            value does not exist or the start index exceeded the value length.

    .. py:method:: misc(cmd, args=None, update_log=True)

        :param str cmd: Command to execute
        :param list args: Zero or more bytestring arguments to misc function.
        :param bool update_log: Add misc command to update log.

        Run a miscellaneous command using the "misc" API. Returns a list of
        zero or more bytestrings.

    .. py:attribute:: size

        Property which exposes the size of the database.

    .. py:attribute:: error

        Return a 2-tuple of error code and message for the last error reported
        by the server (if set).

    .. py:method:: copy(path)

        :param str path: destination for copy of database.
        :return: boolean indicating success

        Copy the database file to the given path.

    .. py:method:: restore(path, timestamp, options=0)

        :param str path: path to update log directory
        :param datetime timestamp: datetime from which to restore
        :param int options: optional flags
        :return: boolean indicating success

        Restore the database file from the update log.

    .. py:method:: set_master(host, port, timestamp, options=0)

        :param str host: host of master server
        :param int port: port of master server
        :param datetime timestamp: start timestamp
        :param int options: optional flags
        :return: boolean indicating success

        Set the replication master.

    .. py:method:: clear_cache()

        :return: boolean indicating success

    .. py:method:: defragment(nsteps=None)

        :param int nsteps: number of defragmentation steps
        :return: boolean indicating success

        Defragment the database.

    .. py:method:: get_range(start, stop=None, max_keys=0, decode_values=True)

        :param str start: start-key for range
        :param str stop: stop-key for range (optional)
        :param int max_keys: maximum keys to fetch
        :param bool decode_values: deserialize the values using the configured
            serialization scheme.
        :return: a dictionary mapping of key-value pairs falling within the
            given range.

        Fetch a range of key/value pairs and return them as a dictionary.

        .. note:: Only works with tree databases.

    .. py:method:: get_rangelist(start, stop=None, max_keys=0, decode_values=True)

        :param str start: start-key for range
        :param str stop: stop-key for range (optional)
        :param int max_keys: maximum keys to fetch
        :param bool decode_values: deserialize the values using the configured
            serialization scheme.
        :return: a list of ordered key-value pairs falling within the given range.

        Fetch a range of key/value pairs and return them as an ordered list of
        key/value tuples.

        .. note:: Only works with tree databases.

    .. py:method:: match_prefix(prefix, max_keys=1024)

        :param str prefix: key prefix to match
        :param int max_keys: maximum number of results to return
        :return: list of keys that matched the given prefix.

    .. py:method:: match_regex(regex, max_keys=None, decode_values=True)

        :param str regex: regular-expression to match
        :param int max_keys: maximum number of results to return
        :param bool decode_values: deserialize the values using the configured
            serialization scheme.
        :return: a dictionary mapping of key-value pairs which matched the regex.

    .. py:method:: match_regexlist(regex, max_keys=None, decode_values=True)

        :param str regex: regular-expression to match
        :param int max_keys: maximum number of results to return
        :param bool decode_values: deserialize the values using the configured
            serialization scheme.
        :return: a list of ordered key-value pairs which matched the regex.

    .. py:method:: iter_from(start_key)

        :param start_key: key to start iteration.
        :return: list of key/value tuples obtained by iterating from start-key.

    .. py:method:: keys()

        :return: list all keys in database
        :rtype: generator

    .. py:method:: keys_fast()

        :return: list of all keys in database
        :rtype: list

        Return a list of all keys in the database in a single operation.

    .. py:method:: items()

        :return: list all key/value tuples in database
        :rtype: generator

    .. py:method:: items_fast()

        :return: list of all key/value tuples in database in a single operation.
        :rtype: list

    .. py:method:: set_index(name, index_type, check_exists=False)

        :param str name: column name to index
        :param int index_type: see :ref:`index-types` for values
        :param bool check_exists: if true, an error will be raised if the index
            already exists.
        :return: boolean indicating success

        Create an index on the given column in a table database.

    .. py:method:: optimize_index(name)

        :param str name: column name index to optimize
        :return: boolean indicating success

        Optimize the index on a given column.

    .. py:method:: delete_index(name)

        :param str name: column name index to delete
        :return: boolean indicating success

        Delete the index on a given column.

    .. py:method:: search(expressions, cmd=None)

        :param list expressions: zero or more search expressions
        :param str cmd: extra command to apply to search results
        :return: varies depending on ``cmd``.

        Perform a search on a table database. Rather than call this method
        directly, it is recommended that you use the :py:class:`QueryBuilder`
        to construct and execute table queries.

    .. py:method:: genuid()

        :return: integer id

        Generate a unique ID.


.. py:class:: QueryBuilder

    Construct and execute table queries.

    .. py:method:: filter(column, op, value)

        :param str column: column name to filter on
        :param int op: operation, see :ref:`filter-types` for available values
        :param value: value for filter expression

        Add a filter expression to the query.

    .. py:method:: order_by(column, ordering=None)

        :param str column: column name to order by
        :param int ordering: ordering method, defaults to lexical ordering.
            See :ref:`ordering-types` for available values.

        Specify ordering of query results.

    .. py:method:: limit(limit=None)

        :param int limit: maximum number of results

        Limit the number of results returned by query.

    .. py:method:: offset(offset=None)

        :param int offset: number of results to skip over.

        Skip over results returned by query.

    .. py:method:: execute(client)

        :param TokyoTyrant client: database client
        :return: list of keys matching query criteria
        :rtype: list

        Execute the query and return a list of the keys of matching records.

    .. py:method:: delete(client)

        :param TokyoTyrant client: database client
        :return: boolean indicating success

        Delete records that match the query criteria.

    .. py:method:: get(client)

        :param TokyoTyrant client: database client
        :return: list of 2-tuples consisting of ``key, value``.
        :rtype list:

        Execute query and return a list of keys and values for records matching
        the query criteria.

    .. py:method:: count(client)

        :param TokyoTyrant client: database client
        :return: number of query results

        Return count of matching records.


.. _index-types:

Index types
^^^^^^^^^^^

.. py:data:: INDEX_STR

.. py:data:: INDEX_NUM

.. py:data:: INDEX_TOKEN

.. py:data:: INDEX_QGRAM

.. _filter-types:

Filter types
^^^^^^^^^^^^

.. py:data:: OP_STR_EQ

.. py:data:: OP_STR_CONTAINS

.. py:data:: OP_STR_STARTSWITH

.. py:data:: OP_STR_ENDSWITH

.. py:data:: OP_STR_ALL

.. py:data:: OP_STR_ANY

.. py:data:: OP_STR_ANYEXACT

.. py:data:: OP_STR_REGEX

.. py:data:: OP_NUM_EQ

.. py:data:: OP_NUM_GT

.. py:data:: OP_NUM_GE

.. py:data:: OP_NUM_LT

.. py:data:: OP_NUM_LE

.. py:data:: OP_NUM_BETWEEN

.. py:data:: OP_NUM_ANYEXACT

.. py:data:: OP_FTS_PHRASE

.. py:data:: OP_FTS_ALL

.. py:data:: OP_FTS_ANY

.. py:data:: OP_FTS_EXPRESSION

.. py:data:: OP_NEGATE

    Combine with other operand using bitwise-or to negate the filter.

.. py:data:: OP_NOINDEX

    Combine with other operand using bitwise-or to prevent using an index.

.. _ordering-types:

Ordering types
^^^^^^^^^^^^^^

.. py:data:: ORDER_STR_ASC

.. py:data:: ORDER_STR_DESC

.. py:data:: ORDER_NUM_ASC

.. py:data:: ORDER_NUM_DESC

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
