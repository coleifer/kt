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
    >>> client = server.client  # Get a client for use with our embedded server.

Kyoto Tycoon
------------

The Kyoto Tycoon section of the usage document assumes you are running
`Kyoto Tycoon <http://fallabs.com/kyototycoon/>`_ on http://localhost:1978

To begin, we'll instantiate the :py:class:`KyotoTycoon` class, which is the
client interface to the database:

.. code-block:: pycon

    >>> from kt import KyotoTycoon
    >>> client = KyotoTycoon(host='127.0.0.1', port=1978)


Tokyo Tyrant
------------

The Tokyo Tyrant section of the usage document assumes you are running
`Tokyo Tyrant <http://fallabs.com/tokyotyrant/>`_ on http://localhost:9871

To begin, we'll instantiate the :py:class:`TokyoTyrant` class, which is the
client interface to the database:

.. code-block:: pycon

    >>> from kt import KyotoTycoon
    >>> client = KyotoTycoon(host='127.0.0.1', port=1978)
