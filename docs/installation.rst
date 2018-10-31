.. _installation:

Installation
============

*kt* can be installed using ``pip``:

.. code-block:: bash

    $ pip install kt

Dependencies
------------

* *cython* - for building the binary protocol C extension.

These libraries are installed automatically if you install *kt* with pip. To
install these dependencies manually, run:

.. code-block:: bash

    $ pip install cython

Installing with git
-------------------

To install the latest version with git:

.. code-block:: bash

    $ git clone https://github.com/coleifer/kt
    $ cd kt/
    $ python setup.py install

Installing Kyoto Tycoon or Tokyo Tyrant
---------------------------------------

If you're using a debian-based linux distribution, you can install using
``apt-get``:

.. code-block:: bash

    $ sudo apt-get install kyototycoon tokyotyrant

Alternatively you can use the following Docker images:

.. code-block:: bash

    $ docker run -it --rm -v kyoto:/var/lib/kyototycoon -p 1978:1978 coleifer/kyototycoon
    $ docker run -it --rm -v tokyo:/var/lib/tokyotyrant -p 9871:9871 coleifer/tokyohash

To build from source and read about the various command-line options, see the
project documentation:

* `Kyoto Tycoon documentation <http://fallabs.com/kyototycoon/>`_
* `Tokyo Tyrant documentation <http://fallabs.com/tokyotyrant/>`_
