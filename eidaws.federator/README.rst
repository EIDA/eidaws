=========================
EIDA Federator webservice 
=========================

Asynchronous implementation of a federating gateway webservice as part of EIDA
NG. Federation is performed for

- *fdsnws-station*
- *fdsnws-dataselect*
- *eidaws-wfcatalog*

webservices across all EIDA datacenters (DCs). This means a client can request
data from an ``eidaws-federator`` resource without having to know where the data
is actually hosted. In order to discover the information location the
``eidaws-stationlite`` webservice is used.

The implementation is based on Python's `aiohttp
<https://docs.aiohttp.org/en/stable/>`_ framework.


Features
========

- Requests are resolved to stream level
- Streamed responses
- Budget for endpoint requests i.e. endpoints are temporarily excluded from
  federation in case of erroneous behaviour
- Optional frontend cache powered by `Redis <https://redis.io/>`_
- HTTP connection pooling to increase both performance and to limit the number
  of concurrent endpoint requests configurable on federated resource granularity
- Support for both HTTP **POST** and **GET** endpoint request method
- Full support for virtual networks
- ``fdsnws-station`` requests run entirely in-memory
- ``fdsnws-dataselect`` and ``eidaws-wfcatalog`` request are buffered in-memory
  for small chunks of data; larger chunks of data are dynamically moved to disk
- ``fdsnws-dataselect`` and ``eidaws-wfcatalog``  response data is returned
  merged and fully aligned when splitting large requests
- Independent configuration of federated resources


Installation
============

Standalone
----------

First of all, choose an installation directory:

.. code::

  $ export PATH_INSTALLATION_DIRECTORY=$HOME/work


**Dependencies**:

Make sure the following software is installed:

- `libxml2 <http://xmlsoft.org/>`_
- `libxslt <http://xmlsoft.org/XSLT/>`_

Regarding the version to be used visit http://lxml.de/installation.html#requirements.

To install the required development packages of these dependencies on Linux
systems, use your distribution specific installation tool, e.g. apt-get on
Debian/Ubuntu:

.. code::

  $ sudo apt-get install libxml2-dev libxslt-dev python3-dev python3-venv


**Download**:

Clone the repository:

.. code::

  $ mkdir -p $PATH_INSTALLATION_DIRECTORY
  $ cd $PATH_INSTALLATION_DIRECTORY && git clone https://github.com/damb/eidaws.git


**Installation**:

In order to install ``eidaws.federator`` services, invoke

.. code::

  $ pip install eidaws.utils eidaws.federator

Note, that encapsulating the installation by means of a `virtual environment
<https://docs.python.org/3/tutorial/venv.html>`_ is strongly recommended.


**Running**:

Federated resources are implemented as standalone applications with respect to
the resources' pathes. Thus, implementations of the following services are
provided:

  - ``eidaws-wfcatalog-json`` (``/eidaws/wfcatalog/json/1``)
  - ``fdsnws-dataselect-miniseed`` (``/eidaws/dataselect/miniseed/1``)
  - ``fdsnws-station-xml`` (``/eidaws/station/xml/1``)
  - ``fdsnws-station-text``  (``/eidaws/station/text/1``)

After installing the ``eidaws.federator`` distribution with

.. code::

  $ pip install eidaws.utils eidaws.federator

the corresponding standalone applications are available:

  - ``eida-federator-wfcatalog-json``
  - ``eida-federator-dataselect-miniseed``
  - ``eida-federator-station-xml``
  - ``eida-federator-station-text``

Running one of those application is as simple as e.g.

.. code::

  $ eida-federator-wfcatalog-json


Additional information and help is provided when invoking the application with
the ``-h|--help`` flag. E.g.

.. code::

  $ eida-federator-wfcatalog-json -h


Configuration
=============

Federated resource service applications can be configured by means of a `YAML
<https://en.wikipedia.org/wiki/YAML>`_ configuration file. An exemplary fully
documented configuration file is provided under
``eidaws.federator/config/eidaws_config.yml.example``. In order to change the default
configuration make a copy of the example configuration with e.g.

.. code::

  $ cp -v eidaws.federator/config/eidaws_config.yml.example \
    eidaws.federator/config/eidaws_config.yml

and adopt the file according to your needs. Then invoke the corresponding
service application with the ``-c|--config`` flag e.g.

.. code::

  $ eida-federator-wfcatalog-json -c config/eidaws_config.yml


Testing
=======

Make sure that an `Redis <https://redis.io/>`_ server instance is up and
running at ``redis://localhost:6379``.

Required test dependencies can be installed with

.. code::

  $ pip install eidaws.utils eidaws.federator
  $ pip install -r eidaws.federator/requirements/test.txt


In order to run the tests, invoke

.. code::

  $ pytest eidaws.federator


Limitations
===========

- AAI of the *fdsnws-dataselect* service is not implemented yet
- A ``fdsnws-station-xml&level=channel|response`` metadata request including only a
  single datacenter might be quite imperformant compared to a direct request to
  the corresponding datacenter.
- In certain cases, HTTP response codes might be misleading
