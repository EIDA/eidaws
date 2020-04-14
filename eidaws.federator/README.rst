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

.. note::

  In favor of a simplified versioning scheme, both resource pathes and allowed
  values for the ``format`` query filter parameter are application specific
  and **not** compliant with `FDSN webservice <https://www.fdsn.org/webservices/>`_
  standards. In order to provide a fully `FDSN webservice
  <https://www.fdsn.org/webservices/>`_ conform API consider the deployment
  behind a *reverse proxy*.

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


Now the service should be up and running at ``localhost:8080``. Check out the
`Configuration`_ section if you'd like to run the service on a different
``hostname:port`` destination.


Additional information and help is provided when invoking the application with
the ``-h|--help`` flag. E.g.

.. code::

  $ eida-federator-wfcatalog-json -h


Note, that for production it has several advantages running the services behind
a *reverse proxy server* such as e.g. `nginx <https://nginx.org/en/>`_. In
particular, if providing a fully compliant `FDSNWS
<https://www.fdsn.org/webservices/>`_ API is desired.


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

  $ eida-federator-wfcatalog-json -c eidaws.federator/config/eidaws_config.yml


Logging
=======

The *eidaws.federator* distribution uses standard `logging
<https://docs.python.org/3/library/logging.html#module-logging>`_ for tracking
the application activity.

Depending on the federated resource service the following loggers enumerated by
name are provided:

- ``eidaws.federator.eidaws.wfcatalog.json``
- ``eidaws.federator.fdsnws.dataselect.miniseed``
- ``eidaws.federator.fdsnws.station.text``
- ``eidaws.federator.fdsnws.station.xml``

When configuring logging by means of a logging configuration file (see also the
`Configuration`_ section), you may subscribe to one of these loggers for
getting log messages.


Testing
=======

Make sure that an `Redis <https://redis.io/>`_ server instance is up and
running at ``redis://localhost:6379``.

After installing the ``eidaws.utils`` and ``eidaws.federator`` distributions,
required test dependencies can be installed with  

.. code::

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
