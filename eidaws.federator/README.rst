.. _NGINX: http://nginx.org/

=========================
EIDA Federator webservice 
=========================

Asynchronous implementation of a federating gateway webservice as part of EIDA
NG. Federation is performed for

- *eidaws-wfcatalog*
- *fdsnws-availability*
- *fdsnws-dataselect*
- *fdsnws-station*

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
- ``fdsnws-availability`` and ``fdsnws-station`` requests run entirely in-memory
- ``fdsnws-dataselect`` and ``eidaws-wfcatalog`` request are buffered in-memory
  for small chunks of data; larger chunks of data are dynamically moved to disk
- ``fdsnws-dataselect`` and ``eidaws-wfcatalog``  response data is returned
  merged and fully aligned when splitting large requests
- Independent configuration of federated resources


Installation
============

Container
---------

For a containerized deployment please refer to the `eidaws-federator-deployment
<https://github.com/damb/eidaws-federator-deployment>`_ repository.


Development
-----------

**Dependencies**:

Make sure the following software is installed:

- `libxml2 <http://xmlsoft.org/>`_
- `libxslt <http://xmlsoft.org/XSLT/>`_

Regarding the version to be used visit http://lxml.de/installation.html#requirements.

To install the required development packages of these dependencies on Linux
systems, use your distribution specific installation tool, e.g. apt-get on
Debian/Ubuntu:

.. code::

  $ sudo apt-get install libxml2-dev libxslt-dev python3-dev


**Download**:

Clone the repository:

.. code::

  $ git clone https://github.com/damb/eidaws.git && cd eidaws


**Installation**:

In order to install ``eidaws.federator`` services, invoke

.. code::

  $ pip install eidaws.utils
  $ pip install eidaws.federator

Note, that encapsulating the installation by means of a `virtual environment
<https://docs.python.org/3/tutorial/venv.html>`_ is strongly recommended.


**Running**:

Federated resources are implemented as standalone applications with respect to
the resources' pathes. Thus, implementations of the following services are
provided:

- ``eidaws-wfcatalog-json`` (``/fedws/wfcatalog/json/1``)
- ``fdsnws-availability-geocsv`` (``/fedws/availability/geocsv/1``)
- ``fdsnws-availability-json`` (``/fedws/availability/json/1``)
- ``fdsnws-availability-request`` (``/fedws/availability/request/1``)
- ``fdsnws-availability-text`` (``/fedws/availability/text/1``)
- ``fdsnws-dataselect-miniseed`` (``/fedws/dataselect/miniseed/1``)
- ``fdsnws-station-xml`` (``/fedws/station/xml/1``)
- ``fdsnws-station-text``  (``/fedws/station/text/1``)

.. note::

  In favor of a simplified versioning scheme, both resource pathes and allowed
  values for the ``format`` query filter parameter are application specific
  and **not** compliant with `FDSN webservice <https://www.fdsn.org/webservices/>`_
  standards. In order to provide a fully `FDSN webservice
  <https://www.fdsn.org/webservices/>`_ conform API consider the deployment
  behind a *reverse proxy*.

After installing the ``eidaws.federator`` distribution the corresponding 
standalone applications are available:

- ``eida-federator-wfcatalog-json``
- ``eida-federator-availability-geocsv``
- ``eida-federator-availability-json``
- ``eida-federator-availability-request``
- ``eida-federator-availability-text``
- ``eida-federator-dataselect-miniseed``
- ``eida-federator-station-xml``
- ``eida-federator-station-text``

Running one of those application is as simple as e.g.

.. code::

  $ eida-federator-wfcatalog-json --serve-static


Now the service should be up and running at ``localhost:8080``. Let's perform
an exemplary request. E.g. in a second terminal window invoke

.. code::

  $ curl -v -o - "http://localhost:8080/fedws/wfcatalog/json/1/version"
  *   Trying ::1:8080...
  * Connected to localhost (::1) port 8080 (#0)
  > GET /fedws/wfcatalog/json/1/version HTTP/1.1
  > Host: localhost:8080
  > User-Agent: curl/7.74.0
  > Accept: */*
  > 
  * Mark bundle as not supporting multiuse
  < HTTP/1.1 200 OK
  < Content-Type: plain/text; charset=utf-8
  < Last-Modified: Wed, 01 Jul 2020 15:40:53 GMT
  < Content-Length: 5
  < Accept-Ranges: bytes
  < Date: Thu, 08 Apr 2021 13:32:19 GMT
  < Server: Python/3.8 aiohttp/3.7.4.post0
  < 
  * Connection #0 to host localhost left intact
  1.0.0

Also, check out the `Configuration`_ section if you'd like to run the service
on a different ``hostname:port`` destination.


Additional information and help is provided when invoking the application with
the ``-h|--help`` flag. E.g.

.. code::

  $ eida-federator-wfcatalog-json -h


Note, that for production it has several advantages running the services behind
a *reverse proxy server* such as e.g. NGINX_. In particular, if providing a
fully compliant `FDSN webservice <https://www.fdsn.org/webservices/>`_ API is
desired.


Configuration
=============

Federated resource service applications can be configured by means of a `YAML
<https://en.wikipedia.org/wiki/YAML>`_ configuration file. Exemplary fully
documented configuration files are provided under
``eidaws.federator/config/eidaws_federator_*_config.yml.example``. In order to
change the default configuration make a copy of the corresponding example
configuration with e.g.

.. code::

  $ cp -v \
    eidaws.federator/config/eidaws_federator_wfcatalog_json_config.yml.example \
    eidaws.federator/config/eidaws_federator_wfcatalog_json_config.yml

and adopt the file according to your needs. Then invoke the corresponding
service application with the ``-c|--config`` flag e.g.

.. code::

  $ eida-federator-wfcatalog-json \
    -c eidaws.federator/config/eidaws_federator_wfcatalog_json_config.yml


Logging
=======

The *eidaws.federator* distribution uses standard `logging
<https://docs.python.org/3/library/logging.html#module-logging>`_ for tracking
the application activity.

Depending on the federated resource service the following loggers enumerated by
name are provided:

- ``eidaws.federator.eidaws.wfcatalog.json``
- ``eidaws.federator.fdsnws.availability.geocsv``
- ``eidaws.federator.fdsnws.availability.json``
- ``eidaws.federator.fdsnws.availability.request``
- ``eidaws.federator.fdsnws.availability.text``
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

Required test dependencies can be installed with  

.. code::

  $ pip install -r eidaws.federator/requirements/test.txt


In order to run the tests, invoke

.. code::

  $ pytest eidaws.utils eidaws.federator


Limitations
===========

- AAI of both *fdsnws-dataselect* and *fdsnws-availability* resources is not
  implemented yet
- A ``fdsnws-station-xml&level=channel|response`` metadata request including
  only a single datacenter might be quite imperformant compared to a direct
  request to the corresponding datacenter.
- In certain cases, HTTP response codes might be misleading due to limitations
  of the `FDSN webservice <https://www.fdsn.org/webservices/>`_ specification
  not fully prepared to operate in a distributed environment.
