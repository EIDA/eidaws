.. _aiohttp: https://docs.aiohttp.org/en/stable/

==============================
EIDA Endpoint Proxy webservice
==============================

Implementation of a simple endpoint proxy webservice as part of EIDA NG. The
service implements shunting requests into a queue in order to prevent endpoint
webservices from overload. Thus, in contrast to `Apache2
<https://httpd.apache.org/>`_'s `mod_bw <https://github.com/IvnSoft/mod_bw>`_
or the `NGINX <http://nginx.org/>`_'s `limit_conn
<http://nginx.org/en/docs/http/ngx_http_limit_conn_module.html#limit_conn>`_
configuration option, it implements the `Leaky Bucket Algorithm
<https://en.wikipedia.org/wiki/Leaky_bucket>`_ instead of returning immediately
an error to the client. Limited access to the proxied endpoint resource is granted
by a configurable connection pool size.

The service is intented to be used in conjunction with ``eidaws-federator`` and
thus implements proxying the following resources:

- ``/fdsnws/dataselect/1/query``
- ``/fdsnws/station/1/query``
- ``/eidaws/wfcatalog/1/query``

The implementation is based on Python's aiohttp_ framework.


Installation
============

Standalone
----------

**Download**:

Clone the repository:

.. code::

  $ git clone https://github.com/EIDA/eidaws.git && cd eidaws


**Installation**:

In order to install the ``eidaws.endpoint_proxy`` service, invoke

.. code::

  $ pip install eidaws.utils
  $ pip install eidaws.endpoint_proxy

Note, that encapsulating the installation by means of a `virtual environment
<https://docs.python.org/3/tutorial/venv.html>`_ is strongly recommended.

**Running**:

In order to run the service simply invoke:

.. code::

  $ eida-endpoint-proxy

Additional information and help is provided when invoking the application with
the ``-h|--help`` flag. I.e.

.. code::

  $ eida-endpoint-proxy -h


Configuration
=============

The service is configured by means of a `YAML
<https://en.wikipedia.org/wiki/YAML>`_ configuration file. An exemplary fully
documented configuration file is provided under
``eidaws.endpoint_proxy/config/eidaws_proxy_config.yml.example``. In order to
change the default configuration make a copy of the example configuration with
e.g.

.. code::

  $ cp -v eidaws.endpoint_proxy/config/eidaws_proxy_config.yml.example \
    eidaws.endpoint_proxy/config/eidaws_proxy_config.yml

and adopt the file according to your needs. Then start the application with
the ``-c|--config`` flag e.g.

.. code::

  $ eida-endpoint-proxy -c eidaws.endpoint_proxy/config/eidaws_proxy_config.yml


Logging
=======

The *eidaws.endpoint_proxy* distribution uses standard `logging
<https://docs.python.org/3/library/logging.html#module-logging>`_ for tracking
the application activity. Besides of `loggers
<https://docs.aiohttp.org/en/stable/logging.html>`_ from ``aiohttp`` an
application specific logger named ``eidaws.endpoint_proxy`` is provided. 

When configuring logging by means of a logging configuration file (see also the
`Configuration`_ section), you may subscribe to these loggers for getting log
messages.


Limitations
===========

The bandwiths limitation is based on the facilities provided by aiohttp_'s
`TCPConnector
<https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp-client-reference-connectors>`_
facilities. Thus, the queue size is not configurable.

Full featured bandwith limitation is implemented by e.g. the `bottleneck
<https://www.npmjs.com/package/bottleneck>`_ package. In future, an improved
implementation might be based on these facilites.
