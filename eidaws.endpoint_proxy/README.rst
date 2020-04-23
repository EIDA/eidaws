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
an error to client. Limited access to the proxied endpoint resource is granted
by a configurable connection pool size.

The service is intented to be used in conjunction with `eidaws-federator
<https://docs.aiohttp.org/en/stable/>`_.

The implementation is based on Python's `aiohttp
<https://docs.aiohttp.org/en/stable/>`_ framework.


Installation
============

Container
---------

TODO

Standalone
----------

TODO

Configuration
=============

TODO

Logging
=======

TODO
