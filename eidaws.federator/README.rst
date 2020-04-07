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
- ``fdsnws-station`` requests run entirely in-memory
- ``fdsnws-dataselect`` request are buffered in-memory for small chunks of
  data; larger chunks of data are dynamically moved to disk
- ``fdsnws-dataselect`` response data is returned merged and fully aligned when
  splitting large requests
- Independent configuration of federated resources
