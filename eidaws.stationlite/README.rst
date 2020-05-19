.. _eidaws-routing: https://github.com/EIDA/routing 

===========================
EIDA Stationlite webservice
===========================

Alternative implementation of a routing webservice as part of EIDA NG. In
contrast to eidaws-routing_ ``eidaws-stationlite`` returns fully resolved
stream epochs. The package implements two major components:

- **Harvesting**: Routing information is collected from EIDA routing
  ``localconfig`` configuration files. Routed stream epochs are fully resolved
  by means of ``fdsnws-station`` and stored in a local DB.

- **Webservice**: REST API allowing clients to query the routing information.
  With a few exceptions the API is similar to the one provided by
  eidaws-routing_.

  The webservice implementation is based on the `Flask
  <https://flask.palletsprojects.com/>`_ framework.


Installation
============

TODO

Harvesting
==========

TODO

Webservice
==========

TODO

Testing
=======

After installing the ``eidaws.utils`` and ``eidaws.stationlite`` distributions,
required test dependencies can be installed with  

.. code::

  $ pip install -r eidaws.stationlite/requirements/test.txt


In order to run the tests, invoke

.. code::

  $ pytest eidaws.utils eidaws.stationlite



