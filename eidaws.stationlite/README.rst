.. _eidaws-routing: https://github.com/EIDA/routing 
.. _Flask: https://flask.palletsprojects.com/

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

  The webservice implementation is based on the Flask_ framework.


Installation
============

Container
---------

TODO

Development
-----------

**Download**:

Clone the repository:

.. code::

  $ git clone https://github.com/damb/eidaws.git && cd eidaws


**Installation**:

In order to install the ``eidaws.stationlite`` distribution, invoke

.. code::

  $ pip install eidaws.utils
  $ pip install eidaws.stationlite[postgres]

The installation of the ``postgres`` feature is only required if the
application is run with a `PostgreSQL <https://www.postgresql.org/>`_ backend.

Note, that encapsulating the installation by means of a `virtual environment
<https://docs.python.org/3/tutorial/venv.html>`_ is strongly recommended.

Harvesting
==========

TODO

Webservice
==========

For development purposes the ``eidaws.stationlite`` webservice can be run using
the built-in Flask_ server:

.. code::

  $ export FLASK_APP=eidaws.stationlite/eidaws/stationlite/server/ && flask run


For additional details execute ``flask run -h``.


**Configuration**:

The service application can be configured by means of a `YAML
<https://en.wikipedia.org/wiki/YAML>`_ configuration file. An exemplary fully
documented configuration file is provided under
``eidaws.stationlite/config/eidaws_config.yml.example``. In order to change the
configuration make a copy of the example configuration with e.g.

.. code::

  $ cp -v eidaws.stationlite/config/eidaws_config.yml.example \
    eidaws.stationlite/config/eidaws_config.yml

and adjust the configuration according to your needs. Then invoke the
corresponding service application with

.. code::

   $ export EIDAWS_STATIONLITE_SETTINGS=../../../config/eidaws_config.yml \
     FLASK_APP=eidaws.stationlite/eidaws/stationlite/server/ && flask run

Logging
=======

The *eidaws.stationlite* distribution uses standard `logging
<https://docs.python.org/3/library/logging.html#module-logging>`_ for tracking
the application activity. An application specific logger named
``eidaws.stationlite`` is provided.

When configuring logging by means of a logging configuration file, you may
subscribe to this logger for getting log messages.

Testing
=======

After installing the ``eidaws.utils`` and ``eidaws.stationlite`` distributions,
required test dependencies can be installed with  

.. code::

  $ pip install -r eidaws.stationlite/requirements/test.txt


In order to run the tests, invoke

.. code::

  $ pytest eidaws.utils eidaws.stationlite



