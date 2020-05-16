# -*- coding: utf-8 -*-

import os
import errno
from flask import Config as _Config, Flask

from eidaws.utils.config import interpolate_environment_variables


# XXX(damb): Backport from Flask==2.0 providing environment variable
# interpolation facilities

_CONFIG_SECTION = 'eidaws.stationlite'


class Config(_Config):
    def from_file(self, filename, load, silent=False, interpolate=True):
        """
        Update the values in the config from a file that is loaded
        using the ``load`` parameter. The loaded data is passed to the
        :meth:`from_mapping` method.

        .. code-block:: python

            import toml
            app.config.from_file("config.toml", load=toml.load)

        :param filename: The path to the data file. This can be an
            absolute path or relative to the config root path.
        :param load: A callable that takes a file handle and returns a
            mapping of loaded data from the file.
        :type load: ``Callable[[Reader], Mapping]`` where ``Reader``
            implements a ``read`` method.
        :param silent: Ignore the file if it doesn't exist.
        :param interpolate: Interpolate environment variables within the
            configuration file
        """

        filename = os.path.join(self.root_path, filename)

        try:
            with open(filename) as f:
                obj = load(f)
        except OSError as e:
            if silent and e.errno in (errno.ENOENT, errno.EISDIR):
                return False

            e.strerror = f"Unable to load configuration file ({e.strerror})"
            raise

        if interpolate:
            obj = interpolate_environment_variables(obj, SECTION, os.environ)

        return self.from_mapping(obj)








Flask.config_class = Config
