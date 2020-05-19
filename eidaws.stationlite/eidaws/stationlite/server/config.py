# -*- coding: utf-8 -*-

import os

from eidaws.utils.config import (
    to_str,
    to_boolean,
    to_int,
    re_path,
    ConversionMap as _ConversionMap,
)
from eidaws.stationlite.settings import STL_BASE_ID


class Config:
    DEBUG = False
    PROPAGATE_EXCEPTIONS = True
    PATH_LOGGING_CONF = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @property
    def SQLALCHEMY_DATABASE_URI(self):
        db_user = os.environ.get("POSTGRES_USER")
        db_pass = os.environ.get("POSTGRES_PASSWORD")

        return "postgresql://{}localhost:5432/stationlite".format(
            ""
            if not db_user
            else "{}{}@".format(db_user, f":{db_pass}" if db_pass else "")
        )


def stl_path(*args):
    return re_path(STL_BASE_ID, *args)


class ConversionMap(_ConversionMap):
    MAP = {
        stl_path("PATH_LOGGING_CONF"): to_str,
        stl_path("DEBUG"): to_boolean,
        stl_path("PROPAGATE_EXCEPTIONS"): to_boolean,
        stl_path("SQLALCHEMY_TRACK_MODIFICATIONS"): to_boolean,
        stl_path("SQLALCHEMY_ENGINE_OPTIONS", "pool_size"): to_int,
        stl_path("SQLALCHEMY_ENGINE_OPTIONS", "pool_timeout"): to_int,
        stl_path("SQLALCHEMY_DATABASE_URI"): to_str,
    }
