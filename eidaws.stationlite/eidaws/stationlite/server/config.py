# -*- coding: utf-8 -*-

import os

from eidaws.utils.config import (
    PATH_JOKER,
    to_boolean,
    to_int,
    re_path,
    ConversionMap as _ConversionMap,
)
from eidaws.stationlite.settings import STL_BASE_ID


class Config:
    DEBUG = False
    PROPAGATE_EXCEPTIONS = False
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
    return re_path(STL_BASE_ID, PATH_JOKER, *args)


class ConversionMap(_ConversionMap):
    MAP = {
        stl_path("DEBUG"): to_boolean,
        stl_path("PROPAGATE_EXCEPTIONS"): to_boolean,
        stl_path("SQLALCHEMY_TRACK_MODIFICATIONS"): to_boolean,
    }
