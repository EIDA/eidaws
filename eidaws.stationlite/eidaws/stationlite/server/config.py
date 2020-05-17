# -*- coding: utf-8 -*-

import os


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
