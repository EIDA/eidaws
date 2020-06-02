# -*- coding: utf-8 -*-

from flask_sqlalchemy import SQLAlchemy

from eidaws.stationlite.engine.db import configure_sqlite

db = SQLAlchemy()


def setup_db(app):
    db.init_app(app)

    if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
        SQLITE_PRAGMAS = ["PRAGMA case_sensitive_like=on"]
        configure_sqlite(SQLITE_PRAGMAS)
