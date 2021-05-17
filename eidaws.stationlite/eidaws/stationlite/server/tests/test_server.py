# -*- coding: utf-8 -*-

import pathlib
import pytest

from eidaws.stationlite.server import create_app

path_module = pathlib.Path(__file__).parent


@pytest.fixture
def client():
    app = create_app()
    app.config[
        "SQLALCHEMY_DATABASE_URI"
    ] = f"sqlite:///{path_module}/data/test.db"
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client

@pytest.fixture
def content_type():
    def _content_type(query_format_or_status_code):
        if query_format_or_status_code in (
            "post",
            "get",
            200,
            204,
            400,
            404,
            413,
            414,
            500,
            503,
        ):
            return "text/plain; charset=utf-8"

    return _content_type
