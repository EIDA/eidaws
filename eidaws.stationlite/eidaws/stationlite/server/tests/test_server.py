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


def build_query_string(mappings):
    return "&".join(f"{k}={v}" for m in mappings for k, v in m.items())


def build_postfile(sncls, **filter_args):
    payload = [f"{k}={v}".encode("utf-8") for k, v in filter_args.items()]
    payload.extend(sncls)

    return b"\n".join(payload)


def create_request_kwargs(method, params_or_data, **filter_args):
    method = method.lower()
    if method == "get":
        return {
            "query_string": build_query_string([filter_args, params_or_data])
        }
    return {"data": build_postfile(params_or_data, **filter_args)}
