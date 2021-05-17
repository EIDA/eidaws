# -*- coding: utf-8 -*-

import pytest

from eidaws.stationlite.server import create_app
from eidaws.stationlite.server.tests.test_server import (
    client,
    content_type,
    path_module,
)

from eidaws.utils.settings import EIDAWS_STATIONLITE_PATH_QUERY


class TestStationLite:
    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "FOO",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2020-01-01",
                    "end": "2020-01-02",
                },
            ),
            ("post", [b"FOO HASLI -- LHZ 2020-01-01 2020-01-02"]),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_no_content(
        self, client, service_args, content_type, method, params_or_data
    ):
        req_kwargs = create_request_kwargs(
            method, params_or_data, **service_args
        )
        resp = getattr(client, method)(
            EIDAWS_STATIONLITE_PATH_QUERY, **req_kwargs
        )

        assert resp.status_code == 204
        assert resp.headers["Content-Type"] == content_type(204)
        assert b"" == resp.data

    @pytest.mark.parametrize("data", [b"", b"="])
    def test_keywordparser_post_invalid(self, client, content_type, data):
        resp = client.post(EIDAWS_STATIONLITE_PATH_QUERY, data=data)

        assert resp.status_code == 400
        assert resp.headers["Content-Type"] == content_type(400)
        assert resp.data.startswith(b"\nError 400: Bad request\n")

    @pytest.mark.parametrize(
        "service_config",
        [
            {
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{path_module}/data/",
                "PROPAGATE_EXCEPTIONS": True,
                "TESTING": True,
            },
            {
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{path_module}/data/test.db",
                "SQLALCHEMY_ENGINE_OPTIONS": {"pool_size": 5},
                "PROPAGATE_EXCEPTIONS": True,
                "TESTING": True,
            },
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_invalid_db_config(self, service_config, content_type):
        app = create_app(service_config)

        with app.test_client() as client:
            resp = client.get(
                EIDAWS_STATIONLITE_PATH_QUERY,
                query_string=(
                    "net=CH&sta=HASLI&loc=--&cha=LHZ&"
                    "start=2020-01-01&end=2020-01-02"
                ),
            )

            assert resp.status_code == 500
            assert resp.headers["Content-Type"] == content_type(500)
            assert resp.data.startswith(
                b"\nError 500: Internal server error\n"
            )
