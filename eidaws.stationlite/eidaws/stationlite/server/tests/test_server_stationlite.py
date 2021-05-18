# -*- coding: utf-8 -*-

import json
import pytest

from operator import itemgetter

from eidaws.stationlite.server import create_app
from eidaws.stationlite.server.tests.test_server import (
    client,
    content_type as content_type_text,
    create_request_kwargs,
    path_module,
)

from eidaws.utils.settings import EIDAWS_STATIONLITE_PATH_QUERY


@pytest.fixture
def content_type_json():
    return "application/json"


@pytest.fixture
def sort_response():
    def _sort_response(dict_lst):
        key = itemgetter(
            "network",
            "station",
            "location",
            "channel",
            "starttime",
            "endtime",
            "restrictedStatus",
        )
        return sorted(dict_lst, key=key)

    return _sort_response


class TestStationLite:
    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "1999-01-01",
                    "end": "2021-01-01",
                },
            ),
            ("post", [b"CH HASLI -- LHZ 1999-01-01 2021-01-01"]),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_single_sncl_merged(
        self, client, content_type_json, method, params_or_data
    ):
        expected_response = [
            {
                "network": "CH",
                "station": "HASLI",
                "location": "",
                "channel": "LHZ",
                "starttime": "1999-01-19T00:00:00",
                "endtime": "2021-01-01T00:00:00",
                "restrictedStatus": "open",
            },
        ]

        req_kwargs = create_request_kwargs(
            method,
            params_or_data,
        )
        resp = getattr(client, method)(
            EIDAWS_STATIONLITE_PATH_QUERY, **req_kwargs
        )

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type_json
        assert expected_response == json.loads(resp.data)

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH",
                    "sta": "HASLI",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "1999-01-01",
                    "end": "2021-01-01",
                },
            ),
            ("post", [b"CH HASLI -- LHZ 1999-01-01 2021-01-01"]),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_single_sncl_raw(
        self, client, content_type_json, sort_response, method, params_or_data
    ):
        expected_response = [
            {
                "network": "CH",
                "station": "HASLI",
                "location": "",
                "channel": "LHZ",
                "starttime": "1999-01-19T00:00:00",
                "endtime": "1999-06-16T00:00:00",
                "restrictedStatus": "open",
            },
            {
                "network": "CH",
                "station": "HASLI",
                "location": "",
                "channel": "LHZ",
                "starttime": "1999-06-16T00:00:00",
                "endtime": "2021-01-01T00:00:00",
                "restrictedStatus": "open",
            },
        ]

        req_kwargs = create_request_kwargs(
            method,
            params_or_data,
            **{"merge": "false"}
        )
        resp = getattr(client, method)(
            EIDAWS_STATIONLITE_PATH_QUERY, **req_kwargs
        )

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type_json
        assert sort_response(expected_response) == sort_response(
            json.loads(resp.data)
        )

    @pytest.mark.parametrize(
        "method,params_or_data",
        [
            (
                "get",
                {
                    "net": "CH,Z3",
                    "sta": "GRIMS,A051A",
                    "loc": "--",
                    "cha": "LHZ",
                    "start": "2016-01-01",
                    "end": "2018-01-01",
                },
            ),
            (
                "post",
                [
                    b"CH GRIMS -- LHZ 2016-01-01 2018-01-01",
                    b"Z3 A051A -- LHZ 2016-01-01 2018-01-01",
                ],
            ),
        ],
        ids=["method=GET", "method=POST"],
    )
    def test_multi_sncl_merged(
        self, client, content_type_json, sort_response, method, params_or_data
    ):
        expected_response = [
            {
                "network": "CH",
                "station": "GRIMS",
                "location": "",
                "channel": "LHZ",
                "starttime": "2016-01-01T00:00:00",
                "endtime": "2018-01-01T00:00:00",
                "restrictedStatus": "open",
            },
            {
                "network": "Z3",
                "station": "A051A",
                "location": "",
                "channel": "LHZ",
                "starttime": "2016-01-01T00:00:00",
                "endtime": "2018-01-01T00:00:00",
                "restrictedStatus": "closed",
            },
        ]

        req_kwargs = create_request_kwargs(
            method,
            params_or_data,
        )
        resp = getattr(client, method)(
            EIDAWS_STATIONLITE_PATH_QUERY, **req_kwargs
        )

        assert resp.status_code == 200
        assert resp.headers["Content-Type"] == content_type_json
        assert sort_response(expected_response) == sort_response(
            json.loads(resp.data)
        )

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
        self, client, content_type_text, method, params_or_data
    ):
        req_kwargs = create_request_kwargs(
            method,
            params_or_data,
        )
        resp = getattr(client, method)(
            EIDAWS_STATIONLITE_PATH_QUERY, **req_kwargs
        )

        assert resp.status_code == 204
        assert resp.headers["Content-Type"] == content_type_text(204)
        assert b"" == resp.data

    @pytest.mark.parametrize("data", [b"", b"="])
    def test_keywordparser_post_invalid(self, client, content_type_text, data):
        resp = client.post(EIDAWS_STATIONLITE_PATH_QUERY, data=data)

        assert resp.status_code == 400
        assert resp.headers["Content-Type"] == content_type_text(400)
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
    def test_invalid_db_config(self, service_config, content_type_text):
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
            assert resp.headers["Content-Type"] == content_type_text(500)
            assert resp.data.startswith(
                b"\nError 500: Internal server error\n"
            )
